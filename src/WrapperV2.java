import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;

public class WrapperV2 {

	public class UnknownProperty extends Exception {
		
		private static final long serialVersionUID = -2154805128624019618L;

		public UnknownProperty(String name) {
			System.err.println("Unknown property (" + name + ")");
		}
	}
	
	/**
	 * 
	 */
	//private static final String LOCAL_DATA = "/home/seb/Dropbox/stage/code/expfiles/berlinData/WEB/viewsN3/activite/activities.xml";
	
	/**
	 * Our namespace prefix.
	 */
	private static final String ONTO_PREFIX = "onto";

	/**
	 * Our namespace.
	 */
	private static final String ONTO_URL = "http://example.org/activite/";

	/**
	 * API URL.
	 */
	private static final String API_URL = "https://data.nantes.fr/api/publication/"
			+ "22440002800011_CG44_TOU_04812/"
			+ "activites_tourisme_et_handicap_STBL/content";

	//https://data.nantes.fr/api/publication/22440002800011_CG44_TOU_04812/activites_tourisme_et_handicap_STBL/content
	/**
	 * Ontology / XML data mapping file path.
	 */
	//private static final String MAPPING_FILE = "res/mapping.properties";
	private static final String MAPPING_FILE = "/home/seb/Dropbox/stage/code/expfiles/berlinData/WEB/viewsN3/activite/mapping.properties";


	/**
	 * Ontology / API mapping file path.
	 */
	private static final String MAPPING_API_FILE = "/home/seb/Dropbox/stage/code/expfiles/berlinData/WEB/viewsN3/activite/mapping_api.properties";

	/**
	 * Ontology / XML data mapping.
	 */
	private HashMap<String, String> mapping_ = new HashMap<String, String>();

	/**
	 * Ontology / API mapping.
	 */
	private HashMap<String, String> mappingAPI_ = new HashMap<String, String>();

	/**
	 * Filters to add to API call
	 */
	private HashMap<String, String> filters_ = new HashMap<String, String>();
	
	/**
	 * Query triples which do not contain literal.
	 * 
	 * <p>Triples that will be evaluated on the data returned by the API.</p> 
	 */
	private LinkedList<Triple> triples_ = new LinkedList<Triple>();

	/**
	 * HTTP Request to the API
	 */
	private String apiCall_ = new String(API_URL);

	/**
	 * XML data returned by the API.
	 */
	private Document apiData_;

	private Model resModel_ = ModelFactory.createDefaultModel();

	/**
	 * Loads mappings files.
	 * 
	 * @throws FileNotFoundException
	 *             Whether a mapping file is missing
	 * @throws IOException
	 *             Whether reading files fails
	 */
	private void loadMappings() throws FileNotFoundException, IOException {
		Properties prop = new Properties();
		prop.load(new FileReader(MAPPING_FILE));

		for (Object key : prop.keySet()) {
			this.mapping_.put(key.toString(), prop.getProperty(key.toString()));
		}

		prop = new Properties();
		prop.load(new FileReader(MAPPING_API_FILE));

		for (Object key : prop.keySet()) {
			this.mappingAPI_.put(key.toString(),
					prop.getProperty(key.toString()));
		}
	}

	/**
	 * Extracts triples from query.
	 * 
	 * @param path
	 *            Path of the query file
	 * @throws IOException 
	 */
	private void loadQuery(String path) throws IOException {
		String qString = new String();
		
		BufferedReader reader = new BufferedReader(new FileReader(path));
		String line = new String();
		
		while((line = reader.readLine()) != null) {
			qString += line;
		}
		
		Query query = QueryFactory.create(qString);
		
		PrefixMapping pm = query.getPrefixMapping();
		Map<String, String> truc = pm.getNsPrefixMap();

		for (String key : truc.keySet()) {
			this.resModel_.setNsPrefix(key, truc.get(key));
		}

		ElementGroup eltGroup = (ElementGroup) query.getQueryPattern();
		ElementPathBlock pathBlock = (ElementPathBlock) eltGroup.getElements()
				.get(0);

		Iterator<TriplePath> triplesIt = pathBlock.patternElts();

		while (triplesIt.hasNext()) {
			TriplePath tp = triplesIt.next();
			Node object = tp.getObject();

			if (object.isLiteral()) {
				Node property = tp.getPredicate();

				this.filters_.put(property.getLocalName(), object.getLiteral()
						.toString());
			}

			else {
				this.triples_.push(tp.asTriple());
			}
		}
	}

	private void buildApiCall() {
		Boolean filter = false;

		if (this.filters_.isEmpty() == false) {
			filter = true;
			this.apiCall_ += "?filter={"; // Filter start

			Set<String> keys = this.filters_.keySet();

			if (keys.size() > 1) {
				this.apiCall_ += "\"$and\":[";

				Boolean first = true;

				for (String key : keys) {
					String apiField = this.mappingAPI_.get(key);
					String value = this.filters_.get(key);

					if (first == false) {
						this.apiCall_ += ",";
					}

					else {
						first = false;
					}

					this.apiCall_ += "{\"" + apiField + "\":{";
					this.apiCall_ += "\"$eq\":\"" + value + "\"";
					this.apiCall_ += "}}";
				}

				this.apiCall_ += "]";
			}

			else {
				String key = keys.iterator().next();
				String apiField = mappingAPI_.get(key);
				String value = filters_.get(key);

				this.apiCall_ += "\"" + apiField + "\":{";
				this.apiCall_ += "\"$eq\":\"" + value + "\"";
				this.apiCall_ += "}";
			}

			this.apiCall_ += "}"; // Filter end
		}

		if (filter == true) {
			this.apiCall_ += "&format=xml";
		}

		else {
			this.apiCall_ += "?format=xml";
		}
	}

	private void getData() throws IOException, JDOMException {
		InputStream stream = null;
		URL url = new URL(this.apiCall_);
		URLConnection connection = url.openConnection();
		stream = connection.getInputStream();

		//FileInputStream stream = new FileInputStream(LOCAL_DATA);
		
		this.apiData_ = new SAXBuilder().build(stream);
	}

	private void processQuery()
			throws IOException, JDOMException, UnknownProperty {
		
		this.getData();

		for (Triple triple : this.triples_) {
			Node s = triple.getSubject();
			Node p = triple.getPredicate();
			Node o = triple.getObject();
			
			Element root = this.apiData_.getRootElement();
			
			List<Element> listeActi =
					root.getChild("data").getChildren("element");

			List<Pair<String, String>> mappings =
					new ArrayList<Pair<String, String>>();
			
			if (p.isVariable() == true) {
				Set<String> keys = mapping_.keySet();
				
				for(String key : keys) {
					mappings.add(
							new Pair<String, String>(key, mapping_.get(key)));
				}
			}
			
			else {
				mappings.add(new Pair<String, String>(p.getLocalName(),
						mapping_.get(p.getLocalName())));
			}
			
			if(mappings.isEmpty()) {
				throw new UnknownProperty(p.getLocalName() + ")");
			}
			
			for(Pair<String, String> mapping : mappings) {
				//ATTENTION BUG ICI MAPPINGS IS NULL FOR VIEW 6
				String value = mapping.getSecond();
				String[] paths = value.split(",");
	
				Iterator<Element> actiIt = listeActi.iterator();
	
				int cnt = 0;
	
				while (actiIt.hasNext()) {
					Element current = actiIt.next();
					cnt++;
					
					for (int idxPath = 0; idxPath < paths.length; idxPath++) {
						String path = paths[idxPath];
						String[] elements = path.split("\\.");
	
						for (int idxElement = 0 ; idxElement < elements.length
								&& current != null ; ++idxElement) {
							
							current = current.getChild(elements[idxElement]);
						}
	
						if ((current != null)
								&& (current.getValue().equals("null") == false)) {
							
							Resource res = resModel_.createResource(ONTO_URL
									+ "activityLocation" + cnt);
							
							Property prop = resModel_.createProperty(ONTO_URL
									+ mapping.getFirst());
							
							res.addProperty(prop, current.getValue());
						}
					}
				}
			}
		}
	}
	
	/**
	 * @param path Path of the query file
	 * @throws IOException
	 * @throws JDOMException
	 * @throws UnknownProperty
	 */
	public void query(String path)
			throws IOException, JDOMException, UnknownProperty {
		
		this.loadQuery(path);
		this.buildApiCall();
		this.processQuery();
	}
	
	public WrapperV2(){
		System.setProperty("http.proxyHost", "cache.sciences.univ-nantes.fr");
		System.setProperty("http.proxyPort", "3128");		
	}
	public static void main(String[] args) {
		
		SetProxy px = new SetProxy();

		WrapperV2 w2 = new WrapperV2();
		String queryPath = new String();
		String outputPath = new String();
		
		if(args.length < 2) {
			System.err.println("Error : argument(s) missing.");
//			return;
			queryPath = "/home/seb/Dropbox/stage/code/expfiles/berlinData/WEB/viewsSparql/view8_0.sparql";
			outputPath = "/home/seb/Dropbox/stage/code/expfiles/berlinData/WEB/viewsN3/view8_0.n3";
		}
		else{
		queryPath = args[0];
		outputPath = args[1];
		}
		
		try {
			w2.loadMappings();
		}

		catch (FileNotFoundException e) {
			System.err.println("Error : mapping file not found.");
		}

		catch (IOException e) {
			System.err.println("Error : reading mapping file failed.");
		}

		try {
			w2.query(queryPath);
			
			FileOutputStream outputStream = new FileOutputStream(outputPath);
			w2.resModel_.write(outputStream, "N-TRIPLE");
		}
		
		catch(UnknownHostException e) {
			System.err.println("Erreur : connection au webservice echouee. "
					+ "Nom d'hote inconnu. Veuillez verifier "
					+ "votre connexion.");
		}
		
		catch(FileNotFoundException e) {
			e.printStackTrace(System.err);
		}
		
		catch (IOException e) {
			e.printStackTrace(System.err);
		}

		catch (JDOMException e) {
			e.printStackTrace(System.err);
		}
		
		catch (UnknownProperty e) {
			e.printStackTrace(System.err);
		}
	}
}
