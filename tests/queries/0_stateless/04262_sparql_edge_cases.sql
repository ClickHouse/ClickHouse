-- Edge cases: empty results, single triple, unbound variable, PREFIX, PREWHERE, chain joins

DROP TABLE IF EXISTS rdf_triples;

CREATE TABLE rdf_triples
(
    subject String,
    predicate LowCardinality(String),
    object String,
    INDEX idx_subject subject TYPE bloom_filter GRANULARITY 4,
    INDEX idx_object object TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY (predicate, subject, object);

INSERT INTO rdf_triples VALUES
    ('http://ex.org/alice', 'rdf:type', 'http://ex.org/Person'),
    ('http://ex.org/alice', 'http://ex.org/name', 'Alice'),
    ('http://ex.org/alice', 'http://ex.org/age', '30'),
    ('http://ex.org/alice', 'http://ex.org/worksAt', 'http://ex.org/dept1'),
    ('http://ex.org/bob', 'rdf:type', 'http://ex.org/Person'),
    ('http://ex.org/bob', 'http://ex.org/name', 'Bob'),
    ('http://ex.org/bob', 'http://ex.org/age', '25'),
    ('http://ex.org/bob', 'http://ex.org/worksAt', 'http://ex.org/dept2'),
    ('http://ex.org/dept1', 'rdf:type', 'http://ex.org/Department'),
    ('http://ex.org/dept1', 'http://ex.org/deptName', 'Engineering'),
    ('http://ex.org/dept1', 'http://ex.org/locatedIn', 'http://ex.org/city1'),
    ('http://ex.org/dept2', 'rdf:type', 'http://ex.org/Department'),
    ('http://ex.org/dept2', 'http://ex.org/deptName', 'Marketing'),
    ('http://ex.org/dept2', 'http://ex.org/locatedIn', 'http://ex.org/city1'),
    ('http://ex.org/city1', 'http://ex.org/cityName', 'Moscow');

-- 1. Empty result: no matching type
SELECT * FROM sparql('SELECT ?s WHERE { ?s <rdf:type> <http://ex.org/Animal> }');

-- 2. Single triple pattern: no joins needed
SELECT * FROM sparql('SELECT ?city WHERE { ?c <http://ex.org/cityName> ?city }');

-- 3. PREFIX expansion: use PREFIX to abbreviate IRIs
SELECT * FROM sparql('PREFIX ex: <http://ex.org/> SELECT ?name WHERE { ?p <rdf:type> <http://ex.org/Person> . ?p ex:name ?name }') ORDER BY name;

-- 4. Unbound variable in projection: ?email is not defined in WHERE
SELECT * FROM sparql('SELECT ?name ?email WHERE { ?p <rdf:type> <http://ex.org/Person> . ?p <http://ex.org/name> ?name }') ORDER BY name;

-- 5. Chain join: person -> worksAt -> department name (object-to-subject navigation)
SELECT * FROM sparql('SELECT ?personName ?deptName WHERE { ?p <http://ex.org/name> ?personName . ?p <http://ex.org/worksAt> ?d . ?d <http://ex.org/deptName> ?deptName }') ORDER BY personName;

-- 6. Three-hop chain: person -> worksAt -> locatedIn -> cityName
SELECT * FROM sparql('SELECT ?personName ?city WHERE { ?p <http://ex.org/name> ?personName . ?p <http://ex.org/worksAt> ?d . ?d <http://ex.org/locatedIn> ?c . ?c <http://ex.org/cityName> ?city }') ORDER BY personName;

-- 7. PREWHERE correctness: verify results with predicate-first sort key
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?p <rdf:type> <http://ex.org/Person> . ?p <http://ex.org/name> ?name . ?p <http://ex.org/age> ?age }') ORDER BY name;

-- 8. FILTER on chain: departments in city with specific name
SELECT * FROM sparql('SELECT ?deptName WHERE { ?d <http://ex.org/deptName> ?deptName . ?d <http://ex.org/locatedIn> ?c . ?c <http://ex.org/cityName> ?city . FILTER(?city = "Moscow") }') ORDER BY deptName;

-- 9. OPTIONAL on chain: person name + optional department
SELECT * FROM sparql('SELECT ?name ?deptName WHERE { ?p <rdf:type> <http://ex.org/Person> . ?p <http://ex.org/name> ?name . OPTIONAL { ?p <http://ex.org/worksAt> ?d . ?d <http://ex.org/deptName> ?deptName } }') ORDER BY name;

-- 10. Combined modifiers on chain: person -> dept, ordered, limited
SELECT * FROM sparql('SELECT ?name ?deptName WHERE { ?p <http://ex.org/name> ?name . ?p <http://ex.org/worksAt> ?d . ?d <http://ex.org/deptName> ?deptName } ORDER BY ?name LIMIT 1');

DROP TABLE rdf_triples;
