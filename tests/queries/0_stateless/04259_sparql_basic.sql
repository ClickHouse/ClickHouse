-- Test basic SPARQL table function integration
-- Creates an rdf_triples table, inserts RDF data, and queries via sparql()

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
    (':alice', 'rdf:type', ':Person'),
    (':alice', ':name', 'Alice'),
    (':alice', ':age', '30'),
    (':alice', ':email', 'alice@example.com'),
    (':bob', 'rdf:type', ':Person'),
    (':bob', ':name', 'Bob'),
    (':bob', ':age', '20'),
    (':carol', 'rdf:type', ':Person'),
    (':carol', ':name', 'Carol'),
    (':carol', ':age', '25'),
    (':carol', ':email', 'carol@example.com');

-- Basic triple pattern join: find all person names
SELECT * FROM sparql('SELECT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }') ORDER BY name;

-- OPTIONAL: find names and emails (not everyone has email)
SELECT * FROM sparql('SELECT ?name ?email WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name . OPTIONAL { ?p <:email> ?email } }') ORDER BY name;

-- FILTER: find people older than 22
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?p <:name> ?name . ?p <:age> ?age . FILTER (?age > 22) }') ORDER BY name;

-- UNION: find all contact info (email or name)
SELECT * FROM sparql('SELECT ?contact WHERE { { ?p <:email> ?contact } UNION { ?p <:name> ?contact } }') ORDER BY contact;

DROP TABLE rdf_triples;
