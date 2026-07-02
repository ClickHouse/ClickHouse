-- Test SPARQL solution modifiers: ORDER BY, LIMIT, OFFSET, DISTINCT
-- Also tests FILTER inside OPTIONAL (placed in ON clause per diploma Section 6.4.3)

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
    (':alice', ':score', '95'),
    (':bob', 'rdf:type', ':Person'),
    (':bob', ':name', 'Bob'),
    (':bob', ':age', '20'),
    (':bob', ':score', '80'),
    (':carol', 'rdf:type', ':Person'),
    (':carol', ':name', 'Carol'),
    (':carol', ':age', '25'),
    (':carol', ':score', '95'),
    (':dave', 'rdf:type', ':Person'),
    (':dave', ':name', 'Dave'),
    (':dave', ':age', '35'),
    (':dave', ':score', '70'),
    (':eve', 'rdf:type', ':Person'),
    (':eve', ':name', 'Eve'),
    (':eve', ':age', '28'),
    (':eve', ':score', '95');

-- 1. ORDER BY ascending (default)
SELECT * FROM sparql('SELECT ?name WHERE { ?p rdf:type <:Person> . ?p <:name> ?name } ORDER BY ?name');

-- 2. ORDER BY descending
SELECT * FROM sparql('SELECT ?name WHERE { ?p rdf:type <:Person> . ?p <:name> ?name } ORDER BY DESC(?name)');

-- 3. LIMIT
SELECT * FROM sparql('SELECT ?name WHERE { ?p rdf:type <:Person> . ?p <:name> ?name } ORDER BY ?name LIMIT 3');

-- 4. LIMIT + OFFSET
SELECT * FROM sparql('SELECT ?name WHERE { ?p rdf:type <:Person> . ?p <:name> ?name } ORDER BY ?name LIMIT 2 OFFSET 2');

-- 5. SELECT DISTINCT (multiple people share score 95)
SELECT * FROM sparql('SELECT DISTINCT ?score WHERE { ?p <:score> ?score }') ORDER BY score;

-- 6. Combined: DISTINCT + ORDER BY + LIMIT
SELECT * FROM sparql('SELECT DISTINCT ?score WHERE { ?p <:score> ?score } ORDER BY ?score LIMIT 3');

-- 7. FILTER inside OPTIONAL (goes into ON clause, not WHERE)
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?p <:name> ?name . OPTIONAL { ?p <:age> ?age . FILTER(?age > 27) } }') ORDER BY name;

DROP TABLE rdf_triples;
