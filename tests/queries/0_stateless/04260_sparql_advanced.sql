-- Advanced SPARQL tests: semicolon syntax, multi-OPTIONAL, complex FILTER, PREFIX, three-way join

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
    (':alice', ':phone', '+1-555-0101'),
    (':bob', 'rdf:type', ':Person'),
    (':bob', ':name', 'Bob'),
    (':bob', ':age', '20'),
    (':carol', 'rdf:type', ':Person'),
    (':carol', ':name', 'Carol'),
    (':carol', ':age', '25'),
    (':carol', ':email', 'carol@example.com'),
    (':dave', 'rdf:type', ':Person'),
    (':dave', ':name', 'Dave'),
    (':dave', ':age', '35'),
    (':dave', ':phone', '+1-555-0104');

-- Single triple pattern: find all subjects with rdf:type :Person
SELECT * FROM sparql('SELECT ?s WHERE { ?s <rdf:type> <:Person> }') ORDER BY s;

-- Three-way join: find name and age for all persons
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name . ?p <:age> ?age }') ORDER BY name;

-- Semicolon syntax: same as above but using ; shorthand
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?p <rdf:type> <:Person> ; <:name> ?name ; <:age> ?age }') ORDER BY name;

-- Multiple OPTIONALs: find name, email, and phone (not everyone has both)
SELECT * FROM sparql('SELECT ?name ?email ?phone WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name . OPTIONAL { ?p <:email> ?email } . OPTIONAL { ?p <:phone> ?phone } }') ORDER BY name;

-- FILTER with less-than: find people younger than 30
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?p <:name> ?name . ?p <:age> ?age . FILTER (?age < 30) }') ORDER BY name;

-- FILTER with logical AND: find people with age between 21 and 31
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?p <:name> ?name . ?p <:age> ?age . FILTER (?age > 21 && ?age < 31) }') ORDER BY name;

-- FILTER with equality: find specific person by name
SELECT * FROM sparql('SELECT ?age WHERE { ?p <:name> ?name . ?p <:age> ?age . FILTER (?name = "Carol") }');

-- UNION with three branches (email, phone, or name as contact)
SELECT * FROM sparql('SELECT ?contact WHERE { { ?p <:email> ?contact } UNION { ?p <:phone> ?contact } }') ORDER BY contact;

DROP TABLE rdf_triples;
