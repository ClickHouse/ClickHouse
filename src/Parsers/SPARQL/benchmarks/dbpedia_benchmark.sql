-- DBpedia subset benchmark adapted for SPARQL-ClickHouse
-- Reference: diploma Section 8.2 — realistic RDF data from Wikipedia infoboxes
-- Tests: natural string length distribution, irregular predicate structure,
-- uneven selectivity across properties
--
-- Usage: clickhouse-client --time < dbpedia_benchmark.sql

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

-- Generate DBpedia-like dataset with realistic Wikipedia entity distribution
-- Entities: Person, Place, Organisation, Work (film/book/album)
-- Properties follow real DBpedia ontology names

-- Persons (largest class in DBpedia)
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'rdf:type', 'dbo:Person' FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'foaf:name', concat('Person ', toString(number)) FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'dbo:birthDate', concat(toString(1920 + number % 80), '-', toString(1 + number % 12), '-', toString(1 + number % 28)) FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'dbo:birthPlace', concat('dbr:City_', toString(number % 500)) FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'dbo:nationality', concat('dbr:Country_', toString(number % 50)) FROM numbers(25000);
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'dbo:occupation', concat('dbr:Occupation_', toString(number % 200)) FROM numbers(20000);
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'dbo:almaMater', concat('dbr:University_', toString(number % 300)) FROM numbers(15000);
INSERT INTO rdf_triples SELECT concat('dbr:Person_', toString(number)), 'rdfs:comment', concat('This is a description of Person ', toString(number), ' who is known for various achievements in their field.') FROM numbers(30000);

-- Places
INSERT INTO rdf_triples SELECT concat('dbr:City_', toString(number)), 'rdf:type', 'dbo:Place' FROM numbers(500);
INSERT INTO rdf_triples SELECT concat('dbr:City_', toString(number)), 'foaf:name', concat('City_', toString(number)) FROM numbers(500);
INSERT INTO rdf_triples SELECT concat('dbr:City_', toString(number)), 'dbo:country', concat('dbr:Country_', toString(number % 50)) FROM numbers(500);
INSERT INTO rdf_triples SELECT concat('dbr:City_', toString(number)), 'dbo:population', toString(10000 + number * 1000) FROM numbers(500);
INSERT INTO rdf_triples SELECT concat('dbr:Country_', toString(number)), 'rdf:type', 'dbo:Country' FROM numbers(50);
INSERT INTO rdf_triples SELECT concat('dbr:Country_', toString(number)), 'foaf:name', concat('Country_', toString(number)) FROM numbers(50);

-- Organisations
INSERT INTO rdf_triples SELECT concat('dbr:Org_', toString(number)), 'rdf:type', 'dbo:Organisation' FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Org_', toString(number)), 'foaf:name', concat('Organisation_', toString(number)) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Org_', toString(number)), 'dbo:foundingYear', toString(1800 + number % 220) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Org_', toString(number)), 'dbo:headquarter', concat('dbr:City_', toString(number % 500)) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Org_', toString(number)), 'dbo:industry', concat('dbr:Industry_', toString(number % 30)) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:University_', toString(number)), 'rdf:type', 'dbo:University' FROM numbers(300);
INSERT INTO rdf_triples SELECT concat('dbr:University_', toString(number)), 'foaf:name', concat('University_', toString(number)) FROM numbers(300);
INSERT INTO rdf_triples SELECT concat('dbr:University_', toString(number)), 'dbo:country', concat('dbr:Country_', toString(number % 50)) FROM numbers(300);

-- Works (films, books, albums)
INSERT INTO rdf_triples SELECT concat('dbr:Film_', toString(number)), 'rdf:type', 'dbo:Film' FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Film_', toString(number)), 'foaf:name', concat('Film_', toString(number)) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Film_', toString(number)), 'dbo:director', concat('dbr:Person_', toString(number % 2000)) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Film_', toString(number)), 'dbo:starring', concat('dbr:Person_', toString(number % 5000)) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Film_', toString(number)), 'dbo:releaseYear', toString(1950 + number % 75) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat('dbr:Book_', toString(number)), 'rdf:type', 'dbo:Book' FROM numbers(3000);
INSERT INTO rdf_triples SELECT concat('dbr:Book_', toString(number)), 'foaf:name', concat('Book_', toString(number)) FROM numbers(3000);
INSERT INTO rdf_triples SELECT concat('dbr:Book_', toString(number)), 'dbo:author', concat('dbr:Person_', toString(number % 3000)) FROM numbers(3000);

SELECT '--- DBpedia Benchmark Queries ---';
SELECT '';
SELECT concat('Total triples: ', toString(count())) FROM rdf_triples;
SELECT '';

-- ============================================================
-- Type / class queries (high selectivity variation)
-- ============================================================

SELECT 'D1: All persons (large class, 30K)';
SELECT count() FROM sparql('SELECT ?p WHERE { ?p <rdf:type> <dbo:Person> }');

SELECT 'D2: All films (medium class, 5K)';
SELECT count() FROM sparql('SELECT ?f WHERE { ?f <rdf:type> <dbo:Film> }');

SELECT 'D3: All countries (small class, 50)';
SELECT count() FROM sparql('SELECT ?c WHERE { ?c <rdf:type> <dbo:Country> }');

-- ============================================================
-- Star queries on Person (most common DBpedia pattern)
-- ============================================================

SELECT 'D4: Person name + birthDate';
SELECT count() FROM sparql('SELECT ?name ?birth WHERE { ?p <rdf:type> <dbo:Person> . ?p <foaf:name> ?name . ?p <dbo:birthDate> ?birth }');

-- SQL baseline for D4
SELECT 'D4 baseline: SQL';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.subject
JOIN rdf_triples AS t3 ON t3.subject = t1.subject
PREWHERE t1.predicate = 'rdf:type'
WHERE t1.object = 'dbo:Person' AND t2.predicate = 'foaf:name' AND t3.predicate = 'dbo:birthDate';

SELECT 'D5: Person name + nationality + occupation (sparse — only 20K have occupation)';
SELECT count() FROM sparql('SELECT ?name ?nat ?occ WHERE { ?p <rdf:type> <dbo:Person> . ?p <foaf:name> ?name . ?p <dbo:nationality> ?nat . ?p <dbo:occupation> ?occ }');

SELECT 'D6: Person full profile with OPTIONAL almaMater';
SELECT count() FROM sparql('SELECT ?name ?birth ?nat ?uni WHERE { ?p <rdf:type> <dbo:Person> . ?p <foaf:name> ?name . ?p <dbo:birthDate> ?birth . ?p <dbo:nationality> ?nat . OPTIONAL { ?p <dbo:almaMater> ?uni } }');

-- ============================================================
-- Cross-entity joins (snowflake patterns)
-- ============================================================

SELECT 'D7: Films with director name';
SELECT count() FROM sparql('SELECT ?filmName ?dirName WHERE { ?f <rdf:type> <dbo:Film> . ?f <foaf:name> ?filmName . ?f <dbo:director> ?d . ?d <foaf:name> ?dirName }');

-- SQL baseline for D7
SELECT 'D7 baseline: SQL';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.subject
JOIN rdf_triples AS t3 ON t3.subject = t1.subject
JOIN rdf_triples AS t4 ON t4.subject = t3.object
PREWHERE t1.predicate = 'rdf:type'
WHERE t1.object = 'dbo:Film' AND t2.predicate = 'foaf:name' AND t3.predicate = 'dbo:director' AND t4.predicate = 'foaf:name';

SELECT 'D8: Person -> birthPlace -> country (two-hop navigation)';
SELECT count() FROM sparql('SELECT ?name ?country WHERE { ?p <rdf:type> <dbo:Person> . ?p <foaf:name> ?name . ?p <dbo:birthPlace> ?city . ?city <dbo:country> ?country }');

SELECT 'D9: Organisation with headquarter city and country';
SELECT count() FROM sparql('SELECT ?orgName ?cityName ?country WHERE { ?o <rdf:type> <dbo:Organisation> . ?o <foaf:name> ?orgName . ?o <dbo:headquarter> ?city . ?city <foaf:name> ?cityName . ?city <dbo:country> ?country }');

-- ============================================================
-- FILTER queries (natural data distributions)
-- ============================================================

SELECT 'D10: Films released after 2000';
SELECT count() FROM sparql('SELECT ?name ?year WHERE { ?f <rdf:type> <dbo:Film> . ?f <foaf:name> ?name . ?f <dbo:releaseYear> ?year . FILTER(?year > 2000) }');

SELECT 'D11: Cities with population > 100000';
SELECT count() FROM sparql('SELECT ?name ?pop WHERE { ?c <rdf:type> <dbo:Place> . ?c <foaf:name> ?name . ?c <dbo:population> ?pop . FILTER(?pop > 100000) }');

SELECT 'D12: Persons with name matching regex';
SELECT count() FROM sparql('SELECT ?name WHERE { ?p <rdf:type> <dbo:Person> . ?p <foaf:name> ?name . FILTER regex(?name, "Person 1..") }');

-- ============================================================
-- UNION queries
-- ============================================================

SELECT 'D13: All creative works (films UNION books)';
SELECT count() FROM sparql('SELECT ?name WHERE { { ?w <rdf:type> <dbo:Film> . ?w <foaf:name> ?name } UNION { ?w <rdf:type> <dbo:Book> . ?w <foaf:name> ?name } }');

-- SQL baseline for D13
SELECT 'D13 baseline: SQL';
SELECT count() FROM (
    SELECT t2.object AS name
    FROM rdf_triples AS t1
    JOIN rdf_triples AS t2 ON t2.subject = t1.subject
    PREWHERE t1.predicate = 'rdf:type'
    WHERE t1.object = 'dbo:Film' AND t2.predicate = 'foaf:name'
    UNION ALL
    SELECT t2.object AS name
    FROM rdf_triples AS t1
    JOIN rdf_triples AS t2 ON t2.subject = t1.subject
    PREWHERE t1.predicate = 'rdf:type'
    WHERE t1.object = 'dbo:Book' AND t2.predicate = 'foaf:name'
);

-- ============================================================
-- Modifier queries
-- ============================================================

SELECT 'D14: Top-20 most populated cities';
SELECT count() FROM sparql('SELECT ?name ?pop WHERE { ?c <rdf:type> <dbo:Place> . ?c <foaf:name> ?name . ?c <dbo:population> ?pop } ORDER BY DESC(?pop) LIMIT 20');

SELECT 'D15: Distinct nationalities';
SELECT count() FROM sparql('SELECT DISTINCT ?nat WHERE { ?p <dbo:nationality> ?nat }');

SELECT 'D16: Films ordered by release year, top 50';
SELECT count() FROM sparql('SELECT ?name ?year WHERE { ?f <rdf:type> <dbo:Film> . ?f <foaf:name> ?name . ?f <dbo:releaseYear> ?year } ORDER BY DESC(?year) LIMIT 50');

-- ============================================================
-- Complex combined queries
-- ============================================================

SELECT 'D17: Persons born in Country_0 with optional occupation, ordered by name';
SELECT count() FROM sparql('SELECT ?name ?occ WHERE { ?p <rdf:type> <dbo:Person> . ?p <foaf:name> ?name . ?p <dbo:birthPlace> ?city . ?city <dbo:country> <dbr:Country_0> . OPTIONAL { ?p <dbo:occupation> ?occ } } ORDER BY ?name LIMIT 100');

SELECT 'D18: Directors who also authored books (cross-domain join)';
SELECT count() FROM sparql('SELECT ?dirName WHERE { ?f <dbo:director> ?d . ?b <dbo:author> ?d . ?d <foaf:name> ?dirName }');

-- SQL baseline for D18
SELECT 'D18 baseline: SQL';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.object = t1.object
JOIN rdf_triples AS t3 ON t3.subject = t1.object
PREWHERE t1.predicate = 'dbo:director'
WHERE t2.predicate = 'dbo:author' AND t3.predicate = 'foaf:name';

SELECT '';
SELECT '--- Summary ---';
SELECT concat('Total triples: ', toString(count())) FROM rdf_triples;

DROP TABLE rdf_triples;
