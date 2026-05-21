-- LUBM (Lehigh University Benchmark) adapted queries for SPARQL-ClickHouse
-- Reference: diploma Section 8.2 — LUBM provides a fixed set of fourteen queries
-- oriented at various aspects: predicate selectivity, join complexity, hierarchical data
--
-- This script creates a LUBM-like dataset and runs the benchmark queries.
-- Usage: clickhouse-client --time < lubm_benchmark.sql
--
-- Schema follows diploma Section 5: single rdf_triples table with
-- LowCardinality(String) predicate, bloom_filter indexes, ORDER BY (predicate, subject, object)

DROP TABLE IF EXISTS mv_types;
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

-- Materialized view for rdf:type pattern (diploma Section 5.4)
CREATE MATERIALIZED VIEW mv_types
ENGINE = MergeTree ORDER BY (object, subject) AS
SELECT subject, object
FROM rdf_triples
WHERE predicate = 'rdf:type';

-- Generate LUBM-1 scale (~100K triples): universities, departments, professors, students, courses
-- Type triples
INSERT INTO rdf_triples SELECT concat(':univ', toString(number)), 'rdf:type', ':University' FROM numbers(10);
INSERT INTO rdf_triples SELECT concat(':dept', toString(number)), 'rdf:type', ':Department' FROM numbers(100);
INSERT INTO rdf_triples SELECT concat(':prof', toString(number)), 'rdf:type', ':Professor' FROM numbers(1000);
INSERT INTO rdf_triples SELECT concat(':student', toString(number)), 'rdf:type', ':Student' FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':course', toString(number)), 'rdf:type', ':Course' FROM numbers(500);
INSERT INTO rdf_triples SELECT concat(':pub', toString(number)), 'rdf:type', ':Publication' FROM numbers(5000);

-- Property triples
INSERT INTO rdf_triples SELECT concat(':dept', toString(number)), ':subOrganizationOf', concat(':univ', toString(number % 10)) FROM numbers(100);
INSERT INTO rdf_triples SELECT concat(':prof', toString(number)), ':worksFor', concat(':dept', toString(number % 100)) FROM numbers(1000);
INSERT INTO rdf_triples SELECT concat(':prof', toString(number)), ':name', concat('Professor_', toString(number)) FROM numbers(1000);
INSERT INTO rdf_triples SELECT concat(':prof', toString(number)), ':email', concat('prof', toString(number), '@univ.edu') FROM numbers(1000);
INSERT INTO rdf_triples SELECT concat(':student', toString(number)), ':memberOf', concat(':dept', toString(number % 100)) FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':student', toString(number)), ':name', concat('Student_', toString(number)) FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':student', toString(number)), ':email', concat('student', toString(number), '@univ.edu') FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat(':student', toString(number)), ':takesCourse', concat(':course', toString(number % 500)) FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':course', toString(number)), ':name', concat('Course_', toString(number)) FROM numbers(500);
INSERT INTO rdf_triples SELECT concat(':pub', toString(number)), ':publicationAuthor', concat(':prof', toString(number % 1000)) FROM numbers(5000);
INSERT INTO rdf_triples SELECT concat(':pub', toString(number)), ':name', concat('Publication_', toString(number)) FROM numbers(5000);

SELECT '--- LUBM Benchmark Queries ---';
SELECT '';

-- Q1: Find students who take a specific course
-- Tests: simple two-pattern join, high selectivity on object
SELECT 'Q1: Students taking course0';
SELECT count() FROM sparql('SELECT ?s WHERE { ?s <rdf:type> <:Student> . ?s <:takesCourse> <:course0> }');

-- Q2: Find all students, their universities and departments
-- Tests: three-way join, transitive navigation
SELECT 'Q2: Students with department and university';
SELECT count() FROM sparql('SELECT ?s ?dept ?univ WHERE { ?s <rdf:type> <:Student> . ?s <:memberOf> ?dept . ?dept <:subOrganizationOf> ?univ }');

-- Q3: Find publications by a specific professor
-- Tests: two-pattern join with constant
SELECT 'Q3: Publications by prof0';
SELECT count() FROM sparql('SELECT ?pub WHERE { ?pub <rdf:type> <:Publication> . ?pub <:publicationAuthor> <:prof0> }');

-- Q4: Find professors with name and email
-- Tests: three-way join, all properties of a type
SELECT 'Q4: Professors with name and email';
SELECT count() FROM sparql('SELECT ?name ?email WHERE { ?p <rdf:type> <:Professor> . ?p <:name> ?name . ?p <:email> ?email }');

-- Q5: Find members of department0
-- Tests: two patterns, medium selectivity
SELECT 'Q5: Members of dept0';
SELECT count() FROM sparql('SELECT ?s WHERE { ?s <rdf:type> <:Student> . ?s <:memberOf> <:dept0> }');

-- Q6: Find all students (type-only query)
-- Tests: single pattern, low selectivity, benefits from mv_types
SELECT 'Q6: All students';
SELECT count() FROM sparql('SELECT ?s WHERE { ?s <rdf:type> <:Student> }');

-- Q7: Find students and their courses with OPTIONAL email
-- Tests: OPTIONAL (LEFT JOIN), three mandatory + one optional pattern
SELECT 'Q7: Students with course and optional email';
SELECT count() FROM sparql('SELECT ?name ?course ?email WHERE { ?s <rdf:type> <:Student> . ?s <:name> ?name . ?s <:takesCourse> ?course . OPTIONAL { ?s <:email> ?email } }');

-- Q8: Find all people (UNION of professors and students)
-- Tests: UNION pattern
SELECT 'Q8: All people (professors UNION students)';
SELECT count() FROM sparql('SELECT ?name WHERE { { ?p <rdf:type> <:Professor> . ?p <:name> ?name } UNION { ?s <rdf:type> <:Student> . ?s <:name> ?name } }');

-- Q9: Find professors in department0 with FILTER
-- Tests: FILTER with equality
SELECT 'Q9: Professors in dept0 via FILTER';
SELECT count() FROM sparql('SELECT ?name WHERE { ?p <rdf:type> <:Professor> . ?p <:name> ?name . ?p <:worksFor> ?dept . FILTER (?dept = ":dept0") }');

-- Q10: Hand-written SQL baseline for Q1 (comparison point per diploma Section 8.3)
SELECT 'Q10: Baseline SQL for Q1';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.subject
WHERE t1.predicate = 'rdf:type' AND t1.object = ':Student'
  AND t2.predicate = ':takesCourse' AND t2.object = ':course0';

-- Q11: Top-10 youngest students (ORDER BY + LIMIT)
SELECT 'Q11: Top-10 youngest students';
SELECT * FROM sparql('SELECT ?name ?age WHERE { ?s <rdf:type> <:Student> . ?s <:name> ?name . ?s <:age> ?age } ORDER BY ?age LIMIT 10');

-- Q12: Distinct departments
SELECT 'Q12: Distinct departments';
SELECT count() FROM sparql('SELECT DISTINCT ?dept WHERE { ?p <:worksFor> ?dept }');

-- Q13: Students with email, ordered, top-50 (combined modifiers)
SELECT 'Q13: Students with email, ordered, limit 50';
SELECT count() FROM sparql('SELECT ?name ?email WHERE { ?s <rdf:type> <:Student> . ?s <:name> ?name . ?s <:email> ?email } ORDER BY ?name LIMIT 50');

-- Q14: Professors older than 45, ordered descending (FILTER + ORDER BY DESC)
SELECT 'Q14: Professors age > 45, ordered desc';
SELECT count() FROM sparql('SELECT ?name ?age WHERE { ?p <rdf:type> <:Professor> . ?p <:name> ?name . ?p <:age> ?age . FILTER(?age > 45) } ORDER BY DESC(?age) LIMIT 20');

-- Q15: Hand-written SQL baseline for Q11 (comparison for ORDER BY + LIMIT)
SELECT 'Q15: Baseline SQL for Q11';
SELECT t2.object AS name, t3.object AS age
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.subject
JOIN rdf_triples AS t3 ON t3.subject = t1.subject
PREWHERE t1.predicate = 'rdf:type'
WHERE t1.object = ':Student' AND t2.predicate = ':name' AND t3.predicate = ':age'
ORDER BY age LIMIT 10;

DROP VIEW mv_types;
DROP TABLE rdf_triples;
