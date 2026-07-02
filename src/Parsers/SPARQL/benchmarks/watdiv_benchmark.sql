-- WatDiv (Waterloo SPARQL Diversity Test Suite) adapted for SPARQL-ClickHouse
-- Reference: diploma Section 8.2 — WatDiv models a social-commercial domain
-- with rich schema and irregular connectivity; queries classified by shape:
-- linear (L), star (S), snowflake (F), complex (C)
--
-- Usage: clickhouse-client --time < watdiv_benchmark.sql

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

-- Generate WatDiv-like social-commercial dataset
-- Entities: Users, Products, Offers, Reviews, Retailers, SubGenres

-- Type triples
INSERT INTO rdf_triples SELECT concat(':user', toString(number)), 'rdf:type', ':User' FROM numbers(50000);
INSERT INTO rdf_triples SELECT concat(':product', toString(number)), 'rdf:type', ':Product' FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':offer', toString(number)), 'rdf:type', ':Offer' FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat(':review', toString(number)), 'rdf:type', ':Review' FROM numbers(20000);
INSERT INTO rdf_triples SELECT concat(':retailer', toString(number)), 'rdf:type', ':Retailer' FROM numbers(500);
INSERT INTO rdf_triples SELECT concat(':genre', toString(number)), 'rdf:type', ':Genre' FROM numbers(50);

-- User properties
INSERT INTO rdf_triples SELECT concat(':user', toString(number)), ':name', concat('User_', toString(number)) FROM numbers(50000);
INSERT INTO rdf_triples SELECT concat(':user', toString(number)), ':age', toString(18 + number % 50) FROM numbers(50000);
INSERT INTO rdf_triples SELECT concat(':user', toString(number)), ':country', concat(':country', toString(number % 30)) FROM numbers(50000);
INSERT INTO rdf_triples SELECT concat(':user', toString(number)), ':friendOf', concat(':user', toString((number * 7 + 13) % 50000)) FROM numbers(50000);

-- Product properties
INSERT INTO rdf_triples SELECT concat(':product', toString(number)), ':name', concat('Product_', toString(number)) FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':product', toString(number)), ':hasGenre', concat(':genre', toString(number % 50)) FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':product', toString(number)), ':price', toString(10 + number % 990) FROM numbers(10000);
INSERT INTO rdf_triples SELECT concat(':product', toString(number)), ':rating', toString(1 + number % 5) FROM numbers(10000);

-- Offer properties (links products to retailers)
INSERT INTO rdf_triples SELECT concat(':offer', toString(number)), ':offeredBy', concat(':retailer', toString(number % 500)) FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat(':offer', toString(number)), ':forProduct', concat(':product', toString(number % 10000)) FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat(':offer', toString(number)), ':offerPrice', toString(5 + number % 500) FROM numbers(30000);
INSERT INTO rdf_triples SELECT concat(':offer', toString(number)), ':validUntil', concat('2026-', toString(1 + number % 12), '-01') FROM numbers(30000);

-- Review properties (links users to products)
INSERT INTO rdf_triples SELECT concat(':review', toString(number)), ':reviewer', concat(':user', toString(number % 50000)) FROM numbers(20000);
INSERT INTO rdf_triples SELECT concat(':review', toString(number)), ':reviewOf', concat(':product', toString(number % 10000)) FROM numbers(20000);
INSERT INTO rdf_triples SELECT concat(':review', toString(number)), ':reviewText', concat('Review_text_', toString(number)) FROM numbers(20000);
INSERT INTO rdf_triples SELECT concat(':review', toString(number)), ':reviewRating', toString(1 + number % 5) FROM numbers(20000);

-- User likes (many-to-many)
INSERT INTO rdf_triples SELECT concat(':user', toString(number)), ':likes', concat(':product', toString(number % 10000)) FROM numbers(50000);

-- Retailer properties
INSERT INTO rdf_triples SELECT concat(':retailer', toString(number)), ':name', concat('Retailer_', toString(number)) FROM numbers(500);
INSERT INTO rdf_triples SELECT concat(':retailer', toString(number)), ':country', concat(':country', toString(number % 30)) FROM numbers(500);

SELECT '--- WatDiv Benchmark Queries ---';
SELECT '';
SELECT concat('Total triples: ', toString(count())) FROM rdf_triples;
SELECT '';

-- ============================================================
-- LINEAR queries (L): chain joins, one variable binds the next
-- ============================================================

SELECT 'L1: User -> friendOf -> likes (2-hop chain)';
SELECT count() FROM sparql('SELECT ?friend ?product WHERE { <:user0> <:friendOf> ?friend . ?friend <:likes> ?product }');

SELECT 'L2: User -> review -> product -> genre (3-hop chain)';
SELECT count() FROM sparql('SELECT ?product ?genre WHERE { <:user0> <:likes> ?product . ?product <:hasGenre> ?genre }');

SELECT 'L3: Offer -> product -> genre chain with retailer';
SELECT count() FROM sparql('SELECT ?retailer ?genre WHERE { ?offer <:offeredBy> ?retailer . ?offer <:forProduct> ?product . ?product <:hasGenre> ?genre }');

-- SQL baseline for L3
SELECT 'L3 baseline: SQL';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.subject
JOIN rdf_triples AS t3 ON t3.subject = t2.object
PREWHERE t1.predicate = ':offeredBy'
WHERE t2.predicate = ':forProduct' AND t3.predicate = ':hasGenre';

SELECT 'L4: Review -> reviewer -> country (3-hop chain)';
SELECT count() FROM sparql('SELECT ?review ?country WHERE { ?review <rdf:type> <:Review> . ?review <:reviewer> ?user . ?user <:country> ?country }');

SELECT 'L5: Long chain — offer -> product -> genre, offer -> retailer -> country';
SELECT count() FROM sparql('SELECT ?genre ?retailerCountry WHERE { ?offer <:forProduct> ?product . ?product <:hasGenre> ?genre . ?offer <:offeredBy> ?retailer . ?retailer <:country> ?retailerCountry }');

SELECT '';

-- ============================================================
-- STAR queries (S): multiple predicates on same subject
-- ============================================================

SELECT 'S1: User star — name + age + country';
SELECT count() FROM sparql('SELECT ?name ?age ?country WHERE { ?u <rdf:type> <:User> . ?u <:name> ?name . ?u <:age> ?age . ?u <:country> ?country }');

-- SQL baseline for S1
SELECT 'S1 baseline: SQL';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.subject
JOIN rdf_triples AS t3 ON t3.subject = t1.subject
JOIN rdf_triples AS t4 ON t4.subject = t1.subject
PREWHERE t1.predicate = 'rdf:type'
WHERE t1.object = ':User' AND t2.predicate = ':name' AND t3.predicate = ':age' AND t4.predicate = ':country';

SELECT 'S2: Product star — name + genre + price + rating';
SELECT count() FROM sparql('SELECT ?name ?genre ?price ?rating WHERE { ?p <rdf:type> <:Product> . ?p <:name> ?name . ?p <:hasGenre> ?genre . ?p <:price> ?price . ?p <:rating> ?rating }');

SELECT 'S3: Offer star — all properties';
SELECT count() FROM sparql('SELECT ?retailer ?product ?price ?valid WHERE { ?o <rdf:type> <:Offer> . ?o <:offeredBy> ?retailer . ?o <:forProduct> ?product . ?o <:offerPrice> ?price . ?o <:validUntil> ?valid }');

SELECT 'S4: Review star — all properties';
SELECT count() FROM sparql('SELECT ?user ?product ?text ?rating WHERE { ?r <rdf:type> <:Review> . ?r <:reviewer> ?user . ?r <:reviewOf> ?product . ?r <:reviewText> ?text . ?r <:reviewRating> ?rating }');

-- SQL baseline for S4
SELECT 'S4 baseline: SQL';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.subject
JOIN rdf_triples AS t3 ON t3.subject = t1.subject
JOIN rdf_triples AS t4 ON t4.subject = t1.subject
JOIN rdf_triples AS t5 ON t5.subject = t1.subject
PREWHERE t1.predicate = 'rdf:type'
WHERE t1.object = ':Review' AND t2.predicate = ':reviewer' AND t3.predicate = ':reviewOf'
  AND t4.predicate = ':reviewText' AND t5.predicate = ':reviewRating';

SELECT 'S5: User with OPTIONAL email (sparse property)';
SELECT count() FROM sparql('SELECT ?name ?age WHERE { ?u <rdf:type> <:User> . ?u <:name> ?name . ?u <:age> ?age }');

SELECT '';

-- ============================================================
-- SNOWFLAKE queries (F): star with branches extending outward
-- ============================================================

SELECT 'F1: Offer -> product name + retailer name';
SELECT count() FROM sparql('SELECT ?prodName ?retailerName WHERE { ?o <:forProduct> ?p . ?p <:name> ?prodName . ?o <:offeredBy> ?r . ?r <:name> ?retailerName }');

-- SQL baseline for F1
SELECT 'F1 baseline: SQL';
SELECT count()
FROM rdf_triples AS t1
JOIN rdf_triples AS t2 ON t2.subject = t1.object
JOIN rdf_triples AS t3 ON t3.subject = t1.subject
JOIN rdf_triples AS t4 ON t4.subject = t3.object
PREWHERE t1.predicate = ':forProduct'
WHERE t2.predicate = ':name' AND t3.predicate = ':offeredBy' AND t4.predicate = ':name';

SELECT 'F2: Review -> reviewer name + product genre';
SELECT count() FROM sparql('SELECT ?userName ?genre WHERE { ?r <:reviewer> ?u . ?u <:name> ?userName . ?r <:reviewOf> ?p . ?p <:hasGenre> ?genre }');

SELECT 'F3: Review -> reviewer country + product name + rating';
SELECT count() FROM sparql('SELECT ?country ?prodName ?rating WHERE { ?r <:reviewer> ?u . ?u <:country> ?country . ?r <:reviewOf> ?p . ?p <:name> ?prodName . ?r <:reviewRating> ?rating }');

SELECT 'F4: Offer snowflake with product genre and retailer country';
SELECT count() FROM sparql('SELECT ?genre ?country ?price WHERE { ?o <:forProduct> ?p . ?p <:hasGenre> ?genre . ?o <:offeredBy> ?r . ?r <:country> ?country . ?o <:offerPrice> ?price }');

SELECT 'F5: Deep snowflake — offer -> product -> genre, offer -> retailer -> country, filtered by price';
SELECT count() FROM sparql('SELECT ?genre ?country WHERE { ?o <:forProduct> ?p . ?p <:hasGenre> ?genre . ?o <:offeredBy> ?r . ?r <:country> ?country . ?o <:offerPrice> ?price . FILTER(?price > 200) }');

SELECT '';

-- ============================================================
-- COMPLEX queries (C): UNION, OPTIONAL, FILTER combinations
-- ============================================================

SELECT 'C1: Products liked or reviewed by user0 (UNION)';
SELECT count() FROM sparql('SELECT ?product WHERE { { <:user0> <:likes> ?product } UNION { ?r <:reviewer> <:user0> . ?r <:reviewOf> ?product } }');

SELECT 'C2: Users with optional friend, filtered by age (OPTIONAL + FILTER)';
SELECT count() FROM sparql('SELECT ?name ?friend WHERE { ?u <rdf:type> <:User> . ?u <:name> ?name . ?u <:age> ?age . OPTIONAL { ?u <:friendOf> ?friend } . FILTER(?age > 60) }');

SELECT 'C3: Products with price filter, optional review count, ordered (FILTER + OPTIONAL + ORDER BY + LIMIT)';
SELECT count() FROM sparql('SELECT ?name ?price ?review WHERE { ?p <rdf:type> <:Product> . ?p <:name> ?name . ?p <:price> ?price . OPTIONAL { ?r <:reviewOf> ?p . ?r <:reviewRating> ?review } . FILTER(?price > 500) } ORDER BY ?name LIMIT 100');

SELECT 'C4: Top-20 cheapest offers with product and retailer names (snowflake + ORDER BY + LIMIT)';
SELECT count() FROM sparql('SELECT ?prodName ?retailerName ?price WHERE { ?o <:forProduct> ?p . ?p <:name> ?prodName . ?o <:offeredBy> ?r . ?r <:name> ?retailerName . ?o <:offerPrice> ?price } ORDER BY ?price LIMIT 20');

SELECT 'C5: Distinct genres of products liked by users in country0';
SELECT count() FROM sparql('SELECT DISTINCT ?genre WHERE { ?u <:country> <:country0> . ?u <:likes> ?product . ?product <:hasGenre> ?genre }');

SELECT '';
SELECT '--- Summary ---';
SELECT concat('Total triples: ', toString(count())) FROM rdf_triples;

DROP TABLE rdf_triples;
