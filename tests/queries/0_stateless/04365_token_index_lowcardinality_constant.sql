-- Token-search skip indexes (text, tokenbf_v1, ngrambf_v1) must prune granules when the
-- constant needle is wrapped in LowCardinality, e.g. hasToken(s, toLowCardinality('rare')).
-- Before the fix the LowCardinality-typed constant failed the string-type gate, no index
-- condition was built, and the query silently degraded to a full scan (all granules read),
-- while a plain-String needle pruned to a single granule. Each assertion below checks that
-- the LowCardinality variant reads the SAME single granule as the plain-String variant.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_ngrambf;

CREATE TABLE t_text (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_text SELECT number, concat('common ', if(number = 500, 'rare', 'x'), ' tail') FROM numbers(1024);

CREATE TABLE t_tokenbf (id UInt64, s String, INDEX idx s TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_tokenbf SELECT number, concat('common ', if(number = 500, 'rare', 'x'), ' tail') FROM numbers(1024);

CREATE TABLE t_ngrambf (id UInt64, s String, INDEX idx s TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_ngrambf SELECT number, concat('common ', if(number = 500, 'rareword', 'xxxxword'), ' tail') FROM numbers(1024);

SELECT 'text hasToken String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE hasToken(s, 'rare')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'text hasToken LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE hasToken(s, toLowCardinality('rare'))) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'text equals LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE s = toLowCardinality('common rare tail')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'text hasToken LowCardinality(Nullable)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE hasToken(s, CAST('rare', 'LowCardinality(Nullable(String))'))) WHERE explain ILIKE '%Granules: 1/128%';

SELECT 'tokenbf hasToken String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE hasToken(s, 'rare')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'tokenbf hasToken LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE hasToken(s, toLowCardinality('rare'))) WHERE explain ILIKE '%Granules: 1/128%';

SELECT 'ngrambf hasToken String';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE hasToken(s, 'rareword')) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'ngrambf hasToken LowCardinality';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE hasToken(s, toLowCardinality('rareword'))) WHERE explain ILIKE '%Granules: 1/128%';

-- Results must stay correct: the LowCardinality needle finds exactly the seeded row.
SELECT 'correctness';
SELECT count(), min(id) FROM t_text WHERE hasToken(s, toLowCardinality('rare'));

DROP TABLE t_text;
DROP TABLE t_tokenbf;
DROP TABLE t_ngrambf;
