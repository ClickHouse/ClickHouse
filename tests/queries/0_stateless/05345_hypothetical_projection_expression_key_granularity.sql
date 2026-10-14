-- the writer sizes the granules of a projection part with the columns of its key expression, also when the part does not store them
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0, allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_est;
DROP TABLE IF EXISTS t_real;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, v ORDER BY intDiv(b, 10));

INSERT INTO t_est SELECT number, number % 100, number FROM numbers(3000);
INSERT INTO t_real SELECT number, number % 100, number FROM numbers(3000);

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, v ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE intDiv(b, 10) = 4)
WHERE match(line, '^(Baseline|With |marks:|status:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real WHERE intDiv(b, 10) = 4)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');


-- a row of a LowCardinality column stores only its index, for a key and for a stored column
DROP HYPOTHETICAL PROJECTION p_k ON t_est;
CREATE TABLE t_real_lc AS t_est;
ALTER TABLE t_real_lc ADD PROJECTION p_lc (SELECT a, b, v ORDER BY toLowCardinality(intDiv(b, 10)));
INSERT INTO t_real_lc SELECT number, number % 100, number FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_lc ON t_est (SELECT a, b, v ORDER BY toLowCardinality(intDiv(b, 10)));
SELECT '-- a key that returns LowCardinality';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE toLowCardinality(intDiv(b, 10)) < 5)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real_lc WHERE toLowCardinality(intDiv(b, 10)) < 5)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

CREATE TABLE s_est (a UInt64, b UInt64, c LowCardinality(String)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
CREATE TABLE s_real AS s_est;
ALTER TABLE s_real ADD PROJECTION p_b (SELECT a, b, c ORDER BY b);
INSERT INTO s_est SELECT number, number % 100, repeat(toString(number % 5), 60) FROM numbers(3000);
INSERT INTO s_real SELECT number, number % 100, repeat(toString(number % 5), 60) FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_b ON s_est (SELECT a, b, c ORDER BY b);
SELECT '-- a stored LowCardinality(String) column';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, c FROM s_est WHERE b < 50)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, c FROM s_real WHERE b < 50)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

-- with many distinct values the dictionary is a large part of the block, and the index needs two bytes
CREATE TABLE t_est_many AS t_est;
CREATE TABLE t_real_many AS t_est;
ALTER TABLE t_real_many ADD PROJECTION p_many (SELECT a, b, v ORDER BY toLowCardinality(b));
INSERT INTO t_est_many SELECT number, number % 1000, number FROM numbers(3000);
INSERT INTO t_real_many SELECT number, number % 1000, number FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_many ON t_est_many (SELECT a, b, v ORDER BY toLowCardinality(b));
SELECT '-- a key with many distinct values';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est_many WHERE toLowCardinality(b) < 900)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT if(explain LIKE '%ReadFromMergeTree (p_many)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, v FROM t_real_many WHERE toLowCardinality(b) < 900) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT 'real with force_optimize_projection: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real_many WHERE toLowCardinality(b) < 900 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE t_real_lc;
DROP TABLE s_est;
DROP TABLE s_real;
DROP TABLE t_est_many;
DROP TABLE t_real_many;
