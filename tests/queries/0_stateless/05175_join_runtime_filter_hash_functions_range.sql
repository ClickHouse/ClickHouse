-- The runtime bloom filter honors every hash function count the setting accepts, on the fixed-size
-- key path and on the generic byte path alike: the build and the probe side agree, so no matching
-- row is lost.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SET join_runtime_filter_exact_values_limit = 1; -- force the bloom filter instead of the exact set
SET query_plan_join_swap_table = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim;

CREATE TABLE dim (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE fact (k UInt64, s String) ENGINE = MergeTree ORDER BY k;

INSERT INTO dim SELECT number * 7, toString(number * 7) FROM numbers(100);
INSERT INTO fact SELECT number, toString(number) FROM numbers(100000);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM fact, dim WHERE fact.k = dim.k SETTINGS join_runtime_bloom_filter_hash_functions = 10)
WHERE explain LIKE '%RuntimeFilter%';

SELECT '-- UInt64 key';
SELECT count() FROM fact, dim WHERE fact.k = dim.k SETTINGS join_runtime_bloom_filter_hash_functions = 1;
SELECT count() FROM fact, dim WHERE fact.k = dim.k SETTINGS join_runtime_bloom_filter_hash_functions = 3;
SELECT count() FROM fact, dim WHERE fact.k = dim.k SETTINGS join_runtime_bloom_filter_hash_functions = 7;
SELECT count() FROM fact, dim WHERE fact.k = dim.k SETTINGS join_runtime_bloom_filter_hash_functions = 8;
SELECT count() FROM fact, dim WHERE fact.k = dim.k SETTINGS join_runtime_bloom_filter_hash_functions = 10;

SELECT '-- String key';
SELECT count() FROM fact, dim WHERE fact.s = dim.s SETTINGS join_runtime_bloom_filter_hash_functions = 1;
SELECT count() FROM fact, dim WHERE fact.s = dim.s SETTINGS join_runtime_bloom_filter_hash_functions = 3;
SELECT count() FROM fact, dim WHERE fact.s = dim.s SETTINGS join_runtime_bloom_filter_hash_functions = 7;
SELECT count() FROM fact, dim WHERE fact.s = dim.s SETTINGS join_runtime_bloom_filter_hash_functions = 8;
SELECT count() FROM fact, dim WHERE fact.s = dim.s SETTINGS join_runtime_bloom_filter_hash_functions = 10;

DROP TABLE fact;
DROP TABLE dim;
