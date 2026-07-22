-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/111311
-- With transform_null_in=1 the analyzer rewrites `x IN (...)` to `nullIn(x, ...)`. When the
-- IN-set has no NULL element, the bloom_filter skip index must still be used (nullIn selects
-- the same rows as in). When the set contains a NULL, the index is not used (no pruning).
-- The contract is full-scan avoidance, so every "prunes" assertion checks that the skip index
-- actually reduced the read granule count (read < total), not merely that it was analyzed.

DROP TABLE IF EXISTS t_bf_null_in;
CREATE TABLE t_bf_null_in (x String, INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_bf_null_in SELECT toString(number) FROM numbers(1000);

SELECT 'String: IN null-free set prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in WHERE x IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in WHERE x IN ('5', '500') SETTINGS transform_null_in = 1;

SELECT 'String: `=` prunes with transform_null_in=1 (was already working)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in WHERE x = '5' SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in WHERE x = '5' SETTINGS transform_null_in = 1;

DROP TABLE t_bf_null_in;

-- Nullable / LowCardinality / LowCardinality(Nullable) type-wrapper matrix.
DROP TABLE IF EXISTS t_bf_null_in_n;
CREATE TABLE t_bf_null_in_n
(
    a Nullable(String),
    b LowCardinality(String),
    c LowCardinality(Nullable(String)),
    INDEX idx_a a TYPE bloom_filter GRANULARITY 1,
    INDEX idx_b b TYPE bloom_filter GRANULARITY 1,
    INDEX idx_c c TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_bf_null_in_n
SELECT if(number % 100 = 0, NULL, toString(number)), toString(number), if(number % 100 = 0, NULL, toString(number))
FROM numbers(1000);

SELECT 'Nullable: IN null-free set prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', '500') SETTINGS transform_null_in = 1;

SELECT 'LowCardinality: IN null-free set prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 1;

SELECT 'LowCardinality(Nullable): IN null-free set prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE c IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE c IN ('5', '500') SETTINGS transform_null_in = 1;

-- When the set contains a NULL, nullIn also matches NULL rows: the index must NOT prune,
-- and the result must include the NULL rows (10 rows: number % 100 = 0 -> {0,100,...,900}).
SELECT 'Nullable: IN set with NULL does not prune, result includes NULL rows';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', NULL) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', NULL) SETTINGS transform_null_in = 1;

-- Correctness cross-check: results are identical with transform_null_in=0 and =1 for a
-- null-free set over a non-null column value.
SELECT 'Correctness: null-free set, results equal for transform_null_in 0 vs 1';
SELECT
    (SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 0) =
    (SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 1);

DROP TABLE t_bf_null_in_n;

-- Array column: whole-array equality bloom filter hashing is not sound for granules that mix
-- empty and non-empty arrays, so the index must NOT be used for `nullIn` on Array columns.
-- Regression: without the Array guard, `x IN ([])` would wrongly prune the granule holding [].
DROP TABLE IF EXISTS t_bf_null_in_arr;
CREATE TABLE t_bf_null_in_arr (x Array(UInt32), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_null_in_arr VALUES ([]), ([1]), ([2]), ([3]);

SELECT 'Array: IN does not prune with transform_null_in=1 (unsound array hashing)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_arr WHERE x IN ([]) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'Array: IN empty array result is correct with transform_null_in=1';
SELECT count() FROM t_bf_null_in_arr WHERE x IN ([]) SETTINGS transform_null_in = 1;

DROP TABLE t_bf_null_in_arr;

-- Type-incompatible set: with transform_null_in=1 the set casts the key strictly (not
-- accurate_or_null), so a String index against an integer set throws at execution. The index
-- must NOT prune such a granule, otherwise the runtime error would be swallowed and the query
-- would wrongly return an empty result. Types are compared modulo Nullable / LowCardinality,
-- so the wrapper cases above are unaffected.
DROP TABLE IF EXISTS t_bf_null_in_ty;
CREATE TABLE t_bf_null_in_ty (x String, INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_bf_null_in_ty SELECT toString(number) FROM numbers(1000);

SELECT 'Type mismatch: String index vs integer set does not prune with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_ty WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64(extract(explain, 'Granules: (\d+)/')) < toUInt64(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'Type mismatch: query still raises the conversion error with transform_null_in=1';
SELECT count() FROM t_bf_null_in_ty WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }

DROP TABLE t_bf_null_in_ty;
