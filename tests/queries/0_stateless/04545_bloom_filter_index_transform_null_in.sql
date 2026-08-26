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
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in WHERE x IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in WHERE x IN ('5', '500') SETTINGS transform_null_in = 1;

-- globalNullIn is classified separately from nullIn, so it needs its own pruning assertion.
SELECT 'String: GLOBAL IN null-free set prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in WHERE x GLOBAL IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in WHERE x GLOBAL IN ('5', '500') SETTINGS transform_null_in = 1;

-- A subquery set takes its element types from the subquery header, a literal set from the tuple,
-- so the type check sees a differently-built set on this path.
SELECT 'String: IN subquery of the same type prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in WHERE x IN (SELECT toString(arrayJoin(['5', '500']))) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in WHERE x IN (SELECT toString(arrayJoin(['5', '500']))) SETTINGS transform_null_in = 1;

SELECT 'String: GLOBAL IN subquery of the same type prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in WHERE x GLOBAL IN (SELECT toString(arrayJoin(['5', '500']))) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in WHERE x GLOBAL IN (SELECT toString(arrayJoin(['5', '500']))) SETTINGS transform_null_in = 1;

SELECT 'String: `=` prunes with transform_null_in=1 (was already working)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in WHERE x = '5' SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
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
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', '500') SETTINGS transform_null_in = 1;

SELECT 'LowCardinality: IN null-free set prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 1;

SELECT 'LowCardinality(Nullable): IN null-free set prunes with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE c IN ('5', '500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE c IN ('5', '500') SETTINGS transform_null_in = 1;

-- When the set contains a NULL, nullIn also matches NULL rows: the index must NOT prune,
-- and the result must include the NULL rows (10 rows: number % 100 = 0 -> {0,100,...,900}).
SELECT 'Nullable: IN set with NULL does not prune, result includes NULL rows';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', NULL) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE a IN ('5', NULL) SETTINGS transform_null_in = 1;

-- Correctness cross-check: results are identical with transform_null_in=0 and =1 for a
-- null-free set over a non-null column value.
SELECT 'Correctness: null-free set, results equal for transform_null_in 0 vs 1';
SELECT
    (SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 0) =
    (SELECT count() FROM t_bf_null_in_n WHERE b IN ('5', '500') SETTINGS transform_null_in = 1);

-- A hand-written `nullIn` reaches this branch regardless of transform_null_in, so the set it
-- carries is NULL-free whenever the setting is off and pruning is then sound.
SELECT 'Nullable: hand-written nullIn null-free set prunes with transform_null_in=0';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE nullIn(a, ('5', '500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE nullIn(a, ('5', '500')) SETTINGS transform_null_in = 0;

SELECT 'Nullable: hand-written globalNullIn null-free set prunes with transform_null_in=0';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_n WHERE globalNullIn(a, ('5', '500')) SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT count() FROM t_bf_null_in_n WHERE globalNullIn(a, ('5', '500')) SETTINGS transform_null_in = 0;

-- A NULL literal is not a set element when transform_null_in = 0, so no NULL row can be pruned away.
SELECT 'Nullable: hand-written nullIn with a NULL literal keeps skip-index and full-scan results equal at transform_null_in=0';
SELECT
    (SELECT count() FROM t_bf_null_in_n WHERE nullIn(a, ('5', NULL)) SETTINGS transform_null_in = 0, use_skip_indexes = 1) =
    (SELECT count() FROM t_bf_null_in_n WHERE nullIn(a, ('5', NULL)) SETTINGS transform_null_in = 0, use_skip_indexes = 0);
SELECT count() FROM t_bf_null_in_n WHERE nullIn(a, ('5', NULL)) SETTINGS transform_null_in = 0;

DROP TABLE t_bf_null_in_n;

-- Array column: whole-array equality bloom filter hashing is not sound for granules that mix
-- empty and non-empty arrays, so the index must NOT be used for `nullIn` on Array columns.
-- Regression: without the Array guard, `x IN ([])` would wrongly prune the granule holding [].
DROP TABLE IF EXISTS t_bf_null_in_arr;
CREATE TABLE t_bf_null_in_arr (x Array(UInt32), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_null_in_arr VALUES ([]), ([1]), ([2]), ([3]);

SELECT 'Array: IN does not prune with transform_null_in=1 (unsound array hashing)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_arr WHERE x IN ([]) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'Array: IN empty array result is correct with transform_null_in=1';
SELECT count() FROM t_bf_null_in_arr WHERE x IN ([]) SETTINGS transform_null_in = 1;

DROP TABLE t_bf_null_in_arr;

-- Type-incompatible set: the index hashes the set value cast to the index type, while execution
-- casts each column value to the set type. Those two casts are not inverse, so a matching row can
-- hash differently from what the index searches for ('01' -> UInt8 1 -> '1'). Pruning such a
-- granule loses that row, so the index must NOT be used. Types are compared modulo Nullable /
-- LowCardinality, so the wrapper cases above are unaffected.
DROP TABLE IF EXISTS t_bf_null_in_ty;
CREATE TABLE t_bf_null_in_ty (x String, INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_bf_null_in_ty SELECT toString(number) FROM numbers(1000);

SELECT 'Type mismatch: String index vs integer set does not prune with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_ty WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'Type mismatch: query still raises the conversion error with transform_null_in=1';
SELECT count() FROM t_bf_null_in_ty WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }

DROP TABLE t_bf_null_in_ty;

-- Lossy round trip: every value below parses as UInt8, so nothing throws, and '01' / '+1' both
-- equal 1 at execution while hashing differently from the index's '1'. Pruning would silently
-- drop them, so this arm fails if the type check above is ever removed.
DROP TABLE IF EXISTS t_bf_null_in_lossy;
CREATE TABLE t_bf_null_in_lossy (x String, INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_bf_null_in_lossy VALUES ('01'), ('+1'), ('2'), ('3');

SELECT 'Lossy cast: results are identical with and without the skip index';
SELECT
    (SELECT count() FROM t_bf_null_in_lossy WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1, use_skip_indexes = 0) =
    (SELECT count() FROM t_bf_null_in_lossy WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1);
SELECT 'Lossy cast: both matching rows are returned';
SELECT count() FROM t_bf_null_in_lossy WHERE x IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1;

DROP TABLE t_bf_null_in_lossy;

-- Tuple lhs: the NULL-free and type checks above are per-column, and the recursive `tuple(...)`
-- branch matches each element against its own index without the set, so those checks cannot be
-- applied there. Reusing the single-column check would prune granules holding a row that a
-- NULL-carrying tuple set matches. `(a, b)` holds 2 in both granules, so only `b` discriminates.
DROP TABLE IF EXISTS t_bf_null_in_tup;
CREATE TABLE t_bf_null_in_tup
(
    a Nullable(Int32),
    b Nullable(Int32),
    INDEX idx_a a TYPE bloom_filter GRANULARITY 1,
    INDEX idx_b b TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_null_in_tup VALUES (2, 50), (9, 51), (2, NULL), (9, 52);

SELECT 'Tuple: IN does not prune with transform_null_in=1';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_null_in_tup WHERE (a, b) IN ((2, NULL)) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));
SELECT 'Tuple: NULL-carrying set returns the same rows with and without the skip index';
SELECT
    (SELECT count() FROM t_bf_null_in_tup WHERE (a, b) IN ((2, NULL)) SETTINGS transform_null_in = 1, use_skip_indexes = 0) =
    (SELECT count() FROM t_bf_null_in_tup WHERE (a, b) IN ((2, NULL)) SETTINGS transform_null_in = 1);

DROP TABLE t_bf_null_in_tup;
