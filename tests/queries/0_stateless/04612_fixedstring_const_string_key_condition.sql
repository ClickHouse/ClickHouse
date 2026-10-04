-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN indexes output differs for parallel replicas.

-- Regression test for KeyCondition mis-pruning when a FixedString constant is
-- compared against a String (or differently-typed FixedString) key column.
--
-- `toFixedString('abc', N) = string_col` is stored as a String Field holding the
-- N padded bytes ('abc\0...\0'). KeyCondition used to build the point range
-- ['abc\0...\0', 'abc\0...\0'] and prune a granule whose min/max is ['abc','abc'],
-- dropping matching rows (wrong count()). The primary-key exact-count path and the
-- minmax skip-index path could then disagree on the surviving granules and hit the
-- `chassert(i == len)` in optimizeUseAggregateProjection. Zero-padded comparison
-- semantics (toFixedString('abc', 5) = 'abc' is true) mean the match is the family
-- 'abc' + '\0'*, not a single point, so index analysis must decline here.

SET allow_suspicious_fixed_string_types = 1;

DROP TABLE IF EXISTS t_gt;
DROP TABLE IF EXISTS t_pk_str;
DROP TABLE IF EXISTS t_pk_inj;
DROP TABLE IF EXISTS t_skip;

-- Ground truth: no key/skip pruning possible.
CREATE TABLE t_gt (p String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
INSERT INTO t_gt SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);

-- String primary key.
CREATE TABLE t_pk_str (p String) ENGINE = MergeTree ORDER BY p SETTINGS index_granularity = 1024;
INSERT INTO t_pk_str SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);

-- Injective-function primary key (this always transformed the constant correctly).
CREATE TABLE t_pk_inj (p String) ENGINE = MergeTree ORDER BY reverse(tuple(bin(p), reverse(p))) SETTINGS index_granularity = 1024;
INSERT INTO t_pk_inj SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);

-- minmax skip index on the String column.
CREATE TABLE t_skip (p String, INDEX idx p TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
INSERT INTO t_skip SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);

-- All four must return the ground-truth count 9000: a wider FixedString constant
-- matches every 'abc' row (trailing NUL padding is ignored by the comparison).
SELECT count() FROM t_gt      WHERE toFixedString('abc', 257) = p;
SELECT count() FROM t_pk_str  WHERE toFixedString('abc', 257) = p;
SELECT count() FROM t_pk_inj  WHERE toFixedString('abc', 257) = p;
SELECT count() FROM t_skip    WHERE toFixedString('abc', 257) = p;

-- With the exact-count projection path enabled (the original chassert(i == len) failure).
SELECT count() FROM t_skip WHERE toFixedString('abc', 257) = p GROUP BY ALL
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;

SELECT count() FROM t_pk_inj WHERE toFixedString('abc', 257) = p GROUP BY ALL
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;

-- EXPLAIN indexes = 1 forces the exact-count analysis that hit the chassert(i == len);
-- select a stable marker so plan-format churn does not make the test brittle.
SELECT countIf(explain LIKE '%ReadFromMergeTree%') > 0
FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_skip WHERE toFixedString('abc', 257) = p GROUP BY ALL
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1
);

-- notEquals: complement must also be correct (1000 non-'abc' rows).
SELECT count() FROM t_gt      WHERE toFixedString('abc', 257) != p;
SELECT count() FROM t_pk_str  WHERE toFixedString('abc', 257) != p;

-- A String column value that carries a trailing NUL byte still matches the
-- FixedString constant (family match), and must not be pruned away.
DROP TABLE IF EXISTS t_nul_gt;
DROP TABLE IF EXISTS t_nul_pk;
CREATE TABLE t_nul_gt (p String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
CREATE TABLE t_nul_pk (p String) ENGINE = MergeTree ORDER BY p SETTINGS index_granularity = 1;
INSERT INTO t_nul_gt VALUES ('abc'), ('abc\0'), ('abc\0\0'), ('abd'), ('abc\0x');
INSERT INTO t_nul_pk VALUES ('abc'), ('abc\0'), ('abc\0\0'), ('abd'), ('abc\0x');
SELECT count() FROM t_nul_gt WHERE toFixedString('abc', 257) = p;
SELECT count() FROM t_nul_pk WHERE toFixedString('abc', 257) = p;

-- Same-width / same-type FixedString comparison keeps working (exact match, no family).
DROP TABLE IF EXISTS t_fs;
CREATE TABLE t_fs (p FixedString(3)) ENGINE = MergeTree ORDER BY p SETTINGS index_granularity = 1024;
INSERT INTO t_fs SELECT toFixedString(if(number < 9000, 'abc', 'xyz'), 3) FROM numbers(10000);
SELECT count() FROM t_fs WHERE p = toFixedString('abc', 3);

-- Differently-sized FixedString key.
DROP TABLE IF EXISTS t_fs5;
CREATE TABLE t_fs5 (p FixedString(5)) ENGINE = MergeTree ORDER BY p SETTINGS index_granularity = 1024;
INSERT INTO t_fs5 SELECT toFixedString(if(number < 9000, 'abc', concat('x', toString(number % 90 + 10))), 5) FROM numbers(10000);
-- Constant narrower than the key (3 <= 5): pads to exactly one key value, pruning stays correct.
SELECT count() FROM t_fs5 WHERE p = toFixedString('abc', 3);
-- Constant wider than the key (257 > 5): must still be correct (index analysis declines).
SELECT count() FROM t_fs5 WHERE p = toFixedString('abc', 257);

-- A non-literal length argument (e.g. toLowCardinality(257)) makes the constant
-- LowCardinality(FixedString(257)) instead of FixedString(257). The guard must strip
-- LowCardinality, otherwise the wrapper slips past it and re-introduces the mis-prune
-- (wrong count()) and the chassert(i == len) failure.
DROP TABLE IF EXISTS t_lc_gt;
DROP TABLE IF EXISTS t_lc_inj;
CREATE TABLE t_lc_gt (p String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
INSERT INTO t_lc_gt SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);
CREATE TABLE t_lc_inj (p String) ENGINE = MergeTree ORDER BY reverse(tuple(bin(p), reverse(p)))
SETTINGS index_granularity = 1024, add_minmax_index_for_string_columns = 1;
INSERT INTO t_lc_inj SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);
SELECT count() FROM t_lc_gt  WHERE toFixedString('abc', toLowCardinality(257)) = p;
SELECT count() FROM t_lc_inj WHERE toFixedString('abc', toLowCardinality(257)) = p;
SELECT count() FROM t_lc_inj WHERE toFixedString('abc', toLowCardinality(257)) = p GROUP BY ALL
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;
SELECT countIf(explain LIKE '%ReadFromMergeTree%') > 0
FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_lc_inj WHERE toFixedString('abc', toLowCardinality(257)) = p GROUP BY ALL
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1
);

-- `tryGetConstant` peels only an outer `Nullable`, so a `LowCardinality(Nullable(FixedString(N)))`
-- constant reaches the guard with the inner `Nullable` intact. Stripping `LowCardinality` alone
-- leaves `Nullable(FixedString(N))`, `isFixedString()` stays false, and the constant slips past the
-- guard back onto the mis-pruning path. `Nullable(FixedString(N))` alone is already peeled by
-- `tryGetConstant`, but is asserted here too. Both must return the ground-truth 9000.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_lcn_gt;
DROP TABLE IF EXISTS t_lcn_skip;
CREATE TABLE t_lcn_gt (p String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
INSERT INTO t_lcn_gt SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);
CREATE TABLE t_lcn_skip (p String, INDEX idx p TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY p SETTINGS index_granularity = 1024;
INSERT INTO t_lcn_skip SELECT if(number < 9000, 'abc', concat('x', toString(number))) FROM numbers(10000);
SELECT count() FROM t_lcn_gt   WHERE p = CAST(toFixedString('abc', 257) AS LowCardinality(Nullable(FixedString(257))));
SELECT count() FROM t_lcn_skip WHERE p = CAST(toFixedString('abc', 257) AS LowCardinality(Nullable(FixedString(257))));
SELECT count() FROM t_lcn_gt   WHERE p = CAST(toFixedString('abc', 257) AS Nullable(FixedString(257)));
SELECT count() FROM t_lcn_skip WHERE p = CAST(toFixedString('abc', 257) AS Nullable(FixedString(257)));
SELECT count() FROM t_lcn_skip WHERE p = CAST(toFixedString('abc', 257) AS LowCardinality(Nullable(FixedString(257)))) GROUP BY ALL
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_gt;
DROP TABLE IF EXISTS t_pk_str;
DROP TABLE IF EXISTS t_pk_inj;
DROP TABLE IF EXISTS t_skip;
DROP TABLE IF EXISTS t_nul_gt;
DROP TABLE IF EXISTS t_nul_pk;
DROP TABLE IF EXISTS t_fs;
DROP TABLE IF EXISTS t_fs5;
DROP TABLE IF EXISTS t_lc_gt;
DROP TABLE IF EXISTS t_lc_inj;
DROP TABLE IF EXISTS t_lcn_gt;
DROP TABLE IF EXISTS t_lcn_skip;
