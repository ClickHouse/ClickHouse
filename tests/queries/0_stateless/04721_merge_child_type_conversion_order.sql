-- Tags: shard

CREATE TABLE t_neg (A Int64) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_neg SELECT -number FROM numbers(1000);
CREATE TABLE dist_neg AS t_neg ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_neg);
CREATE TABLE dist_one AS t_neg ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_neg);

-- A sibling whose type MATCHES the declared type, so the outer table sees no mismatch of its
-- own and the nested and aliased carriers below are genuinely exercised.
CREATE TABLE t_pos (A UInt64) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_pos SELECT number + 100000 FROM numbers(100);
CREATE TABLE dist_pos AS t_pos ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_pos);

CREATE TABLE m_retyped (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_one     (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_one$');
CREATE TABLE m_mixed   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^(dist_neg|dist_pos)$');
CREATE TABLE m_inner   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_outer   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^m_inner$');
CREATE TABLE a_inner ENGINE = Alias(currentDatabase(), 'm_inner');
CREATE TABLE m_alias   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^(a_inner|dist_pos)$');
CREATE TABLE m_ok      (`A` Int64)  ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_ok_u    (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_pos$');

-- Controls whose declared type DIFFERS from the child yet preserves order, so they detect
-- over-refusal. `Merge` derives both shapes itself through `getLeastSupertypeOrVariant` when no
-- column list is given, so refusing them would slow down ordinary tables that are correct today.
CREATE TABLE t_i32 (A Int32) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_i32 SELECT -number FROM numbers(1000);
CREATE TABLE dist_i32 AS t_i32 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_i32);
CREATE TABLE m_widen  (`A` Int64)           ENGINE = Merge(currentDatabase(), '^dist_i32$');
CREATE TABLE m_nullbl (`A` Nullable(Int64)) ENGINE = Merge(currentDatabase(), '^dist_neg$');

-- DISTINCT drops the per-shard duplication, so the expected sequence does not depend on how
-- many streams the pipeline happens to use. The sort stays at the top level: a subquery ORDER BY
-- is not preserved by its parent, which makes any nested form report false failures.
SELECT '-- smallest and largest values through a top-level ORDER BY';
SELECT DISTINCT A FROM m_retyped ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_retyped ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM m_one     ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_mixed   ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_mixed   ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM m_outer   ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_alias   ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_alias   ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM m_ok      ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_ok_u    ORDER BY A ASC  LIMIT 3;

SELECT '-- the reported DISTINCT query, both analyzers';
SELECT DISTINCT plus(0, A) AS v FROM m_retyped ORDER BY ALL ASC LIMIT 3 SETTINGS enable_analyzer = 1;
SELECT DISTINCT plus(0, A) AS v FROM m_retyped ORDER BY ALL ASC LIMIT 3 SETTINGS enable_analyzer = 0;
SELECT count(), min(v), max(v) FROM (SELECT DISTINCT plus(0, A) AS v FROM m_retyped);

SELECT '-- aggregate states must survive the child stage as well';
SELECT min(A), max(A) FROM m_retyped;
SELECT min(A), max(A) FROM m_outer;
SELECT min(A), max(A) FROM m_alias;
SELECT min(A), max(A) FROM m_ok;

SELECT '-- window sort with no top-level ORDER BY (the shape the fuzzer reported)';
SELECT max(s) FROM (SELECT sum(toInt64(A)) OVER (ORDER BY A ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM m_one);

SELECT '-- LIMIT BY and the negative LIMIT sibling';
SELECT DISTINCT A FROM m_retyped ORDER BY A ASC LIMIT 1 BY A LIMIT 3;
SELECT DISTINCT A FROM m_retyped ORDER BY A ASC LIMIT -3;

SELECT '-- UNION ALL forwards the claim upward';
SELECT count(), uniqExact(A) FROM (SELECT A FROM m_retyped UNION ALL SELECT A FROM m_ok_u);

SELECT '-- GROUP BY over the nested and aliased carriers';
SELECT count(), min(A), max(A) FROM (SELECT A, count() FROM m_outer GROUP BY A);
SELECT count(), min(A), max(A) FROM (SELECT A, count() FROM m_alias GROUP BY A);

SELECT '-- controls: matching types keep the stage pushed down, so the merge step survives';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT plus(0, A) FROM m_ok ORDER BY ALL ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT plus(0, A) FROM m_ok_u ORDER BY ALL ASC) WHERE explain ILIKE '%Merge sorted streams%';

-- Controls with DIFFERING but order-preserving types: these are the ones that detect over-refusal,
-- and the values must still be right, so a later over-broadening cannot hide behind the plan check.
SELECT '-- controls: differing but order-preserving types keep the stage too';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT plus(0, A) FROM m_widen ORDER BY ALL ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT plus(0, A) FROM m_nullbl ORDER BY ALL ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM m_widen  ORDER BY A ASC LIMIT 3;
SELECT DISTINCT A FROM m_nullbl ORDER BY A ASC LIMIT 3;

SELECT '-- a self-referential Merge must not hang';
CREATE TABLE m_self (`A` UInt64) ENGINE = Merge(currentDatabase(), '^m_self$');
SELECT count() FROM m_self;

-- Read-in-order rejects these casts through its own monotonicity analysis, so this case is
-- already correct before the fix. It guards that conclusion, it does not witness the fix.
SELECT '-- MergeTree child under read-in-order and aggregation-in-order';
CREATE TABLE m_mt (`A` UInt64) ENGINE = Merge(currentDatabase(), '^t_neg$');
SELECT DISTINCT A FROM m_mt ORDER BY A ASC LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT count(), min(A), max(A) FROM (SELECT A, count() FROM m_mt GROUP BY A SETTINGS optimize_aggregation_in_order = 1);

SELECT '-- an ALIAS column crosses the same boundary';
-- Alias expansion on the analyzer path is not gated on the stage, and the alias expressions are
-- added below the converting DAG, so an alias whose child type disagrees carries the defect while
-- every physical column agrees. Reachable through the non-analyzer path here: on the analyzer path
-- a `Merge` over a `Distributed` with an alias fails with NOT_FOUND_COLUMN_IN_BLOCK regardless of
-- the types, which is a separate defect. `optimize_respect_aliases` is pinned for the same reason:
-- with it off, reading such an alias at `FetchColumns` fails with UNKNOWN_IDENTIFIER on plain
-- master too, for identical declared and child types.
CREATE TABLE t_alias (A Int64, x Int64 ALIAS A) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_alias SELECT -number FROM numbers(1000);
CREATE TABLE dist_alias (A Int64, x Int64 ALIAS A)
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_alias);
CREATE TABLE m_alias_col (`A` Int64, `x` UInt64) ENGINE = Merge(currentDatabase(), '^dist_alias$');
CREATE TABLE m_alias_ok  (`A` Int64, `x` Int64)  ENGINE = Merge(currentDatabase(), '^dist_alias$');
SELECT DISTINCT x FROM m_alias_col ORDER BY x ASC  LIMIT 3 SETTINGS enable_analyzer = 0, optimize_respect_aliases = 1;
SELECT DISTINCT x FROM m_alias_col ORDER BY x DESC LIMIT 3 SETTINGS enable_analyzer = 0, optimize_respect_aliases = 1;
SELECT DISTINCT x FROM m_alias_ok  ORDER BY x ASC  LIMIT 3 SETTINGS enable_analyzer = 0, optimize_respect_aliases = 1;
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT x FROM m_alias_col ORDER BY x ASC) WHERE explain ILIKE '%Merge sorted streams%' SETTINGS enable_analyzer = 0, optimize_respect_aliases = 1;
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT x FROM m_alias_ok ORDER BY x ASC) WHERE explain ILIKE '%Merge sorted streams%' SETTINGS enable_analyzer = 0, optimize_respect_aliases = 1;

SELECT '-- order-preserving conversions inside supported wrappers keep the stage';
-- `Merge` derives all of these itself for a column-list-less table, so refusing them would cost
-- pushdown on ordinary correct tables. Each returns 1 only when the conversion is accepted.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_lc_i32 (A LowCardinality(Int32)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_lc_i32 SELECT -number % 50 FROM numbers(200);
CREATE TABLE dist_lc_i32 AS t_lc_i32 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_lc_i32);
CREATE TABLE t_e8 (A Enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_e8 SELECT ['a', 'b', 'c', 'd'][1 + number % 4] FROM numbers(200);
CREATE TABLE dist_e8 AS t_e8 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_e8);

CREATE TABLE m_lc_widen (`A` LowCardinality(Int64))  ENGINE = Merge(currentDatabase(), '^dist_lc_i32$');
CREATE TABLE m_e8_null  (`A` Nullable(Enum16('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4))) ENGINE = Merge(currentDatabase(), '^dist_e8$');
CREATE TABLE m_e8_int   (`A` Nullable(Int16))        ENGINE = Merge(currentDatabase(), '^dist_e8$');
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_lc_widen ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_e8_null  ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_e8_int   ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
-- Every value assertion is LIMITed like the ones above. Unbounded, they also expose a separate
-- pre-existing defect: a `Merge` over a `Distributed` emits one DISTINCT block per shard, so each
-- value appears twice. That reproduces with identical declared and child types, on plain master.
SELECT DISTINCT A FROM m_lc_widen ORDER BY A ASC LIMIT 3;
SELECT DISTINCT A FROM m_e8_null  ORDER BY A ASC LIMIT 3;
SELECT DISTINCT A FROM m_e8_int   ORDER BY A ASC LIMIT 3;

SELECT '-- the same wrapper shape with the declared type left to Merge to derive';
-- No column list, so the type the children are cast to is whatever `getLeastSupertypeOrVariant`
-- picks. That derivation is what makes these shapes ordinary rather than exotic, and pinning it
-- here keeps the arm honest if the resolver ever changes.
CREATE TABLE t_lc_str (A LowCardinality(String)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_lc_str SELECT toString(number % 50) FROM numbers(200);
CREATE TABLE dist_lc_str AS t_lc_str ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_lc_str);
CREATE TABLE t_null_str (A Nullable(String)) ENGINE = MergeTree ORDER BY A SETTINGS allow_nullable_key = 1;
INSERT INTO t_null_str SELECT toString(number % 50) FROM numbers(200);
CREATE TABLE dist_null_str AS t_null_str ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_null_str);
CREATE TABLE m_derived ENGINE = Merge(currentDatabase(), '^(dist_lc_str|dist_null_str)$');
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'm_derived' AND name = 'A';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_derived ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM m_derived ORDER BY A ASC LIMIT 3;

SELECT '-- an Enum narrowing stays refused even though the target contains the source values';
-- `contains` accepts this pair: the name sets are disjoint, so it falls back to testing the
-- TRUNCATED value, and 128 truncated to Int8 is -128, which the target does hold. The cast
-- truncates the data the same way, so ascending 0, 128 arrives as 0, -128, i.e. reversed.
CREATE TABLE t_e16_wide (A Enum16('a' = 0, 'b' = 128)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_e16_wide SELECT if(number % 2, 'a', 'b') FROM numbers(200);
CREATE TABLE dist_e16_wide AS t_e16_wide ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_e16_wide);
CREATE TABLE m_e16_narrow (`A` Enum8('c' = 0, 'd' = -128)) ENGINE = Merge(currentDatabase(), '^dist_e16_wide$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_e16_narrow ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM m_e16_narrow ORDER BY A ASC LIMIT 2;

SELECT '-- widening into the wide integer types keeps the stage as well';
-- `getLeastSupertype` derives these for an ordinary column-list-less `Merge` over mixed integer
-- widths, so refusing them would cost pushdown on tables that are correct today.
CREATE TABLE m_wide_i128 (`A` Int128)  ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_wide_i256 (`A` Int256)  ENGINE = Merge(currentDatabase(), '^dist_pos$');
CREATE TABLE m_wide_u128 (`A` UInt128) ENGINE = Merge(currentDatabase(), '^dist_pos$');
CREATE TABLE m_e8_i128   (`A` Int128)  ENGINE = Merge(currentDatabase(), '^dist_e8$');
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_wide_i128 ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_wide_i256 ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_wide_u128 ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_e8_i128   ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM m_wide_i128 ORDER BY A ASC LIMIT 3;
SELECT DISTINCT A FROM m_wide_i256 ORDER BY A ASC LIMIT 3;
SELECT DISTINCT A FROM m_wide_u128 ORDER BY A ASC LIMIT 3;
SELECT DISTINCT A FROM m_e8_i128   ORDER BY A ASC LIMIT 3;

-- A signed source into a wider UNSIGNED target still wraps, so it stays refused.
CREATE TABLE m_wide_u256_neg (`A` UInt256) ENGINE = Merge(currentDatabase(), '^dist_neg$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_wide_u256_neg ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';

SELECT '-- element widening inside Array keeps the stage';
-- No column list, so `getLeastSupertype` recurses into `Array` and derives the target itself.
-- The declared type is asserted, so a resolver change reddens instead of going vacuous.
CREATE TABLE t_arr_i32 (A Array(Int32)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_arr_i32 SELECT [toInt32(-number)] FROM numbers(200);
CREATE TABLE dist_arr_i32 AS t_arr_i32 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_arr_i32);
CREATE TABLE t_arr_i64 (A Array(Int64)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_arr_i64 SELECT [toInt64(number) + 100000] FROM numbers(100);
CREATE TABLE dist_arr_i64 AS t_arr_i64 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_arr_i64);
CREATE TABLE m_arr_derived ENGINE = Merge(currentDatabase(), '^(dist_arr_i32|dist_arr_i64)$');
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'm_arr_derived' AND name = 'A';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_arr_derived ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM m_arr_derived ORDER BY A ASC LIMIT 3;

SELECT '-- an order-breaking element conversion, or a one-sided Array, stays refused';
CREATE TABLE m_arr_flip   (`A` Array(UInt64)) ENGINE = Merge(currentDatabase(), '^dist_arr_i64$');
CREATE TABLE m_arr_narrow (`A` Array(Int32))  ENGINE = Merge(currentDatabase(), '^dist_arr_i64$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_arr_flip   ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_arr_narrow ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';

SELECT '-- Tuple and Map need their own analysis, so they stay refused';
CREATE TABLE t_tup_i32 (A Tuple(Int32)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_tup_i32 SELECT tuple(toInt32(-number)) FROM numbers(200);
CREATE TABLE dist_tup_i32 AS t_tup_i32 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_tup_i32);
CREATE TABLE m_tup (`A` Tuple(Int64)) ENGINE = Merge(currentDatabase(), '^dist_tup_i32$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_tup ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
CREATE TABLE t_map_i32 (A Map(String, Int32)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_map_i32 SELECT map('k', toInt32(-number)) FROM numbers(200);
CREATE TABLE dist_map_i32 AS t_map_i32 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_map_i32);
CREATE TABLE m_map (`A` Map(String, Int64)) ENGINE = Merge(currentDatabase(), '^dist_map_i32$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_map ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';

SELECT '-- an equal-width signedness flip inside LowCardinality stays refused';
CREATE TABLE t_lc_i64 (A LowCardinality(Int64)) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_lc_i64 SELECT -number % 50 FROM numbers(200);
CREATE TABLE dist_lc_i64 AS t_lc_i64 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_lc_i64);
CREATE TABLE m_lc_flip (`A` LowCardinality(UInt64)) ENGINE = Merge(currentDatabase(), '^dist_lc_i64$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM m_lc_flip ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM m_lc_flip ORDER BY A ASC LIMIT 3;
