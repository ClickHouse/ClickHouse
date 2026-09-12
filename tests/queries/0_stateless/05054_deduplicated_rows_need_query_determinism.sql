-- A function that is not deterministic in scope of query must not be executed over a representation
-- in which equal rows are stored once (replicated columns, sparse values, LowCardinality dictionary)
-- and then mapped back onto the logical rows.

SET allow_deprecated_error_prone_window_functions = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET enable_dynamic_type = 1;
SET enable_variant_type = 1;
-- The running family restarts per block, so the oracle depends on all rows landing in one block.
SET max_threads = 1;
SET max_block_size = 65505;

-- ---------------------------- replicated columns (ARRAY JOIN) ----------------------------
-- Each arm runs twice, with enable_lazy_columns_replication = 1 and = 0. The compressed
-- representation is the only difference, so the two must agree, and agree on the materialized value.
-- The argument types are wide or variable-width because the ARRAY JOIN producer consults
-- isLazyReplicationUseful(), which declines to replicate a fixed-width column of at most 8 bytes, so
-- a narrow argument is vacuous here. That heuristic belongs to this producer alone: the join section
-- below replicates a narrow column regardless.

DROP TABLE IF EXISTS t_rep;
CREATE TABLE t_rep (st AggregateFunction(sum, UInt32), s String, i128 Int128, u32 UInt32, arr Array(UInt8))
ENGINE = Memory;
INSERT INTO t_rep SELECT initializeAggregation('sumState', toUInt32(number + 1)),
    concat('v', toString(number)), toInt128(10 * (number + 1)), toUInt32(10 * (number + 1)),
    [toUInt8(1), toUInt8(1)] FROM numbers(2);

SELECT 'shape', groupArray(s) FROM (SELECT s FROM t_rep ARRAY JOIN arr LIMIT 100);

SELECT 'runningAccumulate', groupArray(runningAccumulate(st))
FROM (SELECT st FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'runningAccumulate', groupArray(runningAccumulate(st))
FROM (SELECT st FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 0;

-- The two-argument form: both arguments are replicated with the same indexes column.
SELECT 'runningAccumulate grouped', groupArray(runningAccumulate(st, s))
FROM (SELECT st, s FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'runningAccumulate grouped', groupArray(runningAccumulate(st, s))
FROM (SELECT st, s FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 0;

SELECT 'neighbor', groupArray(neighbor(s, 1))
FROM (SELECT s FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'neighbor', groupArray(neighbor(s, 1))
FROM (SELECT s FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 0;

SELECT 'runningDifference', groupArray(runningDifference(i128))
FROM (SELECT i128 FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'runningDifference', groupArray(runningDifference(i128))
FROM (SELECT i128 FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 0;

-- Same template as runningDifference but a second registered name, so one being green proves
-- nothing about the other.
SELECT 'runningDifferenceStartingWithFirstValue', groupArray(runningDifferenceStartingWithFirstValue(i128))
FROM (SELECT i128 FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'runningDifferenceStartingWithFirstValue', groupArray(runningDifferenceStartingWithFirstValue(i128))
FROM (SELECT i128 FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 0;

-- Control: a function with no argument cannot be affected, and documents the boundary.
SELECT 'control rowNumberInBlock', groupArray(n)
FROM (SELECT rowNumberInBlock() AS n FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'control rowNumberInBlock', groupArray(n)
FROM (SELECT rowNumberInBlock() AS n FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 0;

-- Control: a query-deterministic function over the same replicated column keeps the optimization.
SELECT 'control length', groupArray(length(s))
FROM (SELECT s FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'control length', groupArray(length(s))
FROM (SELECT s FROM t_rep ARRAY JOIN arr LIMIT 100) SETTINGS enable_lazy_columns_replication = 0;

-- ---------------------------- replicated columns (join lazy indexing) ----------------------------
-- A join is a second producer of replicated columns, and it replicates a NARROW fixed-width column
-- that ARRAY JOIN would leave alone: when the query plan enables lazy indexing on the join, that
-- short-circuits the width heuristic. enable_lazy_columns_replication does not gate this path, so the
-- arms flip query_plan_min_columns_for_join_lazy_indexing instead, which is what decides whether the
-- optimization runs at all. It changes the representation and nothing else.
-- The plan enables it only for a join below a small enough limit whose probe side has at least that
-- many columns, and only while the replication does not grow the row count. Hence the fixture: the
-- third left row matches twice and the second matches nothing, so three left rows join to three rows.

SET query_plan_max_limit_for_join_lazy_indexing = 1000;
-- The probe side must stay the wide table, or it has too few columns and the arms below stop
-- discriminating while still passing. Both of these can move the build side, and the runner
-- randomizes them.
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
-- A runtime filter drops the probe row that matches nothing before the join, which makes the
-- replication growing and sends these narrow columns down the eager path, so the arms below stop
-- discriminating. The runner randomizes this, and a zero minimum probe size skips the size guard.
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS l_join;
DROP TABLE IF EXISTS r_join;
CREATE TABLE l_join (b DateTime, e DateTime, n UInt32, k1 UInt32, k2 UInt32) ENGINE = Memory;
CREATE TABLE r_join (k1 UInt32, k2 UInt32) ENGINE = Memory;
INSERT INTO l_join VALUES ('2025-01-01 00:00:00', '2025-01-03 00:00:00', 10, 1, 1),
    ('2025-01-05 00:00:00', '2025-01-06 00:00:00', 99, 2, 2),
    ('2025-01-02 00:00:00', '2025-01-04 00:00:00', 30, 3, 3);
INSERT INTO r_join VALUES (1, 1), (3, 3), (3, 3);

-- Pins the shape the arms depend on. 99 belongs to the row that never joins, so an arm reporting it
-- is reading rows that are not in the result.
SELECT 'join shape', groupArray(n) FROM (SELECT n FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10);

-- DateTime is four bytes, so the ARRAY JOIN section above cannot reach runningConcurrency at all.
SELECT 'join runningConcurrency', groupArray(c)
FROM (SELECT runningConcurrency(b, e) AS c FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 3;
SELECT 'join runningConcurrency', groupArray(c)
FROM (SELECT runningConcurrency(b, e) AS c FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 0;

-- Narrow arguments on two carriers the section above can only reach with a wide one.
SELECT 'join runningDifference', groupArray(d)
FROM (SELECT runningDifference(n) AS d FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 3;
SELECT 'join runningDifference', groupArray(d)
FROM (SELECT runningDifference(n) AS d FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 0;

SELECT 'join neighbor', groupArray(x)
FROM (SELECT neighbor(n, 1) AS x FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 3;
SELECT 'join neighbor', groupArray(x)
FROM (SELECT neighbor(n, 1) AS x FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 0;

-- Control: a query-deterministic function over the same replicated column keeps the optimization.
SELECT 'control join length', groupArray(x)
FROM (SELECT length(toString(n)) AS x FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 3;
SELECT 'control join length', groupArray(x)
FROM (SELECT length(toString(n)) AS x FROM l_join INNER JOIN r_join USING (k1, k2) LIMIT 10)
SETTINGS query_plan_min_columns_for_join_lazy_indexing = 0;

-- ---------------------------- Dynamic / Variant executable adaptors ----------------------------
-- These go through ExecutableFunctionDynamicAdaptor / ExecutableFunctionVariantAdaptor, which no
-- other arm exercises. A single alternative keeps the adaptors from partitioning rows by runtime
-- type, which is a separate concern and would confound the arm. The result type is asserted so the
-- arm cannot silently stop taking the adaptor path, via any() rather than GROUP BY: the old
-- analyzer rejects a bare toTypeName(x) beside an aggregate with NOT_AN_AGGREGATE.

DROP TABLE IF EXISTS t_dv;
CREATE TABLE t_dv (st AggregateFunction(sum, UInt32), dyn Dynamic, var Variant(UInt64), arr Array(UInt8))
ENGINE = Memory;
INSERT INTO t_dv SELECT initializeAggregation('sumState', toUInt32(number + 1)),
    CAST(toUInt64(number) AS Dynamic), CAST(toUInt64(number) AS Variant(UInt64)),
    [toUInt8(1), toUInt8(1)] FROM numbers(2);

SELECT 'dynamic', any(toTypeName(x)), groupArray(x)
FROM (SELECT runningAccumulate(st, dyn) AS x FROM (SELECT st, dyn FROM t_dv ARRAY JOIN arr LIMIT 100))
SETTINGS enable_lazy_columns_replication = 1;
SELECT 'dynamic', any(toTypeName(x)), groupArray(x)
FROM (SELECT runningAccumulate(st, dyn) AS x FROM (SELECT st, dyn FROM t_dv ARRAY JOIN arr LIMIT 100))
SETTINGS enable_lazy_columns_replication = 0;

SELECT 'variant', any(toTypeName(x)), groupArray(x)
FROM (SELECT runningAccumulate(st, var) AS x FROM (SELECT st, var FROM t_dv ARRAY JOIN arr LIMIT 100))
SETTINGS enable_lazy_columns_replication = 1;
SELECT 'variant', any(toTypeName(x)), groupArray(x)
FROM (SELECT runningAccumulate(st, var) AS x FROM (SELECT st, var FROM t_dv ARRAY JOIN arr LIMIT 100))
SETTINGS enable_lazy_columns_replication = 0;

-- ---------------------------- sparse columns ----------------------------
-- Two tables with identical data; only the serialization differs. No session setting is involved:
-- any MergeTree column at or above ratio_of_defaults_for_sparse_serialization is sparse by itself.
-- The representation needs no setting, but reaching the function with it does: reading in physical
-- order hands the function the column as stored, while a sorting step materializes it first, so the
-- arms below stop discriminating. The runner randomizes this.
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_dense;
CREATE TABLE t_sparse (id UInt32, x UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0, min_bytes_for_wide_part = 0;
CREATE TABLE t_dense (id UInt32, x UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.1, min_bytes_for_wide_part = 0;
INSERT INTO t_sparse SELECT number, if(number = 2, 5, 0), if(number = 2, 'zz', '') FROM numbers(5);
INSERT INTO t_dense SELECT number, if(number = 2, 5, 0), if(number = 2, 'zz', '') FROM numbers(5);

-- The fixture must prove it armed the predicate: without this a default change turns the arm into a
-- silent no-op that still passes.
SELECT 'serialization', table, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_sparse', 't_dense') AND column IN ('x', 's') AND active
ORDER BY table, column;

SELECT 'sparse runningDifference', id, runningDifference(x) FROM t_sparse ORDER BY id;
SELECT 'dense  runningDifference', id, runningDifference(x) FROM t_dense ORDER BY id;
SELECT 'sparse neighbor', id, neighbor(s, 1) FROM t_sparse ORDER BY id;
SELECT 'dense  neighbor', id, neighbor(s, 1) FROM t_dense ORDER BY id;

-- Control: a query-deterministic function over the same sparse column. Per row, because the order of
-- a bare groupArray over a sparse column is not tied to the subquery's ORDER BY.
SELECT 'control sparse length', id, length(s) FROM t_sparse ORDER BY id;
SELECT 'control dense  length', id, length(s) FROM t_dense ORDER BY id;

-- ---------------------------- LowCardinality dictionary ----------------------------

DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (id UInt32, n LowCardinality(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc SELECT number, arrayElement([1, 7, 1, 7], number + 1) FROM numbers(4);

-- The result type must stay LowCardinality: fixing this seam by suppressing the LowCardinality return
-- type would be a user-visible metadata change stacked on a value fix.
SELECT 'types', toTypeName(n), toTypeName(runningDifference(n)) FROM t_lc LIMIT 1;
SELECT 'lc   runningDifference', id, runningDifference(n) FROM t_lc ORDER BY id;
SELECT 'full runningDifference', id, runningDifference(m) FROM (SELECT id, CAST(n AS UInt32) AS m FROM t_lc ORDER BY id);

-- Control: a query-deterministic function over the same dictionary keeps the optimization.
SELECT 'control lc plus one', id, n + 1 FROM t_lc ORDER BY id;

-- ---------------------------- generators over a LowCardinality dictionary ----------------------------
-- These need no setting at all. Values are unpredictable, so assert how many distinct values appear
-- and never the values themselves. The dictionary holds two entries, so the dictionary path can only
-- produce two distinct values and the materialized path four. The assertion is a threshold and not an
-- equality because these functions promise nothing about uniqueness, so demanding four distinct values
-- out of four would rest on a guarantee that does not exist. The CAST arm is the control.

DROP TABLE IF EXISTS t_gen;
CREATE TABLE t_gen (id UInt32, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_gen SELECT number, ['a', 'a', 'bb', 'bb'][number + 1] FROM numbers(4);

SELECT 'generateUUIDv4 lc',   any(toTypeName(u)), uniqExact(u) > 2 FROM (SELECT generateUUIDv4(s) AS u FROM t_gen);
SELECT 'generateUUIDv4 cast', any(toTypeName(u)), uniqExact(u) > 2 FROM (SELECT generateUUIDv4(CAST(s AS String)) AS u FROM t_gen);
SELECT 'rand lc',             any(toTypeName(v)), uniqExact(v) > 2 FROM (SELECT rand(s) AS v FROM t_gen);
SELECT 'rand cast',           any(toTypeName(v)), uniqExact(v) > 2 FROM (SELECT rand(CAST(s AS String)) AS v FROM t_gen);
SELECT 'randomString lc',     uniqExact(v) > 2 FROM (SELECT randomString(length(s) + 8) AS v FROM t_gen);

DROP TABLE t_rep;
DROP TABLE l_join;
DROP TABLE r_join;
DROP TABLE t_dv;
DROP TABLE t_sparse;
DROP TABLE t_dense;
DROP TABLE t_lc;
DROP TABLE t_gen;
