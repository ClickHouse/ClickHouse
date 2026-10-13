-- Tags: no-parallel-replicas, long
-- no-parallel-replicas: the no-ORDER-BY promotion needs `LimitStep` directly
-- above `AggregatingStep` in a single-stage plan, so under parallel replicas it
-- never engages and the `EXPLAIN` / `AggregationTopKRowsSkipped` assertions
-- that it fired would fail (results would still be correct).

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

-- Correctness of enable_group_by_top_k_optimization for `GROUP BY ... LIMIT`
-- without ORDER BY.  The optimizer promotes this shape into the sorted one by
-- synthesizing a SortingStep over all GROUP BY keys (any N groups are a valid
-- answer, so any deterministic order works); the sort also discards any group
-- a heap eviction left partially aggregated.  Row counts and per-group
-- aggregate values must match the unoptimized query.

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;

DROP TABLE IF EXISTS t_gbylimit_noob;

CREATE TABLE t_gbylimit_noob
(
    a UInt32,
    b UInt32,
    c String,
    d Nullable(UInt32),
    val UInt64
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_gbylimit_noob
SELECT
    (number % 500)::UInt32,
    (number % 200)::UInt32,
    toString(number % 300),
    if(number % 97 = 0, NULL, (number % 400)::UInt32),
    number
FROM numbers(100000);

-- `enable_group_by_top_k_optimization` takes effect per query, not per
-- subquery: inside a single statement the last `SETTINGS` clause wins for the
-- whole query, so joining an optimized `LIMIT`ed query against an unoptimized
-- full aggregation in one statement runs both sides in the same mode.  The
-- unoptimized reference answers are therefore materialized by their own
-- statements below.
DROP TABLE IF EXISTS gt_noob_a;
DROP TABLE IF EXISTS gt_noob_ab;
DROP TABLE IF EXISTS gt_noob_abc;
DROP TABLE IF EXISTS gt_noob_c;
DROP TABLE IF EXISTS gt_noob_d;
DROP TABLE IF EXISTS gt_noob_xy;

CREATE TABLE gt_noob_a (a UInt32, cnt UInt64, s UInt64) ENGINE = Memory;
CREATE TABLE gt_noob_ab (a UInt32, b UInt32, cnt UInt64, s UInt64) ENGINE = Memory;
CREATE TABLE gt_noob_abc (a UInt32, b UInt32, c String, cnt UInt64, s UInt64) ENGINE = Memory;
CREATE TABLE gt_noob_c (c String, cnt UInt64, s UInt64) ENGINE = Memory;
CREATE TABLE gt_noob_d (d Nullable(UInt32), cnt UInt64, s UInt64) ENGINE = Memory;
CREATE TABLE gt_noob_xy (x UInt32, y UInt32, cnt UInt64) ENGINE = Memory;

SET enable_group_by_top_k_optimization = 0;

INSERT INTO gt_noob_a SELECT a, count(), sum(val) FROM t_gbylimit_noob GROUP BY a;
INSERT INTO gt_noob_ab SELECT a, b, count(), sum(val) FROM t_gbylimit_noob GROUP BY a, b;
INSERT INTO gt_noob_abc SELECT a, b, c, count(), sum(val) FROM t_gbylimit_noob GROUP BY a, b, c;
INSERT INTO gt_noob_c SELECT c, count(), sum(val) FROM t_gbylimit_noob GROUP BY c;
INSERT INTO gt_noob_d SELECT d, count(), sum(val) FROM t_gbylimit_noob GROUP BY d;
INSERT INTO gt_noob_xy
SELECT (number % 100000)::UInt32 AS x, (number % 50000)::UInt32 AS y, count()
FROM numbers(2000000) GROUP BY x, y;

SET enable_group_by_top_k_optimization = 1;

SELECT 'single_key_row_count';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'single_key_aggregates';
SELECT count() FROM (
    SELECT a, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a LIMIT 10
) AS optimized
LEFT JOIN gt_noob_a AS full USING (a)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'composite_two_key_row_count';
SELECT count() FROM (
    SELECT a, b, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b LIMIT 15
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'composite_two_key_aggregates';
SELECT count() FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b LIMIT 15
) AS optimized
LEFT JOIN gt_noob_ab AS full USING (a, b)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'composite_three_key_row_count';
SELECT count() FROM (
    SELECT a, b, c, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b, c LIMIT 20
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'composite_three_key_aggregates';
SELECT count() FROM (
    SELECT a, b, c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b, c LIMIT 20
) AS optimized
LEFT JOIN gt_noob_abc AS full USING (a, b, c)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'nullable_key_row_count';
SELECT count() FROM (
    SELECT d, count() AS cnt FROM t_gbylimit_noob GROUP BY d LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'nullable_key_aggregates';
SELECT count() FROM (
    SELECT d, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY d LIMIT 10
) AS optimized
LEFT JOIN gt_noob_d AS full ON optimized.d IS NOT DISTINCT FROM full.d
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'string_key_row_count';
SELECT count() FROM (
    SELECT c, count() AS cnt FROM t_gbylimit_noob GROUP BY c LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'string_key_aggregates';
SELECT count() FROM (
    SELECT c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY c LIMIT 10
) AS optimized
LEFT JOIN gt_noob_c AS full USING (c)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'with_offset_row_count';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 5, 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'limit_exceeds_groups';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 1000
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'limit_one_row_count';
SELECT count() FROM (
    SELECT a, b, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b LIMIT 1
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'two_level_row_count';
SELECT count() FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count()
    FROM numbers(2000000) GROUP BY x, y LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'two_level_aggregates';
SELECT count() FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(2000000) GROUP BY x, y LIMIT 10
) AS optimized
LEFT JOIN gt_noob_xy AS full USING (x, y)
WHERE optimized.cnt != full.cnt;

SELECT 'negative_with_totals';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a WITH TOTALS LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

DROP TABLE t_gbylimit_noob;
DROP TABLE gt_noob_a;
DROP TABLE gt_noob_ab;
DROP TABLE gt_noob_abc;
DROP TABLE gt_noob_c;
DROP TABLE gt_noob_d;
DROP TABLE gt_noob_xy;

-- Plan shape: the no-ORDER-BY query gets a synthesized sort above the
-- aggregation, and the same Top-K annotation as the explicit-ORDER-BY query.

SET max_threads = 1;
SET optimize_trivial_group_by_limit_query = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'no ORDER BY: synthesized sort + Top-K annotation';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN actions = 1 SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

SELECT 'explicit ORDER BY: same Top-K annotation';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN actions = 1 SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting for GROUP BY top-K%';

-- Every returned group must carry complete aggregates, across aggregation
-- methods with different hash-table pruning capabilities.

DROP TABLE IF EXISTS gt_u32;
DROP TABLE IF EXISTS gt_comp;
DROP TABLE IF EXISTS gt_str;
DROP TABLE IF EXISTS gt_u8;
DROP TABLE IF EXISTS gt_date;
DROP TABLE IF EXISTS gt_multi;

CREATE TABLE gt_u32 (k UInt32, s UInt64) ENGINE = Memory;
CREATE TABLE gt_comp (a UInt32, b String, s UInt64) ENGINE = Memory;
CREATE TABLE gt_str (k String, s UInt64) ENGINE = Memory;
CREATE TABLE gt_u8 (k UInt8, s UInt64) ENGINE = Memory;
CREATE TABLE gt_date (k Date, s UInt64) ENGINE = Memory;
CREATE TABLE gt_multi (k UInt32, s UInt64) ENGINE = Memory;

SET enable_group_by_top_k_optimization = 0;

INSERT INTO gt_u32 SELECT k, sum(v) FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k;
INSERT INTO gt_comp SELECT a, b, sum(v) FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toString(number % 10) AS b, 1 AS v FROM numbers(4000)) GROUP BY a, b;
INSERT INTO gt_str SELECT k, sum(v) FROM (SELECT leftPad(toString(999 - (number % 1000)), 4, '0') AS k, 1 AS v FROM numbers(4000)) GROUP BY k;
INSERT INTO gt_u8 SELECT k, sum(v) FROM (SELECT toUInt8(255 - (number % 256)) AS k, 1 AS v FROM numbers(1024)) GROUP BY k;
INSERT INTO gt_date SELECT k, sum(v) FROM (SELECT toDate('2020-01-01') + (999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k;
INSERT INTO gt_multi SELECT k, sum(v) FROM (SELECT (999 - number % 1000)::UInt32 AS k, 1 AS v FROM numbers_mt(2000000)) GROUP BY k;

SET enable_group_by_top_k_optimization = 1;

SELECT 'UInt32 key (erasable hash table, heap + pruning active)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k LIMIT 5) AS l
    INNER JOIN gt_u32 AS f USING (k)
);

SELECT 'Composite key (pruning of composite heaps)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT a, b, sum(v) AS s FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toString(number % 10) AS b, 1 AS v FROM numbers(4000)) GROUP BY a, b LIMIT 5) AS l
    INNER JOIN gt_comp AS f USING (a, b)
);

SELECT 'String key (StringHashTable supports erase)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT leftPad(toString(999 - (number % 1000)), 4, '0') AS k, 1 AS v FROM numbers(4000)) GROUP BY k LIMIT 5) AS l
    INNER JOIN gt_str AS f USING (k)
);

SELECT 'UInt8 key (FixedHashTable cannot erase, heap runs skip-only)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toUInt8(255 - (number % 256)) AS k, 1 AS v FROM numbers(1024)) GROUP BY k LIMIT 5) AS l
    INNER JOIN gt_u8 AS f USING (k)
);

SELECT 'Date key (key16, FixedHashTable cannot erase, heap runs skip-only)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toDate('2020-01-01') + (999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k LIMIT 5) AS l
    INNER JOIN gt_date AS f USING (k)
);

-- The synthesized sort discards any group a heap eviction left partially
-- aggregated, so the heap stays active even with several independent
-- aggregation streams: a key evicted in one stream has >= N
-- better keys in that stream (hence globally), and the sort+limit drops it.
SELECT 'Multi-stream: every returned group complete';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT (999 - number % 1000)::UInt32 AS k, 1 AS v FROM numbers_mt(2000000)) GROUP BY k LIMIT 3) AS l
    INNER JOIN gt_multi AS f USING (k)
) SETTINGS max_threads = 8;

SELECT k FROM (SELECT (999 - number % 1000)::UInt32 AS k FROM numbers_mt(2000000)) GROUP BY k LIMIT 3
SETTINGS enable_group_by_top_k_optimization = 1, max_threads = 8, log_comment = '04500_multi' FORMAT Null;
SELECT k FROM (SELECT (999 - number % 1000)::UInt32 AS k FROM numbers(2000000)) GROUP BY k LIMIT 3
SETTINGS enable_group_by_top_k_optimization = 1, max_threads = 1, log_comment = '04500_single' FORMAT Null;
SELECT k FROM (SELECT toUInt8(255 - (number % 256)) AS k FROM numbers(100000)) GROUP BY k LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1, max_threads = 1, log_comment = '04500_uint8' FORMAT Null;
SYSTEM FLUSH LOGS query_log;

SELECT 'Heap active for multi-stream, single-stream, and skip-only methods';
SELECT
    sumIf(ProfileEvents['AggregationTopKRowsSkipped'], log_comment = '04500_multi') > 0 AS multi_heap_active,
    sumIf(ProfileEvents['AggregationTopKRowsSkipped'], log_comment = '04500_single') > 0 AS single_heap_active,
    sumIf(ProfileEvents['AggregationTopKRowsSkipped'], log_comment = '04500_uint8') > 0 AS skip_only_heap_active
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('04500_multi', '04500_single', '04500_uint8');

-- LowCardinality keys cannot prune the hash table, but the heap still runs in
-- skip-only mode and the synthesized sort discards evicted stragglers; the
-- query must stay within the memory limit and external aggregation must not
-- be force-disabled (~660MB working set without spilling).  The limits are
-- per-query, not session `SET`s: the verification query below reads
-- `system.query_log`, whose cost tracks the whole suite's log volume.

SELECT 'LowCardinality key under a memory limit (skip-only heap + spill)';
SELECT count() FROM
(
    SELECT k, count() AS c
    FROM (SELECT toLowCardinality(toString(number % 3000000)) AS k FROM numbers(9000000))
    GROUP BY k
    LIMIT 5
) SETTINGS log_comment = '04500_lowcard_spill', max_memory_usage = 300000000, max_bytes_before_external_group_by = 50000000;

SYSTEM FLUSH LOGS query_log;

SELECT sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0 AS skipped
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04500_lowcard_spill'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();

SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k LIMIT 5 SETTINGS max_memory_usage = 0, optimize_trivial_group_by_limit_query = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0) WHERE explain LIKE '%Top-K%';

DROP TABLE gt_u32;
DROP TABLE gt_comp;
DROP TABLE gt_str;
DROP TABLE gt_u8;
DROP TABLE gt_date;
DROP TABLE gt_multi;
