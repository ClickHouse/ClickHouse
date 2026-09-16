-- Tags: long
-- `long` because the file builds four MergeTree tables of 300000-800000 rows and runs five timed
-- guards, nine `EXPLAIN PIPELINE` mirrors and three identity checks over them: 5.9 s on a debug build
-- with the CI jemalloc profiler config armed
-- (tests/config/config.d/jemalloc_enable_global_profiler.yaml), against a 60 s Fast test cap on a
-- runner shared with 24 workers. No sanitizer is excluded: the slowest single guarded query measured
-- 0.48 s server-side on that same build, so even a 20x sanitizer multiplier stays 12x under 120 s.
--
-- Regression test for a quadratic blowup in aggregation in order and DISTINCT in order with a
-- fixed-size GROUP BY key: a hashing state built per run of the sorting prefix batch-packed the whole
-- block's grouping key, so one block cost O(runs * block_size). Root cause and fix: see the commit
-- message. Each of the five ranged state constructions a query can select gets its own timed guard,
-- and every timed and identity statement has an `EXPLAIN PIPELINE` mirror so it cannot go vacuously
-- green when the optimizer stops choosing the in-order plan.
--
-- How every timed guard below is sized, stated once for all of them:
--
-- 1. `max_execution_time = 120` guards the quadratic blowup, not the linear runtime, and the two are
--    orders of magnitude apart: 0.48 s for the slowest guarded query fixed against 465 s or more
--    broken. `04537_aggregation_in_order_serialized_keys` settled on 120 s after 20 s flaked there.
-- 2. The aggregation and merge guards observe the limit between key intervals, where
--    `AggregatingInOrderTransform::consume` checks `isCancelled()`. `DistinctSortedStreamTransform`
--    packs every range of a block inside one `transform()` call and is cancelled only between calls,
--    so that guard has to exceed the limit with blocks to spare, and its `max_block_size` is sized by
--    the limit rather than by the packing.
-- 3. The two aggregation guards take the whole table in one 800000-row block: broken that is
--    `rows * block_rows = 6.4e11` key packs, about 465 s at the 0.72 ns per pack an official release
--    build measured, where a 200000-row block is 1.6e11, about 116 s and marginal on the fastest
--    build. The fixed path is O(rows) either way. The DISTINCT guard stays at 400000 (`3.2e11`, about
--    232 s): a larger block pushes its first limit observation past any harness budget.
-- 4. Three settings otherwise cap the reader's block, which is what carries the quadratic term, below
--    `max_block_size`: adaptive granularity aligns it to a few granules (measured 11264 rows at
--    `index_granularity = 1024`) and `preferred_block_size_bytes` caps it at ~65k rows for these row
--    widths through its 1 MB default. Disabling `index_granularity_bytes` also rules out Compact parts,
--    so the two `*_for_wide_part` thresholds are pinned to their always-Wide value to keep the server
--    from warning that it ignores them.
-- 5. Every timed and every identity statement is followed by an `EXPLAIN PIPELINE` assertion over the
--    same query text and the same settings, because otherwise it goes vacuous the day the optimizer
--    stops choosing the in-order plan: a timed one then passes by doing nothing, an identity one
--    compares two runs of the same path.
DROP TABLE IF EXISTS t_agg_in_order_fixed_keys;
CREATE TABLE t_agg_in_order_fixed_keys (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0,
             min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_agg_in_order_fixed_keys SELECT number, number * 7, number * 11 FROM numbers(800000);
OPTIMIZE TABLE t_agg_in_order_fixed_keys FINAL;

-- Zero aggregates, so `Aggregator::executeImpl` builds its consecutive-keys-caching `Method::State`.
SELECT count() FROM (SELECT a, b FROM t_agg_in_order_fixed_keys GROUP BY a, b)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
         max_threads = 1, max_block_size = 800000,
         aggregation_in_order_max_block_bytes = 50000000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0,
         max_execution_time = 120;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, b FROM t_agg_in_order_fixed_keys GROUP BY a, b
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 1, max_block_size = 800000,
             aggregation_in_order_max_block_bytes = 50000000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

-- Exactly one `count()` sets `Aggregator::is_simple_count`, which turns the consecutive-keys cache off
-- and routes to the `Method::StateNoCache` sibling of the state above. The outer aggregate has to
-- consume `n`: left unused, the inner `count()` is pruned and this becomes a copy of the guard above.
SELECT sum(n) FROM (SELECT a, b, count() AS n FROM t_agg_in_order_fixed_keys GROUP BY a, b)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
         max_threads = 1, max_block_size = 800000,
         aggregation_in_order_max_block_bytes = 50000000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0,
         max_execution_time = 120;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, b, count() AS n FROM t_agg_in_order_fixed_keys GROUP BY a, b
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 1, max_block_size = 800000,
             aggregation_in_order_max_block_bytes = 50000000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

-- `b, c` are two non-prefix UInt64 columns, so the set method is the same prepared-keys one, and with
-- `a` unique the ranges are single rows. The block size is larger here because this transform packs
-- every range of a block inside one `transform()` call: at 400000 the first limit evaluation lands at
-- 316.8 s of a 655.0 s query, 200000 leaves it at 378.9 s (3.2x, under the margin the others keep) and
-- 800000 makes it one block, where nothing evaluates the limit at all and the query ran past 1200 s.
SELECT count() FROM (SELECT DISTINCT a, b, c FROM t_agg_in_order_fixed_keys)
SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1,
         max_block_size = 400000, preferred_block_size_bytes = 0,
         preferred_max_column_in_block_size_bytes = 0, max_execution_time = 120;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b, c FROM t_agg_in_order_fixed_keys
    SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1,
             max_block_size = 400000, preferred_block_size_bytes = 0,
             preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%DistinctSortedStreamTransform%';

DROP TABLE t_agg_in_order_fixed_keys;

-- Runs of eight equal `a`, so every state is built over a sub-range and packs its keys per row. A
-- per-row packer whose byte layout disagrees with the batch packer returns a different key for the same
-- row; `sum(v)` makes the check sensitive to which row each key came from, `count()` would not be.

DROP TABLE IF EXISTS t_agg_in_order_fixed_keys_runs;
CREATE TABLE t_agg_in_order_fixed_keys_runs (a UInt64, b UInt64, v UInt64, d UInt8) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0,
             min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_agg_in_order_fixed_keys_runs
    SELECT intDiv(number, 8), number * 7, number * 11, number % 8 FROM numbers(300000);
OPTIMIZE TABLE t_agg_in_order_fixed_keys_runs FINAL;

SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE SELECT a, b, count() AS c, sum(v) AS s FROM t_agg_in_order_fixed_keys_runs GROUP BY a, b
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 1, max_block_size = 200000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0
)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT
(
    SELECT sum(cityHash64(a, b, c, s)) FROM
    (
        SELECT a, b, count() AS c, sum(v) AS s FROM t_agg_in_order_fixed_keys_runs GROUP BY a, b
        SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
                 max_threads = 1, max_block_size = 200000,
                 preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0
    )
)
=
(
    SELECT sum(cityHash64(a, b, c, s)) FROM
    (
        SELECT a, b, count() AS c, sum(v) AS s FROM t_agg_in_order_fixed_keys_runs GROUP BY a, b
        SETTINGS optimize_aggregation_in_order = 0, optimize_read_in_order = 0
    )
);

-- `GROUP BY d, a` is `UInt8, UInt64`, so clause order is not longest-first order and the keys land at
-- different byte offsets in the two layouts. This identity is the end-to-end guard that the per-row
-- packer reproduces the batch layout, which `a, b` cannot see: it is not a duplicate of it.

SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE SELECT d, a, count() AS c, sum(v) AS s FROM t_agg_in_order_fixed_keys_runs GROUP BY d, a
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 1, max_block_size = 200000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0
)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT
(
    SELECT sum(cityHash64(d, a, c, s)) FROM
    (
        SELECT d, a, count() AS c, sum(v) AS s FROM t_agg_in_order_fixed_keys_runs GROUP BY d, a
        SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
                 max_threads = 1, max_block_size = 200000,
                 preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0
    )
)
=
(
    SELECT sum(cityHash64(d, a, c, s)) FROM
    (
        SELECT d, a, count() AS c, sum(v) AS s FROM t_agg_in_order_fixed_keys_runs GROUP BY d, a
        SETTINGS optimize_aggregation_in_order = 0, optimize_read_in_order = 0
    )
);

DROP TABLE t_agg_in_order_fixed_keys_runs;

-- An aggregate projection keyed on a superset of the query's keys is what reaches the merging path:
-- reading `a, m, b` in order while grouping by `a, b` leaves `a` as the only ordered prefix, so with
-- `a` unique the runs are single rows. `force_optimize_projection` keeps both guards honest: without a
-- projection the query takes the executing path instead, and now it fails outright. `p` serves the
-- `sum(v)` guard and `pc` the `count()` one - the same `is_simple_count` split, in `mergeStreamsImpl`.

DROP TABLE IF EXISTS t_agg_in_order_fixed_keys_merge;
CREATE TABLE t_agg_in_order_fixed_keys_merge (a UInt64, m UInt64, b UInt64, v UInt64,
        PROJECTION p (SELECT a, m, b, sum(v) GROUP BY a, m, b),
        PROJECTION pc (SELECT a, m, b, count() GROUP BY a, m, b))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0,
             min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_agg_in_order_fixed_keys_merge SELECT number, number, number * 7, number FROM numbers(800000);
OPTIMIZE TABLE t_agg_in_order_fixed_keys_merge FINAL;

SELECT count() FROM (SELECT a, b, sum(v) FROM t_agg_in_order_fixed_keys_merge GROUP BY a, b)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, optimize_use_projections = 1,
         force_optimize_projection = 1, enable_parallel_replicas = 0, max_threads = 1,
         max_block_size = 800000, aggregation_in_order_max_block_bytes = 50000000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0,
         max_execution_time = 120;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, b, sum(v) FROM t_agg_in_order_fixed_keys_merge GROUP BY a, b
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, optimize_use_projections = 1,
             force_optimize_projection = 1, enable_parallel_replicas = 0, max_threads = 1,
             max_block_size = 800000, aggregation_in_order_max_block_bytes = 50000000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT sum(n) FROM (SELECT a, b, count() AS n FROM t_agg_in_order_fixed_keys_merge GROUP BY a, b)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, optimize_use_projections = 1,
         force_optimize_projection = 1, enable_parallel_replicas = 0, max_threads = 1,
         max_block_size = 800000, aggregation_in_order_max_block_bytes = 50000000,
         preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0,
         max_execution_time = 120;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT a, b, count() AS n FROM t_agg_in_order_fixed_keys_merge GROUP BY a, b
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, optimize_use_projections = 1,
             force_optimize_projection = 1, enable_parallel_replicas = 0, max_threads = 1,
             max_block_size = 800000, aggregation_in_order_max_block_bytes = 50000000,
             preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

DROP TABLE t_agg_in_order_fixed_keys_merge;

-- Runs of eight equal `a` repeat the first row's key inside the run: each range's state packs its keys
-- per row, and a per-row packer whose byte layout disagrees with the batch packer maps a repeated row
-- to a different key, so the row is emitted a second time instead of recognised as a duplicate.

DROP TABLE IF EXISTS t_distinct_in_order_fixed_keys;
CREATE TABLE t_distinct_in_order_fixed_keys (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0,
             min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_distinct_in_order_fixed_keys
    SELECT intDiv(number, 8) AS a, [1, 2, 1, 3, 2, 4, 3, 1][(number % 8) + 1] AS b, b * 7
    FROM numbers(300000);
OPTIMIZE TABLE t_distinct_in_order_fixed_keys FINAL;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b, c FROM t_distinct_in_order_fixed_keys
    SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1,
             max_block_size = 200000, preferred_block_size_bytes = 0,
             preferred_max_column_in_block_size_bytes = 0, enable_parallel_replicas = 0)
WHERE explain ILIKE '%DistinctSortedStreamTransform%';

SELECT
(
    SELECT sum(cityHash64(a, b, c)) FROM
    (
        SELECT DISTINCT a, b, c FROM t_distinct_in_order_fixed_keys
        SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1,
                 max_block_size = 200000, preferred_block_size_bytes = 0,
                 preferred_max_column_in_block_size_bytes = 0, enable_parallel_replicas = 0
    )
)
=
(
    SELECT sum(cityHash64(a, b, c)) FROM
    (
        SELECT DISTINCT a, b, c FROM t_distinct_in_order_fixed_keys
        SETTINGS optimize_distinct_in_order = 0, optimize_read_in_order = 0
    )
);

-- The identity above cannot see a state that covers too few rows: the final `DistinctTransform` above
-- the sorted one removes whatever duplicates such a state lets through. So count the rows the sorted
-- transform itself emitted - with one state per range covering exactly its range, that is the number
-- of distinct rows and no more. The three `log_*` settings only route the measurement to the log.
SELECT count() FROM
(
    SELECT DISTINCT a, b, c FROM t_distinct_in_order_fixed_keys
)
SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1, max_threads = 1,
         max_block_size = 200000, preferred_block_size_bytes = 0,
         preferred_max_column_in_block_size_bytes = 0, enable_parallel_replicas = 0,
         log_processors_profiles = 1, log_queries = 1,
         log_comment = '05217_distinct_in_order_fixed_keys';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

WITH
(
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment = '05217_distinct_in_order_fixed_keys'
      AND event_date >= yesterday() AND event_time >= now() - 600
    ORDER BY event_time_microseconds DESC LIMIT 1
) AS id
SELECT sum(output_rows) = (SELECT uniqExact((a, b, c)) FROM t_distinct_in_order_fixed_keys)
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND query_id = id AND name = 'DistinctSortedStreamTransform';

DROP TABLE t_distinct_in_order_fixed_keys;
