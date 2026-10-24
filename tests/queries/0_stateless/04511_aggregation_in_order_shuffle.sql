-- Tests the `aggregation_in_order_shuffle` optimization: aggregation-in-order that repartitions the sorted
-- input by the hash of the GROUP BY keys into independent shards. The results must be identical to both the
-- default hash aggregation and the ordinary (funnel) aggregation-in-order.

-- With parallel replicas the aggregation plan changes (memory-bound merging) and the shuffle is not applied,
-- so pin the plain single-replica plan to keep the EXPLAIN PIPELINE check below meaningful.
SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle for the whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_aio_shuffle;

CREATE TABLE t_aio_shuffle (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

-- Three overlapping parts (each spans the whole key range) so that reading in order yields several
-- sorted streams that the shuffle has to merge per shard.
SYSTEM STOP MERGES t_aio_shuffle;
INSERT INTO t_aio_shuffle SELECT number % 5000 AS k, rand64() FROM numbers(50000);
INSERT INTO t_aio_shuffle SELECT number % 5000 AS k, rand64() FROM numbers(50000);
INSERT INTO t_aio_shuffle SELECT number % 5000 AS k, rand64() FROM numbers(50000);

-- The shuffle path must actually be used (BufferedShardByHashTransform in the pipeline).
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle GROUP BY k
      SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The setting enables virtual rows only when the read-in-order plan can produce them. This query has no
-- virtual-row transform, so the reshuffle is safe and must be planned based on the actual pipeline state.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle GROUP BY k
      SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
          read_in_order_use_virtual_row = 1);

-- Correctness: order-independent checksum of the result must match the default aggregation.
-- 1) full key
SELECT
    (SELECT groupBitXor(cityHash64(k, s, c)) FROM (SELECT k, sum(v) s, count() c FROM t_aio_shuffle GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, s, c)) FROM (SELECT k, sum(v) s, count() c FROM t_aio_shuffle GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

-- 2) monotonic prefix of the sort key, with several aggregate functions incl. a stateful one
SELECT
    (SELECT groupBitXor(cityHash64(g, s, c, u, mn, mx)) FROM
        (SELECT intDiv(k, 10) g, sum(v) s, count() c, uniqExact(v) u, min(v) mn, max(v) mx FROM t_aio_shuffle GROUP BY g)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(g, s, c, u, mn, mx)) FROM
        (SELECT intDiv(k, 10) g, sum(v) s, count() c, uniqExact(v) u, min(v) mn, max(v) mx FROM t_aio_shuffle GROUP BY g)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

-- 3) low cardinality (few groups, long runs) - exercises the drain path and must not hang
SELECT
    (SELECT groupBitXor(cityHash64(g, s, c)) FROM (SELECT intDiv(k, 1000) g, sum(v) s, count() c FROM t_aio_shuffle GROUP BY g)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 16)
  = (SELECT groupBitXor(cityHash64(g, s, c)) FROM (SELECT intDiv(k, 1000) g, sum(v) s, count() c FROM t_aio_shuffle GROUP BY g)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 16);

-- Shuffle must also agree with the ordinary (funnel) aggregation-in-order.
SELECT
    (SELECT groupBitXor(cityHash64(k, s, c)) FROM (SELECT k, sum(v) s, count() c FROM t_aio_shuffle GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, s, c)) FROM (SELECT k, sum(v) s, count() c FROM t_aio_shuffle GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 0, max_threads = 8);

DROP TABLE t_aio_shuffle;
