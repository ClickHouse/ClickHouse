-- The `aggregation_in_order_shuffle` optimization must not change the per-group row order that the ordinary
-- (funnel) aggregation-in-order produces, so order-dependent aggregates (`groupArray`, `any`, `anyLast`, and
-- `sum` over `Float64`, which is not associative) return exactly the same result with and without it, and
-- also the same result as the single-threaded in-order pipeline.
--
-- The reshuffle merges the scattered sub-streams with `MergingSortedTransform` on
-- `InputOrderInfo::sort_description_for_merging`, which is always a prefix of the `GROUP BY` keys. All rows of
-- one group therefore compare equal in that merge, so it can only concatenate whole per-input runs in input
-- order - exactly what `FinishAggregatingInOrderAlgorithm::addToAggregation` does on the funnel path. The
-- interesting case is a `GROUP BY` key coarser than the read order (`intDiv(k, 10)` over a table sorted by
-- `k`) with streams that overlap in the key range, which is covered below by overlapping parts and by
-- partitions that all span the whole key range.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;

-- The stateless-test profile sets a huge `max_rows_to_group_by` by default, which disables the shuffle.
SET max_rows_to_group_by = 0;

SET optimize_aggregation_in_order = 1;
SET max_block_size = 32;

DROP TABLE IF EXISTS t_aio_shuffle_order;
DROP TABLE IF EXISTS t_aio_shuffle_order_parts;

-- All partitions span the whole key range, so the in-order read produces streams that overlap in `k`.
CREATE TABLE t_aio_shuffle_order (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY k % 5 ORDER BY k
    SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_aio_shuffle_order;
INSERT INTO t_aio_shuffle_order SELECT number % 1000, number FROM numbers(10000);
INSERT INTO t_aio_shuffle_order SELECT number % 1000, number + 100000 FROM numbers(10000);

-- Overlapping parts of different key ranges.
CREATE TABLE t_aio_shuffle_order_parts (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_aio_shuffle_order_parts;
INSERT INTO t_aio_shuffle_order_parts SELECT number % 1000, number FROM numbers(5000);
INSERT INTO t_aio_shuffle_order_parts SELECT number % 1000, number + 100000 FROM numbers(5000);
INSERT INTO t_aio_shuffle_order_parts SELECT number % 997, number + 200000 FROM numbers(5000);
INSERT INTO t_aio_shuffle_order_parts SELECT number % 1013, number + 300000 FROM numbers(5000);

-- The shuffle path must actually be planned for the `GROUP BY` key that is coarser than the read order,
-- otherwise the comparisons below would be vacuous.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10) AS g, groupArray(v) FROM t_aio_shuffle_order GROUP BY g
      SETTINGS max_threads = 8, aggregation_in_order_shuffle = 1);

SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10) AS g, groupArray(v) FROM t_aio_shuffle_order_parts GROUP BY g
      SETTINGS max_threads = 8, aggregation_in_order_shuffle = 1);

-- Shuffle == funnel == single-threaded in-order, byte for byte, for order-dependent aggregates.
SELECT
    (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order GROUP BY g ORDER BY g)
        SETTINGS max_threads = 8, aggregation_in_order_shuffle = 1)
  = (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order GROUP BY g ORDER BY g)
        SETTINGS max_threads = 8, aggregation_in_order_shuffle = 0);

SELECT
    (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order GROUP BY g ORDER BY g)
        SETTINGS max_threads = 8, aggregation_in_order_shuffle = 1)
  = (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order GROUP BY g ORDER BY g)
        SETTINGS max_threads = 1);

SELECT
    (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order_parts GROUP BY g ORDER BY g)
        SETTINGS max_threads = 17, aggregation_in_order_shuffle = 1)
  = (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order_parts GROUP BY g ORDER BY g)
        SETTINGS max_threads = 17, aggregation_in_order_shuffle = 0);

SELECT
    (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order_parts GROUP BY g ORDER BY g)
        SETTINGS max_threads = 17, aggregation_in_order_shuffle = 1)
  = (SELECT cityHash64(groupArray(tuple(g, a, l, f))) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, anyLast(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order_parts GROUP BY g ORDER BY g)
        SETTINGS max_threads = 1);

-- The same for the full sort key, where a group cannot span a coarser key run.
SELECT
    (SELECT cityHash64(groupArray(tuple(k, a, l, f))) FROM
        (SELECT k, groupArray(v) AS a, any(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order_parts GROUP BY k ORDER BY k)
        SETTINGS max_threads = 4, aggregation_in_order_shuffle = 1)
  = (SELECT cityHash64(groupArray(tuple(k, a, l, f))) FROM
        (SELECT k, groupArray(v) AS a, any(v) AS l, sum(toFloat64(v) / 3) AS f
         FROM t_aio_shuffle_order_parts GROUP BY k ORDER BY k)
        SETTINGS max_threads = 4, aggregation_in_order_shuffle = 0);

DROP TABLE t_aio_shuffle_order;
DROP TABLE t_aio_shuffle_order_parts;
