-- The `aggregation_in_order_shuffle` optimization must not change the order in which the rows of one group
-- reach the aggregate function, so aggregates that are a pure function of that order (`groupArray`, `any`,
-- `anyLast`) return exactly the same result as the ordinary (funnel) aggregation-in-order for the same read
-- pipeline.
--
-- The reshuffle merges the scattered sub-streams with `MergingSortedTransform` on
-- `InputOrderInfo::sort_description_for_merging`, which is always a prefix of the `GROUP BY` keys. All rows of
-- one group therefore compare equal in that merge, and `SortCursor` breaks such ties by input index, so the
-- merge can only concatenate whole per-input runs in input order - exactly what
-- `FinishAggregatingInOrderAlgorithm::addToAggregation` does on the funnel path. The interesting case is a
-- `GROUP BY` key coarser than the read order (`intDiv(k, 10)` over a table sorted by `k`) with streams that
-- overlap in the key range, which is covered below by overlapping parts and by partitions that all span the
-- whole key range.
--
-- What is *not* asserted is an identical result for an aggregate that also depends on how the partial states
-- are combined, i.e. `sum` over `Float*`: the funnel accumulates one partial sum per input stream and then
-- merges those states, while the shuffle accumulates the merged row sequence in a single pass, so the two
-- build a different addition tree over the same rows in the same order and can differ in the last bits. That
-- is not specific to this optimization - on the funnel path alone, `sum(toFloat64(v) / 3)` already differs
-- between `max_threads = 8` and `max_threads = 2`, exactly as it does for the default hash aggregation.
--
-- Both arms of every comparison pin `max_threads` and `read_in_order_two_level_merge_threshold`, since those
-- decide how many sorted streams the in-order read produces and whether it inserts a preliminary merge. The
-- per-group row order of aggregation-in-order depends on that stream composition already without this
-- optimization: with `read_in_order_two_level_merge_threshold = 2`, plain `optimize_aggregation_in_order`
-- returns a different `groupArray` for `max_threads = 8` than for `max_threads = 1`. Comparing arms of
-- different stream composition would therefore assert a property that the ordinary aggregation-in-order does
-- not have either.
--
-- The compared value is a `groupBitXor` checksum over the groups (as in 04511) rather than a hash of a
-- `groupArray` over them, because the shuffle deliberately does not preserve the order *between* groups: the
-- checksum is insensitive to that, while still being sensitive to the row order inside each group, which is
-- what is asserted here.

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

-- The shuffle path must actually be planned for each shape compared below, otherwise the comparisons would be
-- vacuous. This covers both stream compositions: with a preliminary merge (threshold 2) and without it.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10) AS g, groupArray(v) FROM t_aio_shuffle_order GROUP BY g
      SETTINGS max_threads = 8, aggregation_in_order_shuffle = 1, read_in_order_two_level_merge_threshold = 2);

SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10) AS g, groupArray(v) FROM t_aio_shuffle_order GROUP BY g
      SETTINGS max_threads = 8, aggregation_in_order_shuffle = 1, read_in_order_two_level_merge_threshold = 1000);

SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10) AS g, groupArray(v) FROM t_aio_shuffle_order_parts GROUP BY g
      SETTINGS max_threads = 17, aggregation_in_order_shuffle = 1, read_in_order_two_level_merge_threshold = 2);

SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10) AS g, groupArray(v) FROM t_aio_shuffle_order_parts GROUP BY g
      SETTINGS max_threads = 17, aggregation_in_order_shuffle = 1, read_in_order_two_level_merge_threshold = 1000);

SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, groupArray(v) FROM t_aio_shuffle_order_parts GROUP BY k
      SETTINGS max_threads = 4, aggregation_in_order_shuffle = 1, read_in_order_two_level_merge_threshold = 2);

-- Shuffle == funnel, byte for byte, for order-dependent aggregates over a `GROUP BY` key coarser than the
-- read order, for both stream compositions.
SELECT
    (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order GROUP BY g)
        SETTINGS max_threads = 8, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order GROUP BY g)
        SETTINGS max_threads = 8, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 0);

SELECT
    (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order GROUP BY g)
        SETTINGS max_threads = 8, read_in_order_two_level_merge_threshold = 1000, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order GROUP BY g)
        SETTINGS max_threads = 8, read_in_order_two_level_merge_threshold = 1000, aggregation_in_order_shuffle = 0);

SELECT
    (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order_parts GROUP BY g)
        SETTINGS max_threads = 17, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order_parts GROUP BY g)
        SETTINGS max_threads = 17, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 0);

SELECT
    (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order_parts GROUP BY g)
        SETTINGS max_threads = 17, read_in_order_two_level_merge_threshold = 1000, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(g, a, x, y)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order_parts GROUP BY g)
        SETTINGS max_threads = 17, read_in_order_two_level_merge_threshold = 1000, aggregation_in_order_shuffle = 0);

-- The same for the full sort key, where a group cannot span a coarser key run.
SELECT
    (SELECT groupBitXor(cityHash64(k, a, x, y)) FROM
        (SELECT k, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order_parts GROUP BY k)
        SETTINGS max_threads = 4, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(k, a, x, y)) FROM
        (SELECT k, groupArray(v) AS a, any(v) AS x, anyLast(v) AS y
         FROM t_aio_shuffle_order_parts GROUP BY k)
        SETTINGS max_threads = 4, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 0);

-- The equalities above are not vacuous: the compared value does depend on the row order inside a group. Each
-- group's array is a concatenation of one run per input stream and is therefore not in `v` order, so making it
-- order-insensitive with `arraySort` changes the result.
SELECT
    (SELECT groupBitXor(cityHash64(g, a)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a FROM t_aio_shuffle_order GROUP BY g)
        SETTINGS max_threads = 8, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 1)
 != (SELECT groupBitXor(cityHash64(g, a)) FROM
        (SELECT intDiv(k, 10) AS g, arraySort(groupArray(v)) AS a FROM t_aio_shuffle_order GROUP BY g)
        SETTINGS max_threads = 8, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 1);

SELECT
    (SELECT groupBitXor(cityHash64(g, a)) FROM
        (SELECT intDiv(k, 10) AS g, groupArray(v) AS a FROM t_aio_shuffle_order_parts GROUP BY g)
        SETTINGS max_threads = 17, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 1)
 != (SELECT groupBitXor(cityHash64(g, a)) FROM
        (SELECT intDiv(k, 10) AS g, arraySort(groupArray(v)) AS a FROM t_aio_shuffle_order_parts GROUP BY g)
        SETTINGS max_threads = 17, read_in_order_two_level_merge_threshold = 2, aggregation_in_order_shuffle = 1);

DROP TABLE t_aio_shuffle_order;
DROP TABLE t_aio_shuffle_order_parts;
