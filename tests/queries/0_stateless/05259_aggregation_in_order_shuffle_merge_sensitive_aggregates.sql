-- Aggregates whose result depends on how partial states are combined (`State` / `Merge`), not only on the order of
-- the rows, are not guaranteed to return the same value with and without `aggregation_in_order_shuffle`: the
-- ordinary (funnel) aggregation-in-order accumulates one partial state per input stream and merges those states,
-- while the shuffle accumulates every group in a single state over the merged row sequence and never merges.
-- The same holds between different values of `max_threads` without this optimization. What the shuffle must
-- preserve is the meaning of the result, which is asserted here for aggregates of different kinds:
-- * set-valued: `groupUniqArray` returns the same set of values, in an unspecified order;
-- * exact: `uniqExact` and `quantileExact` return exactly the same value;
-- * tie-sensitive: `argMax` returns one of the values attained at the maximum, not necessarily the same one;
-- * approximate: `quantile` returns a value within the range of the group.
-- See also 05055_aggregation_in_order_shuffle_order_dependent_aggregates for the aggregates that depend only on the
-- order of the rows.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;

-- The stateless-test profile sets a huge `max_rows_to_group_by` by default, which disables the shuffle.
SET max_rows_to_group_by = 0;

SET optimize_aggregation_in_order = 1;
SET max_block_size = 32;
SET max_threads = 3;
SET read_in_order_two_level_merge_threshold = 1000;

DROP TABLE IF EXISTS t_aio_shuffle_merge_sensitive;

-- All partitions span the whole key range, so the in-order read produces streams that overlap in `k`, and many
-- values of `v` are tied at the maximum of `c` in every group.
CREATE TABLE t_aio_shuffle_merge_sensitive (k UInt64, v UInt64, c UInt8) ENGINE = MergeTree PARTITION BY k % 5 ORDER BY k
    SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_aio_shuffle_merge_sensitive;
INSERT INTO t_aio_shuffle_merge_sensitive SELECT number % 100, (number * 7919) % 211, number % 3 FROM numbers(1000);
INSERT INTO t_aio_shuffle_merge_sensitive SELECT number % 100, (number * 104729) % 223, number % 3 FROM numbers(1000);

-- The shuffle path must actually be planned, otherwise the comparisons below would be vacuous.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10) AS g, groupUniqArray(v) FROM t_aio_shuffle_merge_sensitive GROUP BY g
      SETTINGS aggregation_in_order_shuffle = 1);

-- Order-insensitive checksums over the groups, since the shuffle does not preserve the order between groups.
SELECT
    (SELECT groupBitXor(cityHash64(g, arraySort(u), ue, qe))
     FROM (SELECT intDiv(k, 10) AS g, groupUniqArray(v) AS u, uniqExact(v) AS ue, quantileExact(0.5)(v) AS qe
           FROM t_aio_shuffle_merge_sensitive GROUP BY g SETTINGS aggregation_in_order_shuffle = 1))
    =
    (SELECT groupBitXor(cityHash64(g, arraySort(u), ue, qe))
     FROM (SELECT intDiv(k, 10) AS g, groupUniqArray(v) AS u, uniqExact(v) AS ue, quantileExact(0.5)(v) AS qe
           FROM t_aio_shuffle_merge_sensitive GROUP BY g SETTINGS aggregation_in_order_shuffle = 0));

-- `argMax` picks one of the tied rows, and `quantile` returns a value within the range of the group.
SELECT count(), countIf(has(at_max, am)), countIf(q BETWEEN mn AND mx)
FROM
(
    SELECT intDiv(k, 10) AS g, argMax(v, c) AS am, groupArrayIf(v, c = 2) AS at_max,
           quantile(0.5)(v) AS q, min(v) AS mn, max(v) AS mx
    FROM t_aio_shuffle_merge_sensitive GROUP BY g SETTINGS aggregation_in_order_shuffle = 1
);

DROP TABLE t_aio_shuffle_merge_sensitive;
