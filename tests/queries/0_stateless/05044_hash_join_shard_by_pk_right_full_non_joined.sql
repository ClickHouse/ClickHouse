-- `query_plan_join_shard_by_pk_ranges` clones `HashJoin` with `cloneNoParallel`. After unifying
-- `parallel_hash` into `HashJoin`, that clone used to keep `use_parallel_layout`, so
-- `JoiningTransform` skipped unmatched right rows (it expected `NonJoinedBlocksTransform`, which
-- the sharded pipeline never adds). RIGHT/FULL must still emit those rows.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET use_statistics = 0;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1;
SET join_algorithm = 'hash';
SET query_plan_join_shard_by_pk_ranges = 1;
SET parallel_non_joined_rows_processing = 1;
-- Force the parallel layout on the original join so the missing override would drop unmatched rows.
SET parallel_hash_join_threshold = 0;
SET max_threads = 4;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS t_05044_l;
DROP TABLE IF EXISTS t_05044_r;

CREATE TABLE t_05044_l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE t_05044_r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;

-- Left keys 0..511, right keys 256..1023: unmatched right rows 512..1023, unmatched left 0..255.
INSERT INTO t_05044_l SELECT number, number FROM numbers(512);
INSERT INTO t_05044_r SELECT number + 256, number FROM numbers(768);

SELECT countIf(explain LIKE '%Sharding:%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM t_05044_l AS l RIGHT JOIN t_05044_r AS r ON l.k = r.k);

SELECT
    (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05044_l AS l RIGHT JOIN t_05044_r AS r ON l.k = r.k)
  = (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05044_l AS l RIGHT JOIN t_05044_r AS r ON l.k = r.k
     SETTINGS query_plan_join_shard_by_pk_ranges = 0);

SELECT count(), countIf(isNull(l.k)) FROM t_05044_l AS l RIGHT JOIN t_05044_r AS r ON l.k = r.k;

SELECT
    (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05044_l AS l FULL JOIN t_05044_r AS r ON l.k = r.k)
  = (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05044_l AS l FULL JOIN t_05044_r AS r ON l.k = r.k
     SETTINGS query_plan_join_shard_by_pk_ranges = 0);

SELECT count(), countIf(isNull(l.k)), countIf(isNull(r.k)) FROM t_05044_l AS l FULL JOIN t_05044_r AS r ON l.k = r.k;

DROP TABLE t_05044_l;
DROP TABLE t_05044_r;
