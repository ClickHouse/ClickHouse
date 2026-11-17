-- A join sharded by primary-key ranges renumbers the parts of every side contiguously to split them
-- into layers and then hands the layers back to each read step. The read step keys its per-part
-- state (the ranges refined by the skip indexes at read time, the `_part_index` virtual column) by
-- the `part_index_in_query` the part had after index analysis, which is not contiguous once the
-- analysis has dropped a part. The layers must therefore carry the original index, otherwise the
-- read-time refinement looks up a part that does not exist (`unordered_map::at: key not found`).
-- The table has three parts with disjoint ranges of `c`, so the min-max index on `c` drops two of
-- them for the filtered side.

SET join_algorithm = 'full_sorting_merge';
SET query_plan_join_shard_by_pk_ranges = 1;
SET optimize_read_in_order = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_skip_indexes_on_data_read = 1;
SET use_indexes_refiner_in_read_pools = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS t_shard_pruned;
CREATE TABLE t_shard_pruned (a UInt32, b UInt32, c Int64, d String)
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 64, add_minmax_index_for_numeric_columns = 1;

SYSTEM STOP MERGES t_shard_pruned;
INSERT INTO t_shard_pruned SELECT number % 50, number % 200, toInt64(number), toString(number % 7) FROM numbers(700);
INSERT INTO t_shard_pruned SELECT number % 50, number % 200, toInt64(number), toString(number % 7) FROM numbers(700, 700);
INSERT INTO t_shard_pruned SELECT number % 50, number % 200, toInt64(number), toString(number % 7) FROM numbers(1400, 600);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_shard_pruned' AND active;

-- The filter on the right side keeps only the middle part.
SELECT 'pruned', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM t_shard_pruned AS l INNER JOIN t_shard_pruned AS r ON l.a = r.a WHERE r.c = 794))
                 = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM t_shard_pruned AS l INNER JOIN t_shard_pruned AS r ON l.a = r.a WHERE r.c = 794) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- Both sides pruned to different parts.
SELECT 'both pruned', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM t_shard_pruned AS l INNER JOIN t_shard_pruned AS r ON l.a = r.a WHERE r.c = 794 AND l.c = 1500))
                      = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM t_shard_pruned AS l INNER JOIN t_shard_pruned AS r ON l.a = r.a WHERE r.c = 794 AND l.c = 1500) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- An explicit PREWHERE on a subquery side.
SELECT 'prewhere', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM t_shard_pruned AS l INNER JOIN (SELECT * FROM t_shard_pruned PREWHERE c = 794) AS r ON l.a = r.a))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM t_shard_pruned AS l INNER JOIN (SELECT * FROM t_shard_pruned PREWHERE c = 794) AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- The `_part_index` virtual column names the part by its index after the analysis, so it must agree
-- with the unsharded read.
SELECT 'part index', (SELECT groupUniqArray(pi) FROM (SELECT r._part_index AS pi FROM t_shard_pruned AS l INNER JOIN t_shard_pruned AS r ON l.a = r.a WHERE r.c = 794))
                     = (SELECT groupUniqArray(pi) FROM (SELECT r._part_index AS pi FROM t_shard_pruned AS l INNER JOIN t_shard_pruned AS r ON l.a = r.a WHERE r.c = 794) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- The plan is really sharded, otherwise the cells above pass vacuously.
SELECT 'sharded', countIf(explain LIKE '%Sharding%') = 1
FROM (EXPLAIN actions = 1, pretty = 0 SELECT l.d FROM t_shard_pruned AS l INNER JOIN t_shard_pruned AS r ON l.a = r.a WHERE r.c = 794);

DROP TABLE t_shard_pruned;
