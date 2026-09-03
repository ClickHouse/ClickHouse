DROP TABLE IF EXISTS t_replicated_right;
DROP TABLE IF EXISTS mv_left;

-- The arms below run at the default parallel_replicas_for_non_replicated_merge_tree = 0,
-- where a plain MergeTree is not eligible, so the right side has to be replicated to keep
-- the JOIN offloaded at all.
CREATE TABLE t_replicated_right (key UInt64)
ENGINE = ReplicatedMergeTree('/parallel_replicas/{database}/t_replicated_right', 'r1') ORDER BY key;
INSERT INTO t_replicated_right SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_prefer_local_join = 0;

-- A MaterializedView left side is eligible through the table it resolves to, so it keeps the local
-- join too. Two arms keep the rest honest: the setting that admits the wrapper must also be able to
-- reject it, and the match counter must see rows coming from the wrapper, since a RIGHT JOIN
-- returns the whole right side even when the left one is empty. Matched-ness is reported through
-- ifNull because join_use_nulls decides whether an unmatched left column reads as 0 or NULL.

-- POPULATE, not a second INSERT into the source: an INSERT repeating a block already written is
-- dropped when insert deduplication is on, which the compatibility setting decides. It has to
-- precede AS, where it is a keyword; after AS it parses as an alias of the selected table.
CREATE MATERIALIZED VIEW mv_left (key UInt64)
ENGINE = ReplicatedMergeTree('/parallel_replicas/{database}/mv_left', 'r1') ORDER BY key
POPULATE AS SELECT key FROM t_replicated_right;

SELECT '-- materialized view left, right eligible: the JOIN is still offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM mv_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_materialized_views = 1
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- materialized view left, right eligible: the local join is kept';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM mv_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_materialized_views = 1
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%';

SELECT '-- materialized view left with materialized views disallowed: left is materialized';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM mv_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_materialized_views = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- materialized view left, right eligible: results are correct';
SELECT r.key, ifNull(l.key = r.key, 0) AS matched FROM (SELECT key FROM mv_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1,
         parallel_replicas_allow_materialized_views = 1;

SELECT '-- materialized view left, right eligible: the wrapper contributes matched rows';
SELECT countIf(ifNull(l.key = r.key, 0) AND r.key > 0) FROM (SELECT key FROM mv_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1,
         parallel_replicas_allow_materialized_views = 1;

DROP TABLE mv_left SYNC;
DROP TABLE t_replicated_right SYNC;
