-- Tags: distributed

-- Verify that parallel_distributed_insert_select=2 requires optimize_skip_unused_shards
-- for Distributed tables (otherwise falls back to non-parallel mode).
-- We distinguish the two cases by counting queries with the same initial_query_id in system.query_log:
-- parallel path dispatches sub-queries to each shard, non-parallel path executes a single query.

SET enable_parallel_replicas=0;

DROP TABLE IF EXISTS local_src_04073;
DROP TABLE IF EXISTS local_dst_04073;
DROP TABLE IF EXISTS dist_src_04073;
DROP TABLE IF EXISTS dist_dst_04073;

CREATE TABLE local_src_04073 (key UInt64) ENGINE = MergeTree() ORDER BY key;
CREATE TABLE local_dst_04073 (key UInt64) ENGINE = MergeTree() ORDER BY key;
CREATE TABLE dist_src_04073 AS local_src_04073 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_src_04073, key);
CREATE TABLE dist_dst_04073 AS local_dst_04073 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_dst_04073, key);

INSERT INTO local_src_04073 SELECT number FROM numbers(10);

-- Without optimize_skip_unused_shards: distributed write path is not used, single query only.
INSERT INTO dist_dst_04073 SELECT * FROM dist_src_04073 SETTINGS prefer_localhost_replica = 0, parallel_distributed_insert_select = 2, optimize_skip_unused_shards = 0;

-- With optimize_skip_unused_shards: distributed write path dispatches sub-queries to each shard.
INSERT INTO dist_dst_04073 SELECT * FROM dist_src_04073 SETTINGS prefer_localhost_replica = 0, parallel_distributed_insert_select = 2, optimize_skip_unused_shards = 1;

SYSTEM FLUSH DISTRIBUTED dist_dst_04073;
DROP TABLE dist_src_04073;
DROP TABLE dist_dst_04073;
DROP TABLE local_src_04073;
DROP TABLE local_dst_04073;

SYSTEM FLUSH LOGS query_log;
SELECT
  anyIf(query, is_initial_query) initial_query,
  countIf(query_kind = 'Select') num_selects,
  groupUniqArrayIf(query, NOT is_initial_query AND query_kind = 'Select') selects,
  countIf(query_kind = 'Insert') num_inserts,
  groupUniqArrayIf(query, NOT is_initial_query AND query_kind = 'Insert') inserts
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND initial_query_id IN (
  SELECT query_id
  FROM system.query_log
  WHERE event_date >= yesterday() AND current_database = currentDatabase() AND has(tables, currentDatabase() || '.dist_dst_04073') AND query_kind IN ('Insert', 'Select') AND type = 'QueryFinish'
)
GROUP BY initial_query_id
ORDER BY min(event_time_microseconds)
FORMAT Vertical;
