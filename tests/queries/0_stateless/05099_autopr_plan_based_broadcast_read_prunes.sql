-- A follower may skip its own index analysis and return every mark only when the coordinator assigns
-- its ranges. With plan-based parallel replicas the whole fragment is rebuilt on the follower from one
-- shared context, so the broadcast side of a JOIN - which no coordinator drives - used to satisfy that
-- condition as well, skip primary key pruning, and read the broadcast table in full on every remote
-- replica.
--
-- Rows read are the observable rather than marks: the coordinated read legitimately announces all of
-- its marks, and only the rows actually read separate a pruned broadcast read from an unpruned one.
-- The filter selects one granule of `t_pb_bcast`, so a follower that prunes reads the small probe
-- table plus about a granule, while one that does not reads two million rows more.

DROP TABLE IF EXISTS t_pb_probe;
DROP TABLE IF EXISTS t_pb_bcast;

-- The probe side is kept small so that the rows a follower reads are dominated by how much of the
-- broadcast table it read, which is the thing under test.
CREATE TABLE t_pb_probe (key UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_pb_bcast (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_pb_probe SELECT number FROM numbers(50000);
INSERT INTO t_pb_bcast SELECT number FROM numbers(2000000);

OPTIMIZE TABLE t_pb_probe FINAL;
OPTIMIZE TABLE t_pb_bcast FINAL;

SET enable_analyzer = 1;
-- The broadcast read has to be pruned by its own key condition, not by a filter built from the other
-- side at runtime.
SET enable_join_runtime_filters = 0;
-- Keep the sides fixed, so `t_pb_bcast` is the broadcast one.
SET query_plan_join_swap_table = 'false';

SET enable_parallel_replicas = 1;
-- Parallel replicas are forced rather than decided by the cost model: this is about what the replicas
-- do once chosen, not about whether they are worth choosing.
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_plan_based = 1;
SET serialize_query_plan = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 2;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
-- Without this the local replica claims every range before the remote ones have announced themselves,
-- and the query runs with no follower at all - leaving nothing to observe.
SET parallel_replicas_prefer_local_replica = 0;

SELECT count() FROM t_pb_probe INNER JOIN t_pb_bcast ON t_pb_probe.key = t_pb_bcast.id
WHERE t_pb_bcast.id < 5000
FORMAT Null SETTINGS log_comment = '05099_plan_based';

SET enable_parallel_replicas = 0;
SET parallel_replicas_plan_based = 0;
SET serialize_query_plan = 0;

SYSTEM FLUSH LOGS query_log;

-- Follower queries are matched through `initial_query_id`: they are logged with `current_database` set
-- to `default` rather than to this test's database, so they cannot be selected by database.
--
-- `a_follower_executed` keeps the check honest: with no remote replica taking part there is nothing to
-- prune wrongly, and the row bound would hold for that reason alone.
WITH initial_ids AS
    (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
          AND event_date >= yesterday() AND log_comment = '05099_plan_based'
    )
SELECT
    count() > 0 AS a_follower_executed,
    max(ProfileEvents['SelectedRows']) < 500000 AS every_follower_pruned_the_broadcast_read
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND event_date >= yesterday()
  AND initial_query_id IN (SELECT query_id FROM initial_ids)
FORMAT TSVWithNames;

DROP TABLE t_pb_probe;
DROP TABLE t_pb_bcast;
