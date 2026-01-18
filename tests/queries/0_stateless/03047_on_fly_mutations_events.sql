-- Tags: no-shared-catalog, no-parallel-replicas
-- FIXME no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge
-- no-parallel-replicas: profile events may differ with parallel replicas.

DROP TABLE IF EXISTS t_lightweight_mut_7;

SET apply_mutations_on_fly = 1;
SET max_streams_for_merge_tree_reading = 1;

CREATE TABLE t_lightweight_mut_7 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lightweight_mut_7', '1') ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_7;

INSERT INTO t_lightweight_mut_7  SELECT number, number FROM numbers(100000);

ALTER TABLE t_lightweight_mut_7 UPDATE v = 3 WHERE id % 5 = 0;
ALTER TABLE t_lightweight_mut_7 DELETE WHERE v % 3 = 0;

SYSTEM SYNC REPLICA t_lightweight_mut_7 PULL;

SELECT count() FROM t_lightweight_mut_7;

SYSTEM START MERGES t_lightweight_mut_7;

ALTER TABLE t_lightweight_mut_7 UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;

SYSTEM STOP MERGES t_lightweight_mut_7;

ALTER TABLE t_lightweight_mut_7 UPDATE v = v * v WHERE 1;

SYSTEM SYNC REPLICA t_lightweight_mut_7 PULL;

SELECT 1, sum(v) FROM t_lightweight_mut_7;
SELECT 2, sum(v) FROM t_lightweight_mut_7 SETTINGS apply_mutations_on_fly = 0;

SYSTEM START MERGES t_lightweight_mut_7;

ALTER TABLE t_lightweight_mut_7 UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;

SELECT 3, sum(v) FROM t_lightweight_mut_7;

SYSTEM FLUSH LOGS query_log;

SELECT
    query,
    ProfileEvents['ReadTasksWithAppliedMutationsOnFly'],
    ProfileEvents['MutationsAppliedOnFlyInAllReadTasks']
FROM system.query_log
WHERE current_database = currentDatabase() AND query ILIKE 'SELECT%FROM%t_lightweight_mut_7%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_lightweight_mut_7;
