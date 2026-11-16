-- Tags: shard, zookeeper, no-fasttest

-- An INSERT SELECT from a *Cluster source into a replicated table can be handed to the workers as a whole.
-- With a requirement of the destination it is not: the requirement was checked on this table, and a worker inserts into its own.

DROP TABLE IF EXISTS insert_expected_offload_dst;

CREATE TABLE insert_expected_offload_dst (x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/insert_expected_offload_dst', 'r1') ORDER BY x;

-- No requirement: the workers insert.
INSERT INTO insert_expected_offload_dst SELECT x FROM urlCluster('test_shard_localhost', 'http://localhost:8123/?query=SELECT+1', 'CSV', 'x UInt8')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05099_offload_none';

-- A requirement of the query.
INSERT INTO insert_expected_offload_dst SELECT x FROM urlCluster('test_shard_localhost', 'http://localhost:8123/?query=SELECT+2', 'CSV', 'x UInt8')
SETTINGS insert_expected_column_types = {'x': 'UInt8'}, parallel_distributed_insert_select = 2, log_comment = '05099_offload_query';

-- A requirement of the session, which the query text does not carry.
SET insert_expected_column_types = {'x': 'UInt8'};
INSERT INTO insert_expected_offload_dst SELECT x FROM urlCluster('test_shard_localhost', 'http://localhost:8123/?query=SELECT+3', 'CSV', 'x UInt8')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05099_offload_session';

SET insert_expected_column_types = {'x': 'UInt64'};
INSERT INTO insert_expected_offload_dst SELECT x FROM urlCluster('test_shard_localhost', 'http://localhost:8123/?query=SELECT+4', 'CSV', 'x UInt8')
SETTINGS parallel_distributed_insert_select = 2; -- { serverError INCOMPATIBLE_SCHEMA }
SET insert_expected_column_types = DEFAULT;

SYSTEM FLUSH LOGS query_log;

SELECT l.log_comment, countIf(s.is_initial_query = 0 AND s.query_kind = 'Insert') > 0 AS inserted_by_workers
FROM system.query_log AS s
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment IN ('05099_offload_none', '05099_offload_query', '05099_offload_session')
) AS l ON s.initial_query_id = l.query_id
WHERE s.type = 'QueryFinish' AND s.event_date >= yesterday()
GROUP BY l.log_comment
ORDER BY l.log_comment;

SELECT * FROM insert_expected_offload_dst ORDER BY x;

DROP TABLE insert_expected_offload_dst;
