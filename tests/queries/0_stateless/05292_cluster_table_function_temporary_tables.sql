-- Tags: no-fasttest, zookeeper
-- no-fasttest: needs the test clusters and, for the INSERT, a Replicated table

-- A cluster table function sends the replicas the query text, and each replica resolves the tables in it on
-- its own, so the temporary tables the query reads have to be sent along with it.

SET send_logs_level = 'fatal';

INSERT INTO FUNCTION file(currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64')
SELECT number FROM numbers(4) SETTINGS engine_file_truncate_on_insert = 1;

CREATE TEMPORARY TABLE tmp (id UInt64) AS SELECT arrayJoin([2, 3]);
CREATE TEMPORARY TABLE tmp_empty (id UInt64);

SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n IN (SELECT id FROM tmp);
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n IN tmp;
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n GLOBAL IN tmp;
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n NOT IN (SELECT id FROM tmp);
SELECT count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n NOT IN (SELECT id FROM tmp_empty);

-- A parallel distributed INSERT SELECT sends the whole INSERT to the replicas.
DROP TABLE IF EXISTS dst SYNC;
CREATE TABLE dst (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05292_dst', 'r1') ORDER BY n;
INSERT INTO dst SELECT n FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n IN (SELECT id FROM tmp) SETTINGS parallel_distributed_insert_select = 2;
SELECT sum(n), count() FROM dst;
DROP TABLE dst SYNC;

DROP TABLE IF EXISTS dst_dist SYNC;
DROP TABLE IF EXISTS dst_local SYNC;
CREATE TABLE dst_local (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE dst_dist AS dst_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'dst_local');
INSERT INTO dst_dist SELECT n FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n IN (SELECT id FROM tmp) SETTINGS parallel_distributed_insert_select = 2;
SELECT sum(n), count() FROM dst_local;
DROP TABLE dst_dist SYNC;
DROP TABLE dst_local SYNC;

-- A temporary table the query does not read is not sent.
CREATE TEMPORARY TABLE unused (v UInt64) AS SELECT rand64() FROM numbers(200000);
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05292.tsv', 'TSV', 'n UInt64') WHERE n IN (SELECT id FROM tmp) SETTINGS log_comment = '05292_unused';
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0 AND max(ProfileEvents['NetworkSendBytes']) < 500000 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05292_unused' AND type = 'QueryFinish' AND is_initial_query;
