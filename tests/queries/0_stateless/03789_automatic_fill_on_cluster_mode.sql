SET distributed_ddl_output_mode='none';
SET automatic_fill_on_cluster_mode = false;
SET cluster_for_automatic_fill_mode = 'test_shard_localhost';

SELECT 'Test 1: CREATE TABLE without automatic mode';
CREATE TABLE test_no_auto (id UInt32, value String) ENGINE = MergeTree() ORDER BY id;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 1 verification: Should NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_no_auto%' 
  AND query LIKE '%ON CLUSTER%'
  AND event_time >= now() - INTERVAL 10 SECOND;

DROP TABLE test_no_auto;

SET automatic_fill_on_cluster_mode = true;
SET cluster_for_automatic_fill_mode = 'test_shard_localhost';

SELECT 'Test 2: CREATE TABLE with automatic mode';
CREATE TABLE test_auto_fill_cluster (id UInt32, value String) ENGINE = MergeTree() ORDER BY id;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 2 verification: Should contain ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_auto_fill_cluster%' 
  AND query LIKE '%ON CLUSTER%'
  AND event_time >= now() - INTERVAL 10 SECOND;

SELECT 'Test 3: ALTER TABLE with automatic mode executed successfully';
ALTER TABLE test_auto_fill_cluster ADD COLUMN new_column UInt64 DEFAULT 0;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 3 verification: ALTER TABLE contains ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%ALTER TABLE test_auto_fill_cluster%'
  AND query LIKE '%ON CLUSTER%'
  AND event_time >= now() - INTERVAL 10 SECOND;

SELECT 'Test 4: RENAME TABLE with automatic mode executed successfully';
RENAME TABLE test_auto_fill_cluster TO test_renamed;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 4 verification: RENAME TABLE contains ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%RENAME TABLE test_auto_fill_cluster%' 
  AND query LIKE '%ON CLUSTER%'
  AND event_time >= now() - INTERVAL 10 SECOND;

SELECT 'Test 5: DROP TABLE with automatic mode executed successfully';
DROP TABLE test_renamed;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 5 verification: DROP TABLE contains ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%DROP TABLE test_renamed%' 
  AND query LIKE '%ON CLUSTER%'
  AND event_time >= now() - INTERVAL 10 SECOND;

SELECT 'Test 6: Explicit ON CLUSTER should be preserved';
CREATE TABLE test_explicit_cluster ON CLUSTER test_shard_localhost (id UInt32) ENGINE = MergeTree() ORDER BY id;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 6 verification: Should contain explicit ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_explicit_cluster%'
  AND query LIKE '%ON CLUSTER%'
  AND event_time >= now() - INTERVAL 10 SECOND;

DROP TABLE test_explicit_cluster;
