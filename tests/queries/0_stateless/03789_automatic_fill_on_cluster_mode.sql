-- Tags: no-parallel, no-replicated-database
-- no-replicated-database: auto-fill skips Replicated databases by design, so the
-- ON CLUSTER assertions only hold under non-replicated database engines.

SET distributed_ddl_output_mode='none';
SET allow_experimental_automatic_fill_on_cluster_mode = false;
SET cluster_for_automatic_fill_mode = 'test_shard_localhost';

SELECT 'Test 1: CREATE TABLE without automatic mode';
CREATE TABLE test_no_auto (id UInt32, value String) ENGINE = MergeTree() ORDER BY id;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 1 verification: Should NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_no_auto%' 
  AND query LIKE '%ON CLUSTER%';

DROP TABLE test_no_auto;

SET allow_experimental_automatic_fill_on_cluster_mode = true;
SET cluster_for_automatic_fill_mode = 'test_shard_localhost';

SELECT 'Test 2: CREATE TABLE with automatic mode';
CREATE TABLE test_auto_fill_cluster (id UInt32, value String) ENGINE = MergeTree() ORDER BY id;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 2 verification: Should contain ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_auto_fill_cluster%' 
  AND query LIKE '%ON CLUSTER%';

SELECT 'Test 3: ALTER TABLE with automatic mode executed successfully';
ALTER TABLE test_auto_fill_cluster ADD COLUMN new_column UInt64 DEFAULT 0;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 3 verification: ALTER TABLE contains ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%ALTER TABLE test_auto_fill_cluster%'
  AND query LIKE '%ON CLUSTER%';

SELECT 'Test 4: DROP TABLE with automatic mode executed successfully';
DROP TABLE test_auto_fill_cluster;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 4 verification: DROP TABLE contains ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%DROP TABLE test_auto_fill_cluster%'
  AND query LIKE '%ON CLUSTER%';

SELECT 'Test 5: CREATE/DROP TEMPORARY TABLE is not rewritten ON CLUSTER';
-- Temporary objects cannot be created/dropped ON CLUSTER, so auto-fill must skip them.
-- Both statements must succeed locally instead of throwing.
CREATE TEMPORARY TABLE test_auto_fill_temp (id UInt32) ENGINE = Memory;
DROP TEMPORARY TABLE test_auto_fill_temp;

SELECT 'Test 6: CREATE/DROP FUNCTION is not rewritten ON CLUSTER';
-- User-defined functions are replicated automatically and reject a non-empty cluster with
-- `ON CLUSTER is not allowed because used-defined functions are replicated automatically`,
-- so auto-fill must skip them. Both statements must succeed locally instead of throwing.
DROP FUNCTION IF EXISTS test_auto_fill_function;
CREATE FUNCTION test_auto_fill_function AS (x) -> x + 1;
SELECT test_auto_fill_function(1);
DROP FUNCTION test_auto_fill_function;
