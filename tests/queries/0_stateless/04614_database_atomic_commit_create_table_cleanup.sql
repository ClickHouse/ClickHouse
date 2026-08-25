-- Tags: no-parallel
-- Tag no-parallel: test uses hardcoded table UUID

DROP TABLE IF EXISTS test_database_atomic_commit_create_table;
SYSTEM ENABLE FAILPOINT database_atomic_commit_create_table_failure;
CREATE TABLE test_database_atomic_commit_create_table UUID '00004614-1000-4000-8000-000000000001' (n UInt64) ENGINE=MergeTree ORDER BY n; -- { serverError 425 }
CREATE TABLE test_database_atomic_commit_create_table UUID '00004614-1000-4000-8000-000000000001' (n UInt64) ENGINE=MergeTree ORDER BY n;
SELECT COUNT() FROM system.tables WHERE database=currentDatabase() AND name='test_database_atomic_commit_create_table';
DROP TABLE test_database_atomic_commit_create_table SYNC;
