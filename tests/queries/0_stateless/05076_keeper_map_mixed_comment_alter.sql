-- Tags: zookeeper

SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE=Replicated('/clickhouse/databases/{database}', 'shard1', 'replica1');
USE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE 05076_keepermap (k UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/05076_keepermap') PRIMARY KEY (k);

-- `local` reads the comments the server applied, `keeper` the ones the database published for the
-- other replicas. They must agree after every ALTER; a distinct literal per ALTER keeps a stale
-- value from matching.

SELECT '-- Single comment command type:';
ALTER TABLE 05076_keepermap COMMENT COLUMN k 'col-1';
SELECT 'local:', extractAll(create_table_query, 'COMMENT \'([^\']*)\'') FROM system.tables WHERE database = currentDatabase() AND table = '05076_keepermap';
SELECT 'keeper:', extractAll(value, 'COMMENT \'([^\']*)\'') FROM system.zookeeper WHERE path = '/clickhouse/databases/' || currentDatabase() || '/metadata' AND name = '05076_keepermap';

SELECT '-- Two different comment command types in one ALTER:';
ALTER TABLE 05076_keepermap MODIFY COMMENT 'tbl-2', COMMENT COLUMN k 'col-2';
SELECT 'local:', extractAll(create_table_query, 'COMMENT \'([^\']*)\'') FROM system.tables WHERE database = currentDatabase() AND table = '05076_keepermap';
SELECT 'keeper:', extractAll(value, 'COMMENT \'([^\']*)\'') FROM system.zookeeper WHERE path = '/clickhouse/databases/' || currentDatabase() || '/metadata' AND name = '05076_keepermap';

SELECT '-- Comment-only MODIFY COLUMN:';
ALTER TABLE 05076_keepermap MODIFY COLUMN k COMMENT 'col-3';
SELECT 'local:', extractAll(create_table_query, 'COMMENT \'([^\']*)\'') FROM system.tables WHERE database = currentDatabase() AND table = '05076_keepermap';
SELECT 'keeper:', extractAll(value, 'COMMENT \'([^\']*)\'') FROM system.zookeeper WHERE path = '/clickhouse/databases/' || currentDatabase() || '/metadata' AND name = '05076_keepermap';

SELECT '-- MODIFY/RESET SETTING are still refused, and change nothing:';
ALTER TABLE 05076_keepermap MODIFY SETTING some_setting = 1; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE 05076_keepermap RESET SETTING some_setting; -- { serverError BAD_ARGUMENTS }
ALTER TABLE 05076_keepermap COMMENT COLUMN k 'col-4', MODIFY SETTING some_setting = 1; -- { serverError NOT_IMPLEMENTED }
SELECT 'local:', extractAll(create_table_query, 'COMMENT \'([^\']*)\'') FROM system.tables WHERE database = currentDatabase() AND table = '05076_keepermap';
SELECT 'keeper:', extractAll(value, 'COMMENT \'([^\']*)\'') FROM system.zookeeper WHERE path = '/clickhouse/databases/' || currentDatabase() || '/metadata' AND name = '05076_keepermap';

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier} SYNC;
