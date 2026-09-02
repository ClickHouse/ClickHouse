-- Tags: zookeeper, no-parallel

DROP TABLE IF EXISTS test_temporary_table_02989;
CREATE TEMPORARY TABLE test_temporary_table_02989
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

SELECT name, metadata_version FROM system.tables WHERE name = 'test_temporary_table_02989' AND is_temporary;

DROP TABLE test_temporary_table_02989;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

SELECT '--';

SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 'test_table';

DROP TABLE test_table;

DROP TABLE IF EXISTS test_table_replicated;
CREATE TABLE test_table_replicated
(
    id UInt64,
    value String
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_table_replicated', '1_replica') ORDER BY id;

SELECT '--';

SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 'test_table_replicated';

ALTER TABLE test_table_replicated ADD COLUMN insert_time DateTime;

SELECT '--';

SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 'test_table_replicated';

ALTER TABLE test_table_replicated ADD COLUMN insert_time_updated DateTime;

SELECT '--';

SELECT name, metadata_version FROM system.tables WHERE database = currentDatabase() AND name = 'test_table_replicated';

DROP TABLE test_table_replicated;
