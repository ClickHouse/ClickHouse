-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: this test explicitly creates a Replicated database.

-- Every secondary replaying the DDL holds a metadata transaction, and so does the initiator, so only
-- `isInitialQuery()` can decide whether a column `SETTINGS` name may be refused. The other tests for
-- this check run on an `Atomic` database, where there is no transaction at all.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier}
ENGINE = Replicated('/test/{database}/column_settings_unknown_name', 'shard1', 'replica1') FORMAT Null;
USE {CLICKHOUSE_DATABASE_2:Identifier};

SET distributed_ddl_output_mode = 'throw';

CREATE TABLE t (x UInt64 SETTINGS (not_a_setting = DEFAULT)) ENGINE = MergeTree ORDER BY x FORMAT Null; -- { serverError UNKNOWN_SETTING }

CREATE TABLE ok (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x FORMAT Null;
ALTER TABLE ok ADD COLUMN a UInt64 SETTINGS (not_a_setting = DEFAULT) FORMAT Null; -- { serverError UNKNOWN_SETTING }

-- A settable name still goes through the whole replicated path.
ALTER TABLE ok MODIFY COLUMN y UInt64 SETTINGS (min_compress_block_size = 100) FORMAT Null;
SELECT 'accepted', create_table_query LIKE '%`y` UInt64 SETTINGS (min_compress_block_size = 100)%'
FROM system.tables WHERE database = currentDatabase() AND name = 'ok';

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
