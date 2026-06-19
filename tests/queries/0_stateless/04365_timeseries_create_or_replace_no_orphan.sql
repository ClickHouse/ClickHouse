-- Tags: no-replicated-database, no-shared-catalog
-- no-replicated-database / no-shared-catalog: TimeSeries DDL and DETACH/ATTACH DATABASE
-- go through the replicated DDL log / shared metadata, which changes the path under test.

SET allow_experimental_time_series_table = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 0;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t ENGINE = TimeSeries;

-- CREATE OR REPLACE on an existing table goes through the intermediate _tmp_replace_<hash>
-- table and an EXCHANGE. StorageTimeSeries cannot be renamed, so the EXCHANGE must be
-- rejected up front. Before the fix the check only fired in renameInMemory(), which runs
-- after DatabaseAtomic has already swapped the metadata files on disk and detached the old
-- table in memory; the failed cleanup then stranded an orphaned _tmp_replace_*.sql carrying
-- the old explicit UUID. A later reload loaded both files, hit a duplicate UUID in
-- DatabaseCatalog::addUUIDMapping and aborted the server with a LOGICAL_ERROR.
CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t ENGINE = TimeSeries; -- { serverError NOT_IMPLEMENTED }

-- Reload from disk: this is the path that used to abort. No orphaned _tmp_replace_* table
-- must have been left behind, the original table must still be there, and the server alive.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name LIKE '\_tmp\_replace\_%';
SELECT name FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name NOT LIKE '.inner%' ORDER BY name;
SELECT 'alive';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
