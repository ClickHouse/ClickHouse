-- Tags: no-replicated-database, no-shared-catalog
-- no-replicated-database / no-shared-catalog: TimeSeries DDL and DETACH/ATTACH DATABASE
-- go through the replicated DDL log / shared metadata, which changes the path under test.

SET allow_experimental_time_series_table = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 0;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t ENGINE = TimeSeries;

-- CREATE OR REPLACE goes through an intermediate _tmp_replace_<hash> table and an EXCHANGE.
-- StorageTimeSeries cannot be renamed, so the EXCHANGE must be rejected before DatabaseAtomic
-- swaps any metadata; otherwise a half-applied EXCHANGE strands an orphaned _tmp_replace_*.sql.
CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t ENGINE = TimeSeries; -- { serverError NOT_IMPLEMENTED }

-- A genuine table rename is also rejected.
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t TO {CLICKHOUSE_DATABASE_1:Identifier}.t2; -- { serverError NOT_IMPLEMENTED }

-- Reload from disk: the path that used to abort on a duplicate UUID. Assert no orphaned
-- _tmp_replace_* table was left, the original table survives, and the server stays alive.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier} LIKE '\_tmp\_replace\_%';
SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier} NOT LIKE '.inner%';
SELECT 'alive';

-- RENAME DATABASE only changes the database name (table name and UUID are kept), so it must be
-- allowed even though the TimeSeries table cannot be renamed. DatabaseAtomic::renameDatabase()
-- does not call checkTableCanBeRenamed(), so the guard also lives in renameInMemory().
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};

SHOW TABLES FROM {CLICKHOUSE_DATABASE_2:Identifier} NOT LIKE '.inner%';
SELECT 'renamed';

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
