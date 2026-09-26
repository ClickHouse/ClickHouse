-- Tests that a cross-database `RENAME` reserves a `max_tables` slot for every inner table it moves.
-- When one side of the rename is an `Ordinary` database, the inner table names embed the outer
-- table name, so `StorageMaterializedView::renameInMemory` and `StorageTimeSeries::renameInMemory`
-- move the inner tables too, with nested `RENAME` queries. A destination that cannot fit the whole
-- group must reject the rename before anything is moved, otherwise the first inner tables end up
-- stranded in the destination while the outer table is restored in the source.

SET allow_deprecated_database_ordinary = 1;

-- A materialized view with an inner table needs two slots.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary SETTINGS max_tables = 2;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.occupies_slot (x UInt32) ENGINE = MergeTree ORDER BY x;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.src (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE:Identifier}.mv ENGINE = MergeTree ORDER BY x
    AS SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.src;

-- Only one slot is free, so the view and its inner table do not fit.
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.mv TO {CLICKHOUSE_DATABASE_1:Identifier}.mv; -- { serverError TOO_MANY_TABLES }
-- Nothing was moved: the view is still in the source and the destination is untouched.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv';
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String};

-- With two free slots the whole group fits: the view and its inner table.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 3;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.mv TO {CLICKHOUSE_DATABASE_1:Identifier}.mv;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String};

-- A `TimeSeries` table has four inner tables by default - samples, recent samples, tags and
-- metrics - so it needs five slots.
SET allow_experimental_time_series_table = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Ordinary SETTINGS max_tables = 2;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.ts ENGINE = TimeSeries;

-- The destination fits fewer objects than the group has inner tables, so a rename that reserved
-- only the outer table would move the first inner tables and then be rejected, stranding them.
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.ts TO {CLICKHOUSE_DATABASE_2:Identifier}.ts; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String};

-- One slot short of the whole group is still a rejection.
ALTER DATABASE {CLICKHOUSE_DATABASE_2:Identifier} MODIFY SETTING max_tables = 4;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.ts TO {CLICKHOUSE_DATABASE_2:Identifier}.ts; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String};

ALTER DATABASE {CLICKHOUSE_DATABASE_2:Identifier} MODIFY SETTING max_tables = 5;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.ts TO {CLICKHOUSE_DATABASE_2:Identifier}.ts;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String};

-- The recent samples table is optional, so a `TimeSeries` without it needs one slot less.
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Ordinary SETTINGS max_tables = 3;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.ts_no_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.ts_no_recent TO {CLICKHOUSE_DATABASE_2:Identifier}.ts_no_recent; -- { serverError TOO_MANY_TABLES }
ALTER DATABASE {CLICKHOUSE_DATABASE_2:Identifier} MODIFY SETTING max_tables = 4;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.ts_no_recent TO {CLICKHOUSE_DATABASE_2:Identifier}.ts_no_recent;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
