-- An `UNDROP` rejected by the database `max_rows` limit must leave the table in the dropped-table queue.
-- The drop has to be asynchronous, otherwise there is nothing to undrop.
SET database_atomic_wait_for_drop_and_detach_synchronously = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_rows = 100;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.undrop_source (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.undrop_source SELECT number FROM numbers(50);
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.undrop_source;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.filler SELECT number FROM numbers(90);

UNDROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.undrop_source; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM system.dropped_tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'undrop_source';

-- Freeing rows makes the same `UNDROP` succeed.
TRUNCATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.filler;
UNDROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.undrop_source;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.undrop_source;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
