-- A cross-database `EXCHANGE TABLES` must respect the `max_rows` limit of both databases,
-- counting the net change: the incoming rows minus the outgoing rows.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_rows = 100;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.filler SELECT number FROM numbers(60);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT number FROM numbers(10);
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.t SELECT number FROM numbers(50);

-- 60 + 50 = 110 rows would exceed the limit of the first database, in either argument order.
EXCHANGE TABLES {CLICKHOUSE_DATABASE_1:Identifier}.t AND {CLICKHOUSE_DATABASE_2:Identifier}.t; -- { serverError TOO_MANY_ROWS }
EXCHANGE TABLES {CLICKHOUSE_DATABASE_2:Identifier}.t AND {CLICKHOUSE_DATABASE_1:Identifier}.t; -- { serverError TOO_MANY_ROWS }

SELECT 'a.t', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;
SELECT 'b.t', count() FROM {CLICKHOUSE_DATABASE_2:Identifier}.t;
SELECT name = {CLICKHOUSE_DATABASE_1:String}, rows FROM system.databases WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY name;

-- With a higher limit the same exchange succeeds.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_rows = 110;
EXCHANGE TABLES {CLICKHOUSE_DATABASE_1:Identifier}.t AND {CLICKHOUSE_DATABASE_2:Identifier}.t;

SELECT 'a.t', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;
SELECT 'b.t', count() FROM {CLICKHOUSE_DATABASE_2:Identifier}.t;
SELECT name = {CLICKHOUSE_DATABASE_1:String}, rows FROM system.databases WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY name;

-- The second database has a limit too: moving 50 rows out and 10 rows in fits, moving them back does not.
ALTER DATABASE {CLICKHOUSE_DATABASE_2:Identifier} MODIFY SETTING max_rows = 40;
EXCHANGE TABLES {CLICKHOUSE_DATABASE_2:Identifier}.t AND {CLICKHOUSE_DATABASE_1:Identifier}.t; -- { serverError TOO_MANY_ROWS }
SELECT 'b.t', count() FROM {CLICKHOUSE_DATABASE_2:Identifier}.t;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
