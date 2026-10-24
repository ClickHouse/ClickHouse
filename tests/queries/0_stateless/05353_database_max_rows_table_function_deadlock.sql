-- `system.databases.rows` counts rows under the database mutex, so it must not take the `nested_mutex` of `p`:
-- the first read of `p` holds it while `view` looks up `src` in the same database.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.p (x UInt64) AS view(SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src);

SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.p;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
