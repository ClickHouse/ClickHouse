-- Tags: no-replicated-database
-- Tag no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

-- `joinGet` works with a lazily loaded `Join` table, and a table that is not `Join` is still refused.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_join (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_log (k UInt64, v String) ENGINE = Log;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_join VALUES (1, 'one'), (2, 'two');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_log VALUES (1, 'one');

-- Re-attach the database so the tables become unloaded lazy proxies.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;

SELECT joinGet(t_join, 'v', toUInt64(1));
SELECT number, joinGetOrNull(t_join, 'v', number) FROM numbers(4) ORDER BY number;
SELECT joinGet(t_log, 'v', toUInt64(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_join;
DROP TABLE t_log;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
