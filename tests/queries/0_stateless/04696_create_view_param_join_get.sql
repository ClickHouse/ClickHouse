-- Tags: need-query-parameters

-- The first argument of `joinGet` is qualified with the current database at `CREATE` time, so that a
-- stored definition reads the same `Join` table regardless of the session that evaluates it. A
-- parameterized name is not a name yet: it is the view's interface, resolved when the view is called,
-- and rebuilding it as a resolved table name would drop the `ASTQueryParameter` and freeze a bogus
-- `database.` name (which the dependency scan then rejects as an empty table name).

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.j1 (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.j1 VALUES (1, 'one');

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.j2 (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.j2 VALUES (1, 'two');

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.j1 (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.j1 VALUES (1, 'other');

USE {CLICKHOUSE_DATABASE:Identifier};

-- The `Join` table of the body comes from a parameter: calling the view twice with different names
-- shows the target is only known at call time.
CREATE VIEW v_param AS SELECT joinGet({pjoin:Identifier}, 'v', toUInt64(1)) AS g;
SELECT 'param', (SELECT g FROM v_param(pjoin = 'j1')), (SELECT g FROM v_param(pjoin = 'j2'));

-- The body keeps the placeholder instead of a name frozen at `CREATE` time.
SELECT 'body keeps placeholder', position(create_table_query, '{pjoin') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'v_param';

-- A parameterized body must not record a referential dependency on the same-named table of the
-- current database: the parameter can name any table, so that edge would guard the wrong one.
DROP TABLE j2 SETTINGS check_referential_table_dependencies = 1;
SELECT 'parameterized body records no dependency';

-- The control: a resolvable name is still qualified with the create-time database, so the view keeps
-- reading the same `Join` table from a session whose current database is another one, and it is
-- still recorded as a referential dependency.
CREATE VIEW v_literal AS SELECT joinGet(j1, 'v', toUInt64(1)) AS g;
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'qualified at create time', (SELECT g FROM {CLICKHOUSE_DATABASE:Identifier}.v_literal);
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE j1 SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
