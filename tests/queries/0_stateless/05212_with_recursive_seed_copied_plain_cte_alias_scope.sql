-- Tags: no-old-analyzer
-- no-old-analyzer: `WITH RECURSIVE` needs the analyzer, and a background mutation selects its
-- analyzer from the background context, so a session `enable_analyzer` cannot reach the
-- `ALTER ... UPDATE` arms.

-- The seed of a recursive element, its first `UNION` branch, is resolved like any other query: a
-- reference to the element's own name there is a table, or an enclosing CTE of that name, and
-- only the branches after the seed reference the element itself. `AddDefaultDatabaseVisitor` must
-- qualify a table in the seed, and it must also qualify one inside the copy of an enclosing plain
-- CTE body that `ApplyWithSubqueryVisitor` substitutes into the seed, tagged with the CTE name:
-- the recursive name is the nearest binding at the copy, but the copy is the body of the enclosing
-- plain CTE, whose self-reference is a table there. Otherwise the query kept in the creating
-- session, and the stored text on reload, rebind the table to the recursive CTE or to the reader's
-- database, and differ from the query the analyzer accepted.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `src` exists in the session database and in the views' database, with different contents.
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (11);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 0);

USE {CLICKHOUSE_DATABASE_1:Identifier};

-- The live queries: the seed reads the table, directly or through the enclosing plain CTE.
SELECT 'live seed', groupArray(id) FROM (
    WITH RECURSIVE src AS (SELECT max(id) AS id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT id FROM src ORDER BY id);
SELECT 'live copied plain cte', groupArray(id) FROM (
    WITH src AS (SELECT max(id) AS id FROM src)
    SELECT id FROM (
        WITH RECURSIVE src AS (SELECT id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM src)
    ORDER BY id);

CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v_seed AS
    WITH RECURSIVE src AS (SELECT max(id) AS id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT id FROM src;
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v AS
    WITH src AS (SELECT max(id) AS id FROM src)
    SELECT id FROM (
        WITH RECURSIVE src AS (SELECT id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM src);

USE {CLICKHOUSE_DATABASE:Identifier};

-- The views live in the other database, not in currentDatabase(), and are read from a session
-- whose own `src` holds 11: neither the seed nor the copied body may reach it.
SELECT 'seed in the creating session', groupArray(id) FROM (SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.v_seed ORDER BY id);
SELECT 'copied plain cte in the creating session', groupArray(id) FROM (SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.v ORDER BY id);

-- The seed is qualified in the stored text and the recursive member is not. With the enclosing
-- plain CTE only its body is qualified: the seed's reference is a CTE name there.
SELECT 'stored seed', replaceAll(create_table_query, {CLICKHOUSE_DATABASE_1:String}, 'db1') LIKE '%WITH RECURSIVE src AS (SELECT max(id) AS id FROM db1.src UNION ALL SELECT id + 1 FROM src WHERE id < 3) SELECT id FROM src'
    FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND database != currentDatabase() AND name = 'v_seed';
SELECT 'stored copied plain cte', replaceAll(create_table_query, {CLICKHOUSE_DATABASE_1:String}, 'db1') LIKE '%WITH src AS (SELECT max(id) AS id FROM db1.src) SELECT id FROM (WITH RECURSIVE src AS (SELECT id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3) SELECT id FROM src)'
    FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND database != currentDatabase() AND name = 'v';

-- A reload re-expands the stored text and must agree with the creating session.
USE {CLICKHOUSE_DATABASE_1:Identifier};
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.v_seed;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.v_seed;
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.v;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.v;
USE {CLICKHOUSE_DATABASE:Identifier};
SELECT 'seed after reload', groupArray(id) FROM (SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.v_seed ORDER BY id);
SELECT 'copied plain cte after reload', groupArray(id) FROM (SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.v ORDER BY id);

-- The same shapes inside a mutation of a table in the other database.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t UPDATE v = (
    WITH RECURSIVE src AS (SELECT max(id) AS id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) FROM src) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'mutation seed', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t UPDATE v = (
    WITH src AS (SELECT max(id) AS id FROM src)
    SELECT sum(id) FROM (
        WITH RECURSIVE src AS (SELECT id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM src)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'mutation copied plain cte', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE src;
