-- Tags: no-old-analyzer
-- no-old-analyzer: `WITH RECURSIVE` needs the analyzer, and a background mutation selects its
-- analyzer from the background context, so a session `enable_analyzer` cannot reach the
-- `ALTER ... UPDATE` arms.

-- `RECURSIVE` is a property of the whole `WITH` list, but only the elements whose body is a
-- `UNION` are recursive; a single-`SELECT` element of a `WITH RECURSIVE` list is an ordinary CTE,
-- and inside its own body its name still denotes a table. `AddDefaultDatabaseVisitor` must
-- classify the elements one by one, or the self-reference of the ordinary element is left
-- unqualified and later reads the reader's table rather than the declaring database's one.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `src` exists in the session database and in the view's database, with different contents.
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (99);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (7);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 0);

-- The failing shape: a mixed `WITH RECURSIVE` list whose ordinary element shadows a real table
-- and references it in its own body. The view is created from the database that holds `src` = 7,
-- so the self-reference must be qualified to that database.
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v AS
    WITH RECURSIVE src AS (SELECT max(id) AS id FROM src),
                   r AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM r WHERE id < 3)
    SELECT id FROM src;
-- Control: the same list without `RECURSIVE` is qualified the same way.
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v_plain AS
    WITH src AS (SELECT max(id) AS id FROM src),
         r AS (SELECT 1 AS id UNION ALL SELECT 2 AS id)
    SELECT id FROM src;
USE {CLICKHOUSE_DATABASE:Identifier};

-- The view lives in the other database, not in currentDatabase().
SELECT 'stored', replaceAll(create_table_query, {CLICKHOUSE_DATABASE_1:String}, 'db1') LIKE '%FROM db1.src)%' FROM system.tables
    WHERE database = {CLICKHOUSE_DATABASE_1:String} AND database != currentDatabase() AND name = 'v';
SELECT 'view from other database', * FROM {CLICKHOUSE_DATABASE_1:Identifier}.v;
SELECT 'plain list', * FROM {CLICKHOUSE_DATABASE_1:Identifier}.v_plain;

-- The recursive element of the same list keeps its self-reference bare and still recurses.
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v_r AS
    WITH RECURSIVE src AS (SELECT max(id) AS id FROM src),
                   r AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM r WHERE id < 3)
    SELECT sum(id) FROM r;
SELECT 'recursive element', * FROM {CLICKHOUSE_DATABASE_1:Identifier}.v_r;

-- The same list inside a mutation of a table in the other database.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t UPDATE v = (
    WITH RECURSIVE src AS (SELECT max(id) AS id FROM src),
                   r AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM r WHERE id < 3)
    SELECT id FROM src) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'mutation', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

-- Control: the ordinary element does not shadow a table when an enclosing `WITH` binds the same
-- name. Then its self-reference is the enclosing CTE, not a table, and must stay unqualified.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t UPDATE v = (
    WITH src AS (SELECT 5 AS id)
    SELECT id FROM (
        WITH RECURSIVE src AS (SELECT max(id) AS id FROM src),
                       r AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM r WHERE id < 3)
        SELECT id FROM src)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'enclosing cte wins', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

-- An `INTERSECT` body is a recursive element too, which the analyzer confirms by rejecting its
-- union mode as a recursive CTE rather than resolving `r` as a table.
SELECT count() FROM (
    WITH RECURSIVE src AS (SELECT max(id) AS id FROM {CLICKHOUSE_DATABASE_1:Identifier}.src),
                   r AS (SELECT 1 AS id INTERSECT SELECT id FROM r)
    SELECT id FROM r); -- { serverError UNSUPPORTED_METHOD }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE src;
