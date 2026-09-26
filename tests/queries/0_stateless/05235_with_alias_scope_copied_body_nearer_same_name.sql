-- `ApplyWithSubqueryVisitor` substitutes a reference to a `WITH` element with a copy of the
-- element's body, tagged with the element's name, and `AddDefaultDatabaseVisitor` walks that copy
-- as the body of the element it came from. A name inside such a copy still denotes what the
-- analyzer makes of it in the query as written: a `WITH` element's body is resolved where the
-- element is *referenced*, so a nearer element of the same name at the reference wins, and a copy
-- of a body that reads that name must not be qualified to a table. Here `a`'s body reads `src`,
-- and at the only reference to `a` a nearer `src` is in scope, so the answer is that element's and
-- no table is read - by whatever database the query is run from.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `src` exists in the session database and in the view's database, with different contents.
-- Neither may be read.
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (22);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (7);

USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'live', id FROM (
    WITH
        src AS (SELECT max(id) AS id FROM src),
        a AS (SELECT id FROM src)
    SELECT id FROM
    (
        WITH src AS (SELECT 99 AS id)
        SELECT id FROM a
    ));

CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v AS
    WITH
        src AS (SELECT max(id) AS id FROM src),
        a AS (SELECT id FROM src)
    SELECT id FROM
    (
        WITH src AS (SELECT 99 AS id)
        SELECT id FROM a
    );

USE {CLICKHOUSE_DATABASE:Identifier};

-- The view answers what the analyzer answered, and the same from either database.
SELECT 'from the session database', id FROM {CLICKHOUSE_DATABASE_1:Identifier}.v;
SELECT 'from the view database', id FROM (SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.v);

-- Only the table inside the body of `src` itself is qualified: there the name is a table. Every
-- other occurrence is an element of the statement.
SELECT 'stored', replaceAll(create_table_query, {CLICKHOUSE_DATABASE_1:String}, 'db1') LIKE '%WITH src AS (SELECT max(id) AS id FROM db1.src), a AS (SELECT id FROM src) SELECT id FROM (WITH src AS (SELECT 99 AS id) SELECT id FROM a)'
    FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND database != currentDatabase() AND name = 'v';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE src;
