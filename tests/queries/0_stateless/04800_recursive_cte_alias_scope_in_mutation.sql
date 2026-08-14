-- Tags: need-query-parameters

SET enable_lightweight_update = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `src` exists in the session database and in the updated table's database, with different contents.
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (99);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 0);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.u (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.u VALUES (1, 0);

-- The alias of a recursive CTE must not suppress qualification of a later same-named table.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src) * 1000
             + (SELECT max(id) FROM src)
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'W1', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src) * 1000
          + (SELECT max(id) FROM src)
    WHERE id = 1;
SELECT 'W2', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (SELECT sum(id) FROM (
        SELECT 1 AS id
        UNION ALL WITH RECURSIVE src AS (SELECT 5 AS id) SELECT max(id) AS id FROM src
        UNION ALL SELECT max(id) AS id FROM src))
    WHERE id = 1;
SELECT 'W3', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

-- A non-colliding alias, which the visitor has always handled.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH RECURSIVE r AS (SELECT 7 AS id) SELECT max(id) FROM r) * 1000
             + (SELECT max(id) FROM src)
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C1', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (SELECT max(id) FROM src) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C2', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- The alias is not visible before the subquery that declares it.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (SELECT max(id) FROM src) * 1000
             + (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src)
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C3', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- A recursive CTE references itself, so its alias is visible inside its own definition.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH RECURSIVE src AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM src WHERE n < 4)
                SELECT sum(n) FROM src)
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C4', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src) * 1000
             + (WITH RECURSIVE src AS (SELECT 10 AS id) SELECT max(id) FROM src)
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C5', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- A later branch of a UNION carries a copy of the first branch's WITH list, without the RECURSIVE flag.
-- `cte` is a physical table in the database the view's identifiers are qualified with.
CREATE TABLE cte (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO cte VALUES (555);
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v6 AS
    WITH RECURSIVE cte AS (SELECT 1 AS n) SELECT n FROM cte UNION ALL SELECT n FROM cte;
SELECT 'C6', sum(n) FROM {CLICKHOUSE_DATABASE_1:Identifier}.v6;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (SELECT sum(x) FROM (
        WITH RECURSIVE src AS (SELECT 3 AS x) SELECT x FROM src
        UNION ALL SELECT max(x) AS x FROM src))
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C6b', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (SELECT sum(id) FROM (
        SELECT 1 AS id
        UNION ALL WITH RECURSIVE r AS (SELECT 5 AS id) SELECT max(id) AS id FROM r
        UNION ALL SELECT max(id) AS id FROM src))
    WHERE id = 1;
SELECT 'C7', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v8 AS WITH cte AS (SELECT 1 AS n) SELECT sum(n) AS s FROM cte;
SELECT 'C8', s FROM {CLICKHOUSE_DATABASE_1:Identifier}.v8;

-- A non-recursive CTE cannot reference itself, so inside its own definition the name is still a table.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT max(id) AS m FROM src) SELECT m FROM src) WHERE id = 1;
SELECT 'C9', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM src WHERE n < 4)
             SELECT sum(n) FROM src) WHERE id = 1;
SELECT 'C10', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT max(id) AS m FROM src) SELECT m FROM src)
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'R1', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- The same leak through the other places a table identifier is reached from.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src) * 1000
          + (SELECT max(a.id) FROM src AS a INNER JOIN src AS b ON a.id = b.id) WHERE id = 1;
SELECT 'W4', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src) * 1000
          + (SELECT count() FROM numbers(50) WHERE number IN src) WHERE id = 1;
SELECT 'W5', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src) * 1000
          + (SELECT max(id) FROM (SELECT id FROM src INTERSECT SELECT 2 AS id)) WHERE id = 1;
SELECT 'W6', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (SELECT (SELECT (SELECT (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT max(id) FROM src)))) * 1000
          + (SELECT max(id) FROM src) WHERE id = 1;
SELECT 'W7', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT sum(id) FROM src
             WHERE id IN (SELECT id FROM src) GROUP BY id ORDER BY sum(id) LIMIT 1) * 1000
          + (SELECT max(id) FROM src) WHERE id = 1;
SELECT 'W8', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

-- The alias stays visible to everything nested inside the SELECT that declares it.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src)) WHERE id = 1;
SELECT 'C11', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH RECURSIVE src AS (SELECT 7 AS id)
             SELECT max(id) + (WITH RECURSIVE src AS (SELECT 30 AS id) SELECT max(id) FROM src) FROM src)
    WHERE id = 1;
SELECT 'C12', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mvsrc (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv
    ENGINE = MergeTree ORDER BY tuple() AS
    WITH RECURSIVE src AS (SELECT 7 AS id)
    SELECT (SELECT max(id) FROM src) AS a, id FROM {CLICKHOUSE_DATABASE_1:Identifier}.mvsrc;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.mvsrc VALUES (1);
SELECT 'C13', a FROM {CLICKHOUSE_DATABASE_1:Identifier}.mv;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH 5 AS k SELECT k + (SELECT max(id) FROM src)) WHERE id = 1;
SELECT 'C14', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

-- A sibling UNION branch of a recursive CTE must be qualified in the stored view definition,
-- otherwise the view cannot be resolved from another database. Closes #104972.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.cte_name (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.cte_name VALUES (100);
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE VIEW v9 AS
    SELECT x FROM (
        WITH RECURSIVE cte_name AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM cte_name WHERE x < 3)
        SELECT x FROM cte_name)
    UNION ALL SELECT x FROM cte_name;
USE {CLICKHOUSE_DATABASE:Identifier};
SELECT 'W9', sum(x) FROM {CLICKHOUSE_DATABASE_1:Identifier}.v9;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
