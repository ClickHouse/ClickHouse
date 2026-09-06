-- Tags: no-old-analyzer, long
-- long: one run of this file goes past the flaky check's 180s per-run budget under ASan
-- with S3 storage and metadata in Keeper, where every statement pays an object-storage
-- round trip. Untagged, that budget fails the check outright rather than reporting a flake.
-- no-old-analyzer: a background mutation selects its analyzer from the background context, so a
-- session `enable_analyzer` cannot reach the `ALTER ... UPDATE` arms, and `WITH RECURSIVE` needs it.

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

-- `ALTER ... MODIFY QUERY` and `CREATE MATERIALIZED VIEW` expand a non-recursive CTE reference
-- into a copy of its body before qualifying, so the name must still reach the table there.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst (v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv2
    TO {CLICKHOUSE_DATABASE_1:Identifier}.dst AS
    SELECT id AS v FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv2
    MODIFY QUERY WITH src AS (SELECT max(id) AS v FROM src) SELECT v FROM src;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);
SELECT 'C15', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.dst;

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv3
    TO {CLICKHOUSE_DATABASE_1:Identifier}.dst AS
    WITH src AS (SELECT max(id) AS v FROM src) SELECT v FROM src;
INSERT INTO src VALUES (99);
SELECT 'C16', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.dst ORDER BY v;

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

-- A plain CTE shadowing a same-named recursive one must not reveal the table to the recursive
-- element's own self-reference, so storing the query must not change its result.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.r (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.r VALUES (100);
CREATE TABLE r (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO r VALUES (900);
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v10 AS
    WITH RECURSIVE r AS (
        SELECT 1 AS n
        UNION ALL SELECT n + 1 FROM (WITH r AS (SELECT n FROM r) SELECT n FROM r) WHERE n < 3)
    SELECT sum(n) AS s FROM r;
SELECT 'C17', (WITH RECURSIVE r AS (
        SELECT 1 AS n
        UNION ALL SELECT n + 1 FROM (WITH r AS (SELECT n FROM r) SELECT n FROM r) WHERE n < 3)
    SELECT sum(n) FROM r);
SELECT 'C18', s FROM {CLICKHOUSE_DATABASE_1:Identifier}.v10;

-- A plain CTE referenced from a nested SELECT: inside the expanded copy the name is the table.
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v11 AS
    WITH r AS (SELECT max(n) AS s FROM r) SELECT s FROM (SELECT s FROM r);
SELECT 'C19', s FROM {CLICKHOUSE_DATABASE_1:Identifier}.v11;
DROP TABLE r;

-- Without `enable_global_with_statement` a plain name declared in an enclosing SELECT is not
-- visible in a nested one, so there it denotes the updated table's `src` (2), not the CTE (7).
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src)) WHERE id = 1
    SETTINGS mutations_sync = 2, enable_global_with_statement = 0;
SELECT 'C20', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src)) WHERE id = 1
    SETTINGS mutations_sync = 2, enable_global_with_statement = 1;
SELECT 'C21', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- In the declaring SELECT the name is the alias whichever way the setting is set.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id) SELECT max(id) FROM src) WHERE id = 1
    SETTINGS mutations_sync = 2, enable_global_with_statement = 0;
SELECT 'C22', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- A recursive name is visible inside its own definition, which is a nested SELECT, so the
-- self-reference keeps building 1..4 rather than reading the table.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 4)
                SELECT sum(id) FROM src) WHERE id = 1
    SETTINGS mutations_sync = 2, enable_global_with_statement = 0;
SELECT 'C23', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- A SELECT's own SETTINGS clause decides the visibility of an enclosing plain name, so the
-- nested SELECT reads the updated table's `src` (2) while the statement setting is left at 1.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C24', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- The shorthand form of the same clause stands for `= true`, so the name is the alias (7).
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement))
    WHERE id = 1 SETTINGS mutations_sync = 2, enable_global_with_statement = 0;
SELECT 'C25', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- An override reaches only the SELECT that carries it: the innermost one turns inheritance back
-- on for itself, so the name is the alias (7) even though the enclosing SELECT turned it off.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (SELECT (WITH src AS (SELECT 7 AS id)
                        SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 1))
                SETTINGS enable_global_with_statement = 0)
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C26', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- A nested clause is clamped to the constraints before it is applied, and that drops an entry
-- repeating the value already in effect. Of a setting written twice only the entry that changes it
-- survives, so here the value is 0 and the nested SELECT reads the updated table's `src` (2).
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT max(id) FROM src
                        SETTINGS enable_global_with_statement = 0, enable_global_with_statement = 1))
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C27', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- `compatibility` reaches the setting without naming it: 20.3 predates the default becoming 1,
-- so the nested SELECT reads the updated table's `src` (2).
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT max(id) FROM src SETTINGS compatibility = '20.3'))
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C28', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- A newer inner `compatibility` reverts what an older outer one derived, so the name is the
-- alias (7) again.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (SELECT (WITH src AS (SELECT 7 AS id)
                        SELECT (SELECT max(id) FROM src SETTINGS compatibility = '24.1'))
                SETTINGS compatibility = '20.3')
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C29', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- An expression alias is in scope in the whole `SELECT` that declares it, including its `WITH`
-- elements, so the right argument of the second `IN` is the tuple (1) and not the table (0).
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v12 AS
    WITH ((7 IN ((7, 8) AS src)) AND (7 IN src)) AS flag SELECT toUInt64(flag) AS s;
SELECT 'C30', s FROM {CLICKHOUSE_DATABASE_1:Identifier}.v12;

-- A name in the same position that is not an alias still denotes the table (1).
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v13 AS
    WITH (99 IN src) AS flag SELECT toUInt64(flag) AS s;
SELECT 'C31', s FROM {CLICKHOUSE_DATABASE_1:Identifier}.v13;

-- An enclosing `readonly` makes the constraints drop the inner setting, so the name stays the
-- alias (7), where C24 has the same inner clause and no enclosing one and reads the table (2).
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0)
                        SETTINGS readonly = 1))
    WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'C32', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
