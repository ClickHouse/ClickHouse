-- https://github.com/ClickHouse/ClickHouse/issues/94894
-- An alias used as the right argument of IN in a materialized view query was qualified
-- with the default database as if it were a table name, and the CREATE query failed
-- with UNKNOWN_IDENTIFIER.

CREATE TABLE dest
(
    `object` String
)
ENGINE = MergeTree
ORDER BY object;

-- The alias of a longer expression in WHERE, referenced by another IN (the original issue).
CREATE MATERIALIZED VIEW mv1 TO dest
AS SELECT 'foo' AS object
WHERE (object IN ('foo', 'bar') AS objects) AND object IN objects;

-- The alias of the tuple inside the first IN, referenced by the second IN.
CREATE TABLE src
(
    `s` String
)
ENGINE = MergeTree
ORDER BY s;

CREATE MATERIALIZED VIEW mv2 TO dest
AS SELECT s AS object FROM src
WHERE (object IN (('foo', 'bar') AS objects)) AND object IN objects;

INSERT INTO src VALUES ('foo'), ('baz');

SELECT * FROM dest ORDER BY object;

-- An alias from the enclosing query is not visible inside a nested select query
-- (like in `MarkTableIdentifiersVisitor`): the right argument of IN there is still
-- a table name and is qualified with the database.
CREATE TABLE objects
(
    `s` String
)
ENGINE = MergeTree
ORDER BY s;

INSERT INTO objects VALUES ('foo');

CREATE VIEW v1 AS SELECT 'foo' AS objects WHERE 'foo' IN (SELECT s FROM objects WHERE s IN objects);

SELECT replaceAll(create_table_query, currentDatabase(), '[db]') FROM system.tables
WHERE database = currentDatabase() AND name = 'v1';

SELECT * FROM v1;
