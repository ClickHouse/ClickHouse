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
