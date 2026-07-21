-- https://github.com/ClickHouse/ClickHouse/issues/82084
-- A table has the same name as the database it is in. A fully qualified column name
-- `db.table.column` must be resolved even though its first part also matches the table name.

-- These queries already work with the old infrastructure (`enable_analyzer = 0`); the fix is analyzer-only,
-- so force the analyzer to make sure the test actually exercises the resolver change.
SET enable_analyzer = 1;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier} (id Int32, t Tuple(x Int32)) ENGINE = MergeTree ORDER BY ();
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier} VALUES (42, (7));

SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.id FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier};
SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.id FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier} AS alias_name;
SELECT {CLICKHOUSE_DATABASE:Identifier}.id FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier};
SELECT id FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier};
SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.* FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier};
SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.t.x FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier};
SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.id FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier} AS {CLICKHOUSE_DATABASE:Identifier};

-- A fully qualified column name of another table must be resolved even when a table
-- with the same name as the database participates in the join.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.other (id Int32, value Int32) ENGINE = MergeTree ORDER BY ();
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.other VALUES (42, 1);

SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.id, {CLICKHOUSE_DATABASE:Identifier}.other.value
FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}
JOIN {CLICKHOUSE_DATABASE:Identifier}.other USING (id);

-- `analyzer_compatibility_prefer_alias_over_subcolumn` restricts JOIN resolution to the side whose
-- table name or alias matches the first identifier part. That pruning must stay database-aware:
-- the same token can be the database name of the other side, and the `db.table.column`
-- interpretation must still be reachable there.
SELECT {CLICKHOUSE_DATABASE:Identifier}.other.value
FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}
JOIN {CLICKHOUSE_DATABASE:Identifier}.other USING (id)
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

-- The first part of a qualified column name can match the name of one table expression while being
-- the database name of a different table expression in the same scope (a table named like another
-- database). A failed lookup behind the table name must fall through, so that the database-qualified
-- interpretation is attempted next.
CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tbl (id Int32, value Int32) ENGINE = MergeTree ORDER BY ();
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.tbl VALUES (42, 5);
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE_1:Identifier} (id Int32) ENGINE = MergeTree ORDER BY ();
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE_1:Identifier} VALUES (42);

SELECT {CLICKHOUSE_DATABASE_1:Identifier}.tbl.value
FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE_1:Identifier}
JOIN {CLICKHOUSE_DATABASE_1:Identifier}.tbl USING (id);

-- The same fall-through must work when the compat setting prunes JOIN resolution by qualifier.
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.tbl.value
FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE_1:Identifier}
JOIN {CLICKHOUSE_DATABASE_1:Identifier}.tbl USING (id)
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

-- The table name interpretation still takes precedence when the lookup behind it succeeds.
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.id
FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE_1:Identifier}
JOIN {CLICKHOUSE_DATABASE_1:Identifier}.tbl USING (id);

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A missing column must still produce an error.
SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.nonexistent FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}; -- { serverError UNKNOWN_IDENTIFIER }
