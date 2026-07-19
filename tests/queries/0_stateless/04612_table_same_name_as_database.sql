-- https://github.com/ClickHouse/ClickHouse/issues/82084
-- A table has the same name as the database it is in. A fully qualified column name
-- `db.table.column` must be resolved even though its first part also matches the table name.

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

-- A missing column must still produce an error.
SELECT {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}.nonexistent FROM {CLICKHOUSE_DATABASE:Identifier}.{CLICKHOUSE_DATABASE:Identifier}; -- { serverError UNKNOWN_IDENTIFIER }
