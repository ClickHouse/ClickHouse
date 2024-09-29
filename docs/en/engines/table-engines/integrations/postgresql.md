---
slug: /en/engines/table-engines/integrations/postgresql
title: PostgreSQL Table Engine
sidebar_position: 160
sidebar_label: PostgreSQL
---

The PostgreSQL engine allows `SELECT` and `INSERT` queries on data stored on a remote PostgreSQL server.

:::note
Currently, only PostgreSQL versions 12 and up are supported.
:::

:::note Replicating or migrating Postgres data with with PeerDB
> In addition to the Postgres table engine, you can use [PeerDB](https://docs.peerdb.io/introduction) by ClickHouse to set up a continuous data pipeline from Postgres to ClickHouse. PeerDB is a tool designed specifically to replicate data from Postgres to ClickHouse using change data capture (CDC).
:::

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

The table structure can differ from the original PostgreSQL table structure:

- Column names should be the same as in the original PostgreSQL table, but you can use just some of these columns and in any order.
- Column types may differ from those in the original PostgreSQL table. ClickHouse tries to [cast](../../../engines/database-engines/postgresql.md#data_types-support) values to the ClickHouse data types.
- The [external_table_functions_use_nulls](../../../operations/settings/settings.md#external-table-functions-use-nulls) setting defines how to handle Nullable columns. Default value: 1. If 0, the table function does not make Nullable columns and inserts default values instead of nulls. This is also applicable for NULL values inside arrays.

**Engine Parameters**

- `host:port` — PostgreSQL server address.
- `database` — Remote database name.
- `table` — Remote table name.
- `user` — PostgreSQL user.
- `password` — User password.
- `schema` — Non-default table schema. Optional.
- `on_conflict` — Conflict resolution strategy. Example: `ON CONFLICT DO NOTHING`. Optional. Note: adding this option will make insertion less efficient.

[Named collections](/docs/en/operations/named-collections.md) (available since version 21.11) are recommended for production environment. Here is an example:

```
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

Some parameters can be overridden by key value arguments:
``` sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

## Implementation Details {#implementation-details}

`SELECT` queries on PostgreSQL side run as `COPY (SELECT ...) TO STDOUT` inside read-only PostgreSQL transaction with commit after each `SELECT` query.

Simple `WHERE` clauses such as `=`, `!=`, `>`, `>=`, `<`, `<=`, and `IN` are executed on the PostgreSQL server.

All joins, aggregations, sorting, `IN [ array ]` conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to PostgreSQL finishes.

`INSERT` queries on PostgreSQL side run as `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` inside PostgreSQL transaction with auto-commit after each `INSERT` statement.

PostgreSQL `Array` types are converted into ClickHouse arrays.

:::note
Be careful - in PostgreSQL an array data, created like a `type_name[]`, may contain multi-dimensional arrays of different dimensions in different table rows in same column. But in ClickHouse it is only allowed to have multidimensional arrays of the same count of dimensions in all table rows in same column.
:::

Supports multiple replicas that must be listed by `|`. For example:

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

Replicas priority for PostgreSQL dictionary source is supported. The bigger the number in map, the less the priority. The highest priority is `0`.

In the example below replica `example01-1` has the highest priority:

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

## Usage Example {#usage-example}

### Table in PostgreSQL

``` text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
 (1 row)
```

### Creating Table in ClickHouse, and connecting to  PostgreSQL table created above

This example uses the [PostgreSQL table engine](/docs/en/engines/table-engines/integrations/postgresql.md) to connect the ClickHouse table to the PostgreSQL table and use both SELECT and INSERT statements to the PostgreSQL database:

``` sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

### Inserting initial data from PostgreSQL table into ClickHouse table, using a SELECT query

The [postgresql table function](/docs/en/sql-reference/table-functions/postgresql.md) copies the data from PostgreSQL to ClickHouse, which is often used for improving the query performance of the data by querying or performing analytics in ClickHouse rather than in PostgreSQL, or can also be used for migrating data from PostgreSQL to ClickHouse. Since we will be copying the data from PostgreSQL to ClickHouse, we will use a MergeTree table engine in ClickHouse and call it postgresql_copy:

``` sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

``` sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

### Inserting incremental data from PostgreSQL table into ClickHouse table

If then performing ongoing synchronization between the PostgreSQL table and ClickHouse table after the initial insert, you can use a WHERE clause in ClickHouse to insert only data added to PostgreSQL based on a timestamp or unique sequence ID.

This would require keeping track of the max ID or timestamp previously added, such as the following:

``` sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

Then inserting values from PostgreSQL table greater than the max

``` sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
WHERE int_id > maxIntID;
```

### Selecting data from the resulting ClickHouse table

``` sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

``` text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

### Using Non-default Schema

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**See Also**

- [The `postgresql` table function](../../../sql-reference/table-functions/postgresql.md)
- [Using PostgreSQL as a dictionary source](../../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-postgresql)

## Related content

- Blog: [ClickHouse and PostgreSQL - a match made in data heaven - part 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
- Blog: [ClickHouse and PostgreSQL - a Match Made in Data Heaven - part 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)
