---
toc_priority: 11
toc_title: PostgreSQL
---

# PostgreSQL {#postgresql}

The PostgreSQL engine allows to perform `SELECT` and `INSERT` queries on data that is stored on a remote PostgreSQL server.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password'[, `schema`]);
```

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

The table structure can differ from the original PostgreSQL table structure:

-   Column names should be the same as in the original PostgreSQL table, but you can use just some of these columns and in any order.
-   Column types may differ from those in the original PostgreSQL table. ClickHouse tries to [cast](../../../engines/database-engines/postgresql.md#data_types-support) values to the ClickHouse data types.
-   The [external_table_functions_use_nulls](../../../operations/settings/settings.md#external-table-functions-use-nulls) setting defines how to handle Nullable columns. Default value: 1. If 0, the table function does not make Nullable columns and inserts default values instead of nulls. This is also applicable for NULL values inside arrays.

**Engine Parameters**

-   `host:port` — PostgreSQL server address.
-   `database` — Remote database name.
-   `table` — Remote table name.
-   `user` — PostgreSQL user.
-   `password` — User password.
-   `schema` — Non-default table schema. Optional.

## Implementation Details {#implementation-details}

`SELECT` queries on PostgreSQL side run as `COPY (SELECT ...) TO STDOUT` inside read-only PostgreSQL transaction with commit after each `SELECT` query.

Simple `WHERE` clauses such as `=`, `!=`, `>`, `>=`, `<`, `<=`, and `IN` are executed on the PostgreSQL server.

All joins, aggregations, sorting, `IN [ array ]` conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to PostgreSQL finishes.

`INSERT` queries on PostgreSQL side run as `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` inside PostgreSQL transaction with auto-commit after each `INSERT` statement.

PostgreSQL `Array` types are converted into ClickHouse arrays.

!!! info "Note"
    Be careful - in PostgreSQL an array data, created like a `type_name[]`, may contain multi-dimensional arrays of different dimensions in different table rows in same column. But in ClickHouse it is only allowed to have multidimensional arrays of the same count of dimensions in all table rows in same column.
	
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

Table in PostgreSQL:

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

Table in ClickHouse, retrieving data from the PostgreSQL table created above:

``` sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

``` sql
SELECT * FROM postgresql_table WHERE str IN ('test');
```

``` text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

Using Non-default Schema:

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

-   [The `postgresql` table function](../../../sql-reference/table-functions/postgresql.md)
-   [Using PostgreSQL as a source of external dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[Original article](https://clickhouse.tech/docs/en/engines/table-engines/integrations/postgresql/) <!--hide-->
