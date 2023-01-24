---
sidebar_position: 42
sidebar_label: postgresql
---

# postgresql

Allows `SELECT` and `INSERT` queries to be performed on data that is stored on a remote PostgreSQL server.

**Syntax**

``` sql
postgresql('host:port', 'database', 'table', 'user', 'password'[, `schema`])
```

**Arguments**

-   `host:port` — PostgreSQL server address.
-   `database` — Remote database name.
-   `table` — Remote table name.
-   `user` — PostgreSQL user.
-   `password` — User password.
-   `schema` — Non-default table schema. Optional.

**Returned Value**

A table object with the same columns as the original PostgreSQL table.

:::note    
In the `INSERT` query to distinguish table function `postgresql(...)` from table name with column names list you must use keywords `FUNCTION` or `TABLE FUNCTION`. See examples below.
:::

## Implementation Details

`SELECT` queries on PostgreSQL side run as `COPY (SELECT ...) TO STDOUT` inside read-only PostgreSQL transaction with commit after each `SELECT` query.

Simple `WHERE` clauses such as `=`, `!=`, `>`, `>=`, `<`, `<=`, and `IN` are executed on the PostgreSQL server.

All joins, aggregations, sorting, `IN [ array ]` conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to PostgreSQL finishes.

`INSERT` queries on PostgreSQL side run as `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` inside PostgreSQL transaction with auto-commit after each `INSERT` statement.

PostgreSQL Array types converts into ClickHouse arrays.

:::note    
Be careful, in PostgreSQL an array data type column like Integer[] may contain arrays of different dimensions in different rows, but in ClickHouse it is only allowed to have multidimensional arrays of the same dimension in all rows.
:::

Supports multiple replicas that must be listed by `|`. For example:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

or

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

Supports replicas priority for PostgreSQL dictionary source. The bigger the number in map, the less the priority. The highest priority is `0`.

**Examples**

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

Selecting data from ClickHouse:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Inserting:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
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

-   [The PostgreSQL table engine](../../engines/table-engines/integrations/postgresql.md)
-   [Using PostgreSQL as a source of external dictionary](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[Original article](https://clickhouse.com/docs/en/sql-reference/table-functions/postgresql/) <!--hide-->
