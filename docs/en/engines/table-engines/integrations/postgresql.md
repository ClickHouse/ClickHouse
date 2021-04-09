---
toc_priority: 8
toc_title: PostgreSQL
---

# PostgreSQL {#postgresql}

The PostgreSQL engine allows you to perform `SELECT` queries on data that is stored on a remote PostgreSQL server.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password');
```

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

The table structure can differ from the original PostgreSQL table structure:

-   Column names should be the same as in the original PostgreSQL table, but you can use just some of these columns and in any order.
-   Column types may differ from those in the original PostgreSQL table. ClickHouse tries to [cast](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) values to the ClickHouse data types.
-   Setting `external_table_functions_use_nulls` defines how to handle Nullable columns. Default is 1, if 0 - table function will not make nullable columns and will insert default values instead of nulls. This is also applicable for null values inside array data types.

**Engine Parameters**

-   `host:port` — PostgreSQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — PostgreSQL user.

-   `password` — User password.

SELECT Queries on PostgreSQL side run as `COPY (SELECT ...) TO STDOUT` inside read-only PostgreSQL transaction with commit after each `SELECT` query.

Simple `WHERE` clauses such as `=, !=, >, >=, <, <=, IN` are executed on the PostgreSQL server.

All joins, aggregations, sorting, `IN [ array ]` conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to PostgreSQL finishes.

INSERT Queries on PostgreSQL side run as `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` inside PostgreSQL transaction with auto-commit after each `INSERT` statement.

PostgreSQL Array types converts into ClickHouse arrays.
Be careful in PostgreSQL an array data created like a type_name[] may contain multi-dimensional arrays of different dimensions in different table rows in same column, but in ClickHouse it is only allowed to have multidimensional arrays of the same count of dimensions in all table rows in same column.

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

postgres=# insert into test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> select * from test;
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
SELECT * FROM postgresql_table WHERE str IN ('test') 
```

``` text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
1 rows in set. Elapsed: 0.019 sec.
```


## See Also {#see-also}

-   [The ‘postgresql’ table function](../../../sql-reference/table-functions/postgresql.md)
-   [Using PostgreSQL as a source of external dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[Original article](https://clickhouse.tech/docs/en/engines/table-engines/integrations/postgresql/) <!--hide-->
