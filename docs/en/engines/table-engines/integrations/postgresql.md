---
toc_priority: 11
toc_title: PostgreSQL
---

#PostgreSQL {#postgresql}

The PostgreSQL engine allows to perform `SELECT` and `INSERT` queries on data that is stored on a remote PostgreSQL server.

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

**Engine Parameters**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

## Usage Example {#usage-example}

Consider the table in ClickHouse, retrieving data from the PostgreSQL table:

``` sql
CREATE TABLE test_table
(
    `int_id` Int32,
    'value' Int32
)
ENGINE = PostgreSQL('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword');
```

``` sql
SELECT * FROM test_database.test_table;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

Inserting data from ClickHouse into the PosegreSQL table:

``` sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

## See Also {#see-also}

-   [The 'postgresql' table function](../../../sql-reference/table-functions/postgresql.md)
-   [Using PostgreSQL as a source of external dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/postgresql/) <!--hide-->
