---
toc_priority: 35
toc_title: PostgreSQL
---

# PostgreSQL {#postgresql}

Allows to connect to databases on a remote [PostgreSQL](https://www.postgresql.org) server. Supports read and write operations (`SELECT` and `INSERT` queries) to exchange data between ClickHouse and PostgreSQL.

Gives the real-time access to table list and table structure from remote PostgreSQL with the help of `SHOW TABLES` and `DESCRIBE TABLE` queries.

Supports table structure modifications (`ALTER TABLE ... ADD|DROP COLUMN`). If `use_table_cache` parameter (see the Engine Parameters below) it set to `1`, the table structure is cached and not checked for being modified, but can be updated with `DETACH` and `ATTACH` queries.

## Creating a Database {#creating-a-database}

``` sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**Engine Parameters**

-   `host:port` — PostgreSQL server address.
-   `database` — Remote database name.
-   `user` — PostgreSQL user.
-   `password` — User password.
-   `schema` — PostgreSQL schema.
-   `use_table_cache` —  Defines if the database table structure is cached or not. Optional. Default value: `0`.

## Data Types Support {#data_types-support}

| PostgerSQL       | ClickHouse                                                   |
|------------------|--------------------------------------------------------------|
| DATE             | [Date](../../sql-reference/data-types/date.md)               |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)       |
| REAL             | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)           |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md)       |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)          |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)           |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))|
| ARRAY            | [Array](../../sql-reference/data-types/array.md)             |


## Examples of Use {#examples-of-use}

Database in ClickHouse, exchanging data with the PostgreSQL server:

``` sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 1);
```

``` sql
SHOW DATABASES;
```

``` text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

``` sql
SHOW TABLES FROM test_database;
```

``` text
┌─name───────┐
│ test_table │
└────────────┘
```

Reading data from the PostgreSQL table:

``` sql
SELECT * FROM test_database.test_table;
```

``` text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

Writing data to the PostgreSQL table:

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

Consider the table structure was modified in PostgreSQL:

``` sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

As the `use_table_cache` parameter was set to `1` when the database was created, the table structure in ClickHouse was cached and therefore not modified:

``` sql
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

After detaching the table and attaching it again, the structure was updated:

``` sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

[Original article](https://clickhouse.com/docs/en/database-engines/postgresql/) <!--hide-->
