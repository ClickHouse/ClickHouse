---
toc_priority: 29
toc_title: MaterializeMySQL
---

# MaterializeMySQL {#materialize-mysql}

Creates ClickHouse database with all the tables existing in MySQL, and all the data in those tables.

ClickHouse server works as MySQL replica. It reads binlog and performs DDL and DML queries.

This feature is experimental.

## Creating a Database {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializeMySQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
```

**Engine Parameters**

-   `host:port` — MySQL server endpoint.
-   `database` — MySQL database name.
-   `user` — MySQL user.
-   `password` — User password.

## Virtual columns {#virtual-columns}

When working with the `MaterializeMySQL` database engine, [ReplacingMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md) tables are used with virtual `_sign` and `_version` columns.
 
- `_version` — Transaction counter. Type [UInt64](../../sql-reference/data-types/int-uint.md).
- `_sign` — Deletion mark. Type [Int8](../../sql-reference/data-types/int-uint.md). Possible values:
    - `1` — Row is not deleted, 
    - `-1` — Row is deleted.

## Data Types Support {#data_types-support}

| MySQL                   | ClickHouse                                                   |
|-------------------------|--------------------------------------------------------------|
| TINY                    | [Int8](../../sql-reference/data-types/int-uint.md)           |
| SHORT                   | [Int16](../../sql-reference/data-types/int-uint.md)          |
| INT24                   | [Int32](../../sql-reference/data-types/int-uint.md)          |
| LONG                    | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| LONGLONG                | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| FLOAT                   | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE                  | [Float64](../../sql-reference/data-types/float.md)           |
| DECIMAL, NEWDECIMAL     | [Decimal](../../sql-reference/data-types/decimal.md)         |
| DATE, NEWDATE           | [Date](../../sql-reference/data-types/date.md)               |
| DATETIME, TIMESTAMP     | [DateTime](../../sql-reference/data-types/datetime.md)       |
| DATETIME2, TIMESTAMP2   | [DateTime64](../../sql-reference/data-types/datetime64.md)   |
| ENUM                    | [Enum](../../sql-reference/data-types/enum.md)               |
| STRING                  | [String](../../sql-reference/data-types/string.md)           |
| VARCHAR, VAR_STRING     | [String](../../sql-reference/data-types/string.md)           |
| BLOB                    | [String](../../sql-reference/data-types/string.md)           |

Other types are not supported. If MySQL table contains a column of such type, ClickHouse throws exception "Unhandled data type" and stops replication.

[Nullable](../../sql-reference/data-types/nullable.md) is supported.

## Specifics and Recommendations {#specifics-and-recommendations}

### DDL Queries {#ddl-queries}

MySQL DDL queries are converted into the corresponding ClickHouse DDL queries ([ALTER](../../sql-reference/statements/alter/index.md), [CREATE](../../sql-reference/statements/create/index.md), [DROP](../../sql-reference/statements/drop.md), [RENAME](../../sql-reference/statements/rename.md)). If ClickHouse cannot parse some DDL query, the query is ignored.

### Data Replication {#data-replication}

`MaterializeMySQL` does not support direct `INSERT`, `DELETE` and `UPDATE` queries. However, they are supported in terms of data replication:

- MySQL `INSERT` query is converted into `INSERT` with `_sign=1`.

- MySQL `DELETE` query is converted into `INSERT` with `_sign=-1`. 

- MySQL `UPDATE` query is converted into `INSERT` with `_sign=-1` and `INSERT` with `_sign=1`.

### Selecting from MaterializeMySQL Tables {#select}

`SELECT` query from `MaterializeMySQL` tables has some specifics:

- If `_version` is not specified in the `SELECT` query, [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier is used. So only rows with `MAX(_version)` are selected.

- If `_sign` is not specified in the `SELECT` query, `WHERE _sign=1` is used by default. So the deleted rows are not included into the result set.

### Index Conversion {#index-conversion}

MySQL `PRIMARY KEY` and `INDEX` clauses are converted into `ORDER BY` tuples in ClickHouse tables.

ClickHouse has only one physical order, which is determined by `ORDER BY` clause. To create a new physical order, use [materialized views](../../sql-reference/statements/create/view.md#materialized).

**Notes**

- Rows with `_sign=-1` are not deleted physically from the tables.
- Cascade `UPDATE/DELETE` queries are not supported by the `MaterializeMySQL` engine.
- Replication can be easily broken.
- Manual operations on database and tables are forbidden.
- `MaterializeMySQL` is influenced by [optimize_on_insert](../../operations/settings/settings.md#optimize-on-insert) setting. The data is merged in the corresponding table in the `MaterializeMySQL` database when a table in the MySQL server changes.

## Examples of Use {#examples-of-use}

Queries in MySQL:

``` sql
mysql> CREATE DATABASE db;
mysql> CREATE TABLE db.test (a INT PRIMARY KEY, b INT);
mysql> INSERT INTO db.test VALUES (1, 11), (2, 22);
mysql> DELETE FROM db.test WHERE a=1;
mysql> ALTER TABLE db.test ADD COLUMN c VARCHAR(16);
mysql> UPDATE db.test SET c='Wow!', b=222;
mysql> SELECT * FROM test;
```

```text
+---+------+------+ 
| a |    b |    c |
+---+------+------+ 
| 2 |  222 | Wow! |
+---+------+------+
```

Database in ClickHouse, exchanging data with the MySQL server:

The database and the table created:

``` sql
CREATE DATABASE mysql ENGINE = MaterializeMySQL('localhost:3306', 'db', 'user', '***');
SHOW TABLES FROM mysql;
```

``` text
┌─name─┐
│ test │
└──────┘
```

After inserting data:

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬──b─┐ 
│ 1 │ 11 │ 
│ 2 │ 22 │ 
└───┴────┘
```

After deleting data, adding the column and updating:

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬───b─┬─c────┐ 
│ 2 │ 222 │ Wow! │ 
└───┴─────┴──────┘
```

[Original article](https://clickhouse.tech/docs/en/engines/database-engines/materialize-mysql/) <!--hide-->
