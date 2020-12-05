---
toc_priority: 29
toc_title: MaterializeMySQL
---

# MaterializeMySQL {#materialize-mysql}

 Creates ClickHouse database with all the tables existing in MySQL, and all the data in those tables. 

 ClickHouse server works as MySQL replica. It reads binlog and performs DDL and DML queries.   

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
| STRING                  | [String](../../sql-reference/data-types/string.md)           |
| VARCHAR, VAR_STRING     | [String](../../sql-reference/data-types/string.md)           |
| BLOB                    | [String](../../sql-reference/data-types/string.md)           |

Other types are not supported. If MySQL table contains a column of such type, ClickHouse throws exception "Unhandled data type" and stops replication.

[Nullable](../../sql-reference/data-types/nullable.md) is supported.

## Specifics and Recommendations {#specifics-and-recommendations}

### DDL Queries

MySQL DDL queries are converted into the corresponding ClickHouse DDL queries ([ALTER](../../sql-reference/statements/alter/index.md), [CREATE](../../sql-reference/statements/create/index.md), [DROP](../../sql-reference/statements/drop.md), [RENAME](../../sql-reference/statements/rename.md)). If ClickHouse cannot parse some DDL query, the query is ignored.

### DML Queries

`MaterializeMySQL` engine supports only `INSERT` and `SELECT` queries.

MySql `DELETE` query is converted into `INSERT` with `_sign=-1`. 

MySQL `UPDATE` query is converted into `INSERT` with `_sign=-1` and `INSERT` with `_sign=1`.

`SELECT` query has some specifics:

- If `_version` is not specified in the `SELECT` query, [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier is used. So only rows with `MAX(_version)` are selected.

- If `_sign` is not specified in the `SELECT` query, `WHERE _sign=1` is used by default, so the deleted rows are not included into the result set. 

### Index Conversion

MySQL `PRIMARY KEY` and `INDEX` clauses are converted into `ORDER BY` tuples in ClickHouse tables.

ClickHouse has only one physical order, which is determined by `ORDER BY` clause. To create a new physical order, use [materialized views](../../sql-reference/statements/create/view.md#materialized).

 **Notes**

 - Rows with `_sign=-1` are not deleted physically from the tables. 
 - Cascade `UPDATE/DELETE` queries are not supported by the `MaterializeMySQL` engine.
 - Replication can be easily broken.
 - Manual operations on database and tables are forbidden.

## Examples of Use {#examples-of-use}

Table in MySQL:

``` text
mysql> CREATE DATABASE db;
mysql> CREATE TABLE db.test (a INT PRIMARY KEY, b INT);
mysql> INSERT INTO db.test VALUES (1, 11), (2, 22);
mysql> DELETE FROM db.test WHERE a=1;
mysql> ALTER TABLE db.test ADD COLUMN c VARCHAR(16);
mysql> UPDATE db.test SET c='Wow!', b=222;
mysql> SELECT * FROM test;

+---+------+------+ 
| a |    b |    c |
+---+------+------+ 
| 2 |  222 | Wow! |
+---+------+------+
```

Database in ClickHouse, exchanging data with the MySQL server:

``` sql
CREATE DATABASE mysql ENGINE = MaterializeMySQL('localhost:3306', 'db', 'user', '***');
SHOW TABLES FROM mysql;
```

``` text
┌─name─┐
│ test │
└──────┘
```

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬──b─┐ 
│ 1 │ 11 │ 
│ 2 │ 22 │ 
└───┴────┘
```

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬───b─┬─c────┐ 
│ 2 │ 222 │ Wow! │ 
└───┴─────┴──────┘
```

``` sql
INSERT INTO mysql_db.test VALUES (3,4);
```

``` sql
SELECT * FROM mysql_db.test;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

[Original article](https://clickhouse.tech/docs/en/database_engines/materialize-mysql/) <!--hide-->
