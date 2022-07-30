---
sidebar_label: MaterializedMySQL
sidebar_position: 70
---

# [experimental] MaterializedMySQL 

:::warning
This is an experimental feature that should not be used in production.
:::

Creates a ClickHouse database with all the tables existing in MySQL, and all the data in those tables. The ClickHouse server works as MySQL replica. It reads `binlog` and performs DDL and DML queries.

## Creating a Database {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedMySQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
[TABLE OVERRIDE table1 (...), TABLE OVERRIDE table2 (...)]
```

**Engine Parameters**

-   `host:port` — MySQL server endpoint.
-   `database` — MySQL database name.
-   `user` — MySQL user.
-   `password` — User password.

## Engine Settings

### max_rows_in_buffer

`max_rows_in_buffer` — Maximum number of rows that data is allowed to cache in memory (for single table and the cache data unable to query). When this number is exceeded, the data will be materialized. Default: `65 505`.

### max_bytes_in_buffer

`max_bytes_in_buffer` —  Maximum number of bytes that data is allowed to cache in memory (for single table and the cache data unable to query). When this number is exceeded, the data will be materialized. Default: `1 048 576`.

### max_flush_data_time

`max_flush_data_time` — Maximum number of milliseconds that data is allowed to cache in memory (for database and the cache data unable to query). When this time is exceeded, the data will be materialized. Default: `1000`.

### max_wait_time_when_mysql_unavailable

`max_wait_time_when_mysql_unavailable` — Retry interval when MySQL is not available (milliseconds). Negative value disables retry. Default: `1000`.

### allows_query_when_mysql_lost
`allows_query_when_mysql_lost` — Allows to query a materialized table when MySQL is lost. Default: `0` (`false`).

### materialized_mysql_tables_list

`materialized_mysql_tables_list` — a comma-separated list of mysql database tables, which will be replicated by MaterializedMySQL database engine. Default value: empty list — means whole tables will be replicated.

```sql
CREATE DATABASE mysql ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', '***')
     SETTINGS
        allows_query_when_mysql_lost=true,
        max_wait_time_when_mysql_unavailable=10000;
```

## Settings on MySQL-server Side

For the correct work of `MaterializedMySQL`, there are few mandatory `MySQL`-side configuration settings that must be set:

### default_authentication_plugin 

`default_authentication_plugin = mysql_native_password` since `MaterializedMySQL` can only authorize with this method.

### gtid_mode

`gtid_mode = on` since GTID based logging is a mandatory for providing correct `MaterializedMySQL` replication.

:::note 
While turning on `gtid_mode` you should also specify `enforce_gtid_consistency = on`.
:::

## Virtual Columns {#virtual-columns}

When working with the `MaterializedMySQL` database engine, [ReplacingMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md) tables are used with virtual `_sign` and `_version` columns.

### \_version

`_version` — Transaction counter. Type [UInt64](../../sql-reference/data-types/int-uint.md).

### \_sign

`_sign` — Deletion mark. Type [Int8](../../sql-reference/data-types/int-uint.md). Possible values:
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
| YEAR                    | [UInt16](../../sql-reference/data-types/int-uint.md)         |
| TIME                    | [Int64](../../sql-reference/data-types/int-uint.md)          |
| ENUM                    | [Enum](../../sql-reference/data-types/enum.md)               |
| STRING                  | [String](../../sql-reference/data-types/string.md)           |
| VARCHAR, VAR_STRING     | [String](../../sql-reference/data-types/string.md)           |
| BLOB                    | [String](../../sql-reference/data-types/string.md)           |
| GEOMETRY                | [String](../../sql-reference/data-types/string.md)           |
| BINARY                  | [FixedString](../../sql-reference/data-types/fixedstring.md) |
| BIT                     | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| SET                     | [UInt64](../../sql-reference/data-types/int-uint.md)         |

[Nullable](../../sql-reference/data-types/nullable.md) is supported.

The data of TIME type in MySQL is converted to microseconds in ClickHouse.

Other types are not supported. If MySQL table contains a column of such type, ClickHouse throws exception "Unhandled data type" and stops replication.

## Specifics and Recommendations {#specifics-and-recommendations}

### Compatibility Restrictions {#compatibility-restrictions}

Apart of the data types limitations there are few restrictions comparing to `MySQL` databases, that should be resolved before replication will be possible:

- Each table in `MySQL` should contain `PRIMARY KEY`.

- Replication for tables, those are containing rows with `ENUM` field values out of range (specified in `ENUM` signature) will not work.

### DDL Queries {#ddl-queries}

MySQL DDL queries are converted into the corresponding ClickHouse DDL queries ([ALTER](../../sql-reference/statements/alter/index.md), [CREATE](../../sql-reference/statements/create/index.md), [DROP](../../sql-reference/statements/drop), [RENAME](../../sql-reference/statements/rename.md)). If ClickHouse cannot parse some DDL query, the query is ignored.

### Data Replication {#data-replication}

`MaterializedMySQL` does not support direct `INSERT`, `DELETE` and `UPDATE` queries. However, they are supported in terms of data replication:

- MySQL `INSERT` query is converted into `INSERT` with `_sign=1`.

- MySQL `DELETE` query is converted into `INSERT` with `_sign=-1`.

- MySQL `UPDATE` query is converted into `INSERT` with `_sign=-1` and `INSERT` with `_sign=1` if the primary key has been changed, or
  `INSERT` with `_sign=1` if not.

### Selecting from MaterializedMySQL Tables {#select}

`SELECT` query from `MaterializedMySQL` tables has some specifics:

- If `_version` is not specified in the `SELECT` query, the
  [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier is used, so only rows with
  `MAX(_version)` are returned for each primary key value.

- If `_sign` is not specified in the `SELECT` query, `WHERE _sign=1` is used by default. So the deleted rows are not
  included into the result set.

- The result includes columns comments in case they exist in MySQL database tables.

### Index Conversion {#index-conversion}

MySQL `PRIMARY KEY` and `INDEX` clauses are converted into `ORDER BY` tuples in ClickHouse tables.

ClickHouse has only one physical order, which is determined by `ORDER BY` clause. To create a new physical order, use
[materialized views](../../sql-reference/statements/create/view.md#materialized).

**Notes**

- Rows with `_sign=-1` are not deleted physically from the tables.
- Cascade `UPDATE/DELETE` queries are not supported by the `MaterializedMySQL` engine, as they are not visible in the
  MySQL binlog.
- Replication can be easily broken.
- Manual operations on database and tables are forbidden.
- `MaterializedMySQL` is affected by the [optimize_on_insert](../../operations/settings/settings.md#optimize-on-insert)
  setting. Data is merged in the corresponding table in the `MaterializedMySQL` database when a table in the MySQL
  server changes.

### Table Overrides {#table-overrides}

Table overrides can be used to customize the ClickHouse DDL queries, allowing you to make schema optimizations for your
application. This is especially useful for controlling partitioning, which is important for the overall performance of
MaterializedMySQL.

These are the schema conversion manipulations you can do with table overrides for MaterializedMySQL:

 * Modify column type. Must be compatible with the original type, or replication will fail. For example,
   you can modify a UInt32 column to UInt64, but you can not modify a String column to Array(String).
 * Modify [column TTL](../table-engines/mergetree-family/mergetree/#mergetree-column-ttl).
 * Modify [column compression codec](../../sql-reference/statements/create/table/#codecs).
 * Add [ALIAS columns](../../sql-reference/statements/create/table/#alias).
 * Add [skipping indexes](../table-engines/mergetree-family/mergetree/#table_engine-mergetree-data_skipping-indexes)
 * Add [projections](../table-engines/mergetree-family/mergetree/#projections). Note that projection optimizations are
   disabled when using `SELECT ... FINAL` (which MaterializedMySQL does by default), so their utility is limited here.
   `INDEX ... TYPE hypothesis` as [described in the v21.12 blog post]](https://clickhouse.com/blog/en/2021/clickhouse-v21.12-released/)
   may be more useful in this case.
 * Modify [PARTITION BY](../table-engines/mergetree-family/custom-partitioning-key/)
 * Modify [ORDER BY](../table-engines/mergetree-family/mergetree/#mergetree-query-clauses)
 * Modify [PRIMARY KEY](../table-engines/mergetree-family/mergetree/#mergetree-query-clauses)
 * Add [SAMPLE BY](../table-engines/mergetree-family/mergetree/#mergetree-query-clauses)
 * Add [table TTL](../table-engines/mergetree-family/mergetree/#mergetree-query-clauses)

```sql
CREATE DATABASE db_name ENGINE = MaterializedMySQL(...)
[SETTINGS ...]
[TABLE OVERRIDE table_name (
    [COLUMNS (
        [col_name [datatype] [ALIAS expr] [CODEC(...)] [TTL expr], ...]
        [INDEX index_name expr TYPE indextype[(...)] GRANULARITY val, ...]
        [PROJECTION projection_name (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]), ...]
    )]
    [ORDER BY expr]
    [PRIMARY KEY expr]
    [PARTITION BY expr]
    [SAMPLE BY expr]
    [TTL expr]
), ...]
```

Example:

```sql
CREATE DATABASE db_name ENGINE = MaterializedMySQL(...)
TABLE OVERRIDE table1 (
    COLUMNS (
        userid UUID,
        category LowCardinality(String),
        timestamp DateTime CODEC(Delta, Default)
    )
    PARTITION BY toYear(timestamp)
),
TABLE OVERRIDE table2 (
    COLUMNS (
        client_ip String TTL created + INTERVAL 72 HOUR
    )
    SAMPLE BY ip_hash
)
```

The `COLUMNS` list is sparse; existing columns are modified as specified, extra ALIAS columns are added. It is not
possible to add ordinary or MATERIALIZED columns.  Modified columns with a different type must be assignable from the
original type. There is currently no validation of this or similar issues when the `CREATE DATABASE` query executes, so
extra care needs to be taken.

You may specify overrides for tables that do not exist yet.

:::warning
It is easy to break replication with table overrides if not used with care. For example:
    
* If an ALIAS column is added with a table override, and a column with the same name is later added to the source
  MySQL table, the converted ALTER TABLE query in ClickHouse will fail and replication stops.
* It is currently possible to add overrides that reference nullable columns where not-nullable are required, such as in
  `ORDER BY` or `PARTITION BY`. This will cause CREATE TABLE queries that will fail, also causing replication to stop.
:::

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
┌─a─┬───b─┬─c────┐
│ 2 │ 222 │ Wow! │
└───┴─────┴──────┘
```

Database in ClickHouse, exchanging data with the MySQL server:

The database and the table created:

``` sql
CREATE DATABASE mysql ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', '***');
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

[Original article](https://clickhouse.com/docs/en/engines/database-engines/materialized-mysql/) <!--hide-->
