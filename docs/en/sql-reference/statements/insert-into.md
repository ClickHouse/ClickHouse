---
slug: /en/sql-reference/statements/insert-into
sidebar_position: 33
sidebar_label: INSERT INTO
---

# INSERT INTO Statement

Inserts data into a table.

**Syntax**

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

You can specify a list of columns to insert using  the `(c1, c2, c3)`. You can also use an expression with column [matcher](../../sql-reference/statements/select/index.md#asterisk) such as `*` and/or [modifiers](../../sql-reference/statements/select/index.md#select-modifiers) such as [APPLY](../../sql-reference/statements/select/index.md#apply-modifier), [EXCEPT](../../sql-reference/statements/select/index.md#except-modifier), [REPLACE](../../sql-reference/statements/select/index.md#replace-modifier).

For example, consider the table:

``` sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

``` sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

If you want to insert data in all the columns, except 'b', you need to pass so many values how many columns you chose in parenthesis then:

``` sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

``` sql
SELECT * FROM insert_select_testtable;
```

```
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

In this example, we see that the second inserted row has `a` and `c` columns filled by the passed values, and `b` filled with value by default. It is also possible to use `DEFAULT` keyword to insert default values:

``` sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

If a list of columns does not include all existing columns, the rest of the columns are filled with:

- The values calculated from the `DEFAULT` expressions specified in the table definition.
- Zeros and empty strings, if `DEFAULT` expressions are not defined.

Data can be passed to the INSERT in any [format](../../interfaces/formats.md#formats) supported by ClickHouse. The format must be specified explicitly in the query:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT ... VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse removes all spaces and one line feed (if there is one) before the data. When forming a query, we recommend putting the data on a new line after the query operators (this is important if the data begins with spaces).

Example:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

You can insert data separately from the query by using the [command-line client](/docs/en/integrations/sql-clients/clickhouse-client-local) or the [HTTP interface](/docs/en/interfaces/http/).

:::note
If you want to specify `SETTINGS` for `INSERT` query then you have to do it _before_ `FORMAT` clause since everything after `FORMAT format_name` is treated as data. For example:
```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```
:::

## Constraints

If table has [constraints](../../sql-reference/statements/create/table.md#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — server will raise an exception containing constraint name and expression, the query will be stopped.

## Inserting the Results of SELECT

**Syntax**

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

Columns are mapped according to their position in the SELECT clause. However, their names in the SELECT expression and the table for INSERT may differ. If necessary, type casting is performed.

None of the data formats except Values allow setting values to expressions such as `now()`, `1 + 2`, and so on. The Values format allows limited use of expressions, but this is not recommended, because in this case inefficient code is used for their execution.

Other queries for modifying data parts are not supported: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
However, you can delete old data using `ALTER TABLE ... DROP PARTITION`.

`FORMAT` clause must be specified in the end of query if `SELECT` clause contains table function [input()](../../sql-reference/table-functions/input.md).

To insert a default value instead of `NULL` into a column with not nullable data type, enable [insert_null_as_default](../../operations/settings/settings.md#insert_null_as_default) setting.

`INSERT` also supports CTE(common table expression). For example, the following two statements are equivalent:

``` sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```


## Inserting Data from a File

**Syntax**

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

Use the syntax above to insert data from a file, or files, stored on the **client** side. `file_name` and `type` are string literals. Input file [format](../../interfaces/formats.md) must be set in the `FORMAT` clause.

Compressed files are supported. The compression type is detected by the extension of the file name. Or it can be explicitly specified in a `COMPRESSION` clause. Supported types are: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

This functionality is available in the [command-line client](../../interfaces/cli.md) and [clickhouse-local](../../operations/utilities/clickhouse-local.md).

**Examples**

### Single file with FROM INFILE
Execute the following queries using [command-line client](../../interfaces/cli.md):

```bash
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

Result:

```text
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

### Multiple files with FROM INFILE using globs

This example is very similar to the previous one but inserts from multiple files using `FROM INFILE 'input_*.csv`.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
In addition to selecting multiple files with `*`, you can use ranges (`{1,2}` or `{1..9}`) and other [glob substitutions](/docs/en/sql-reference/table-functions/file.md/#globs-in-path). These three all would work with the above example:
```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```
:::

## Inserting using a Table Function

Data can be inserted into tables referenced by [table functions](../../sql-reference/table-functions/index.md).

**Syntax**
``` sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**Example**

[remote](../../sql-reference/table-functions/index.md#remote) table function is used in the following queries:

``` sql
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

Result:

``` text
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

## Inserting into ClickHouse Cloud

By default, services on ClickHouse Cloud provide multiple replicas for high availability. When you connect to a service, a connection is established to one of these replicas.

After an `INSERT` succeeds, data is written to the underlying storage. However, it may take some time for replicas to receive these updates. Therefore, if you use a different connection that executes a `SELECT` query on one of these other replicas, the updated data may not yet be reflected.

It is possible to use the `select_sequential_consistency` to force the replica to receive the latest updates. Here is an example of a SELECT query using this setting:

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

Note that using `select_sequential_consistency` will increase the load on ClickHouse Keeper (used by ClickHouse Cloud internally) and may result in slower performance depending on the load on the service. We recommend against enabling this setting unless necessary. The recommended approach is to execute read/writes in the same session or to use a client driver that uses the native protocol (and thus supports sticky connections).

## Inserting into a replicated setup

In a replicated setup, data will be visible on other replicas after it has been replicated. Data begins being replicated (downloaded on other replicas) immediately after an `INSERT`. This differs from ClickHouse Cloud, where data is immediately written to shared storage and replicas subscribe to metadata changes.

Note that for replicated setups, `INSERTs` can sometimes take a considerable amount of time (in the order of one second) as it requires committing to ClickHouse Keeper for distributed consensus. Using S3 for storage also adds additional latency.

## Performance Considerations

`INSERT` sorts the input data by primary key and splits them into partitions by a partition key. If you insert data into several partitions at once, it can significantly reduce the performance of the `INSERT` query. To avoid this:

- Add data in fairly large batches, such as 100,000 rows at a time.
- Group data by a partition key before uploading it to ClickHouse.

Performance will not decrease if:

- Data is added in real time.
- You upload data that is usually sorted by time.

### Asynchronous inserts

It is possible to asynchronously insert data in small but frequent inserts. The data from such insertions is combined into batches and then safely inserted into a table. To use asynchronous inserts, enable the [`async_insert`](../../operations/settings/settings.md#async-insert) setting.

Using `async_insert` or the [`Buffer` table engine](/en/engines/table-engines/special/buffer) results in additional buffering.

### Large or long-running inserts

When you are inserting large amounts of data, ClickHouse will optimize write performance through a process called "squashing". Small blocks of inserted data in memory are merged and squashed into larger blocks before being written to disk. Squashing reduces the overhead associated with each write operation. In this process, inserted data will be available to query after ClickHouse completes writing each [`max_insert_block_size`](/en/operations/settings/settings#max_insert_block_size) rows.

**See Also**

- [async_insert](../../operations/settings/settings.md#async-insert)
- [async_insert_threads](../../operations/settings/settings.md#async-insert-threads)
- [wait_for_async_insert](../../operations/settings/settings.md#wait-for-async-insert)
- [wait_for_async_insert_timeout](../../operations/settings/settings.md#wait-for-async-insert-timeout)
- [async_insert_max_data_size](../../operations/settings/settings.md#async-insert-max-data-size)
- [async_insert_busy_timeout_ms](../../operations/settings/settings.md#async-insert-busy-timeout-ms)
- [async_insert_stale_timeout_ms](../../operations/settings/settings.md#async-insert-stale-timeout-ms)
