---
slug: /en/sql-reference/table-functions/file
sidebar_position: 60
sidebar_label: file
---

# file

Provides a table-like interface to SELECT from and INSERT to files. This table function is similar to the [s3](/docs/en/sql-reference/table-functions/url.md) table function.  Use file() when working with local files, and s3() when working with buckets in S3, GCS, or MinIO.

The `file` function can be used in `SELECT` and `INSERT` queries to read from or write to files.

**Syntax**

``` sql
file(path [,format] [,structure] [,compression])
```

**Parameters**

- `path` — The relative path to the file from [user_files_path](/docs/en/operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Path to file support following globs in read-only mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc', 'def'` — strings.
- `format` — The [format](/docs/en/interfaces/formats.md#formats) of the file.
- `structure` — Structure of the table. Format: `'column1_name column1_type, column2_name column2_type, ...'`.
- `compression` — The existing compression type when used in a `SELECT` query, or the desired compression type when used in an `INSERT` query.  The supported compression types are `gz`, `br`, `xz`, `zst`, `lz4`, and `bz2`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

## File Write Examples

### Write to a TSV file

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

As a result, the data is written into the file `test.tsv`:

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1	2	3
3	2	1
1	3	2
```

### Partitioned Write to multiple TSV files

If you specify `PARTITION BY` expression when inserting data into a file() function, a separate file is created for each partition value. Splitting the data into separate files helps to improve reading operations efficiency.

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

As a result, the data is written into three files: `test_1.tsv`, `test_2.tsv`, and `test_3.tsv`.

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3	2	1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1	3	2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1	2	3
```

## File Read Examples

### SELECT from a CSV file

Setting `user_files_path` and the contents of the file `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Getting data from a table in `test.csv` and selecting the first two rows from it:

``` sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

Getting the first 10 lines of a table that contains 3 columns of [UInt32](/docs/en/sql-reference/data-types/int-uint.md) type from a CSV file:

``` sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 10;
```

### Inserting data from a file into a table:

``` sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```
```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Globs in Path

Multiple path components can have globs. For being processed file must exist and match to the whole path pattern (not only suffix or prefix).

- `*` — Substitutes any number of any characters except `/` including empty string.
- `?` — Substitutes any single character.
- `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`, including `/`.
- `{N..M}` — Substitutes any number in range from N to M including both borders.
- `**` - Fetches all files inside the folder recursively.

Constructions with `{}` are similar to the [remote](remote.md) table function.

**Example**

Suppose we have several files with the following relative paths:

- 'some_dir/some_file_1'
- 'some_dir/some_file_2'
- 'some_dir/some_file_3'
- 'another_dir/some_file_1'
- 'another_dir/some_file_2'
- 'another_dir/some_file_3'

Query the number of rows in these files:

``` sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

Query the number of rows in all files of these two directories:

``` sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

:::note
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**Example**

Query the data from files named `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**Example**

Query the data from all files inside `big_dir` directory recursively:

``` sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**Example**

Query the data from all `file002` files from any folder inside `big_dir` directory recursively:

``` sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

## Virtual Columns

- `_path` — Path to the file.
- `_file` — Name of the file.

## Settings

- [engine_file_empty_if_not_exists](/docs/en/operations/settings/settings.md#engine-file-emptyif-not-exists) - allows to select empty data from a file that doesn't exist. Disabled by default.
- [engine_file_truncate_on_insert](/docs/en/operations/settings/settings.md#engine-file-truncate-on-insert) - allows to truncate file before insert into it. Disabled by default.
- [engine_file_allow_create_multiple_files](/docs/en/operations/settings/settings.md#engine_file_allow_create_multiple_files) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [engine_file_skip_empty_files](/docs/en/operations/settings/settings.md#engine_file_skip_empty_files) - allows to skip empty files while reading. Disabled by default.
- [storage_file_read_method](/docs/en/operations/settings/settings.md#engine-file-emptyif-not-exists) - method of reading data from storage file, one of: read, pread, mmap (only for clickhouse-local). Default value: `pread` for clickhouse-server, `mmap` for clickhouse-local.



**See Also**

- [Virtual columns](/docs/en/engines/table-engines/index.md#table_engines-virtual_columns)
- [Rename files after processing](/docs/en/operations/settings/settings.md#rename_files_after_processing)
