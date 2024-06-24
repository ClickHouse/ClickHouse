---
slug: /en/sql-reference/table-functions/file
sidebar_position: 60
sidebar_label: file
---

# file

A table engine which provides a table-like interface to SELECT from and INSERT into files, similar to the [s3](/docs/en/sql-reference/table-functions/url.md) table function.  Use `file()` when working with local files, and `s3()` when working with buckets in object storage such as S3, GCS, or MinIO.

The `file` function can be used in `SELECT` and `INSERT` queries to read from or write to files.

**Syntax**

``` sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

**Parameters**

- `path` — The relative path to the file from [user_files_path](/docs/en/operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Supports in read-only mode the following [globs](#globs_in_path): `*`, `?`, `{abc,def}` (with `'abc'` and `'def'` being strings) and `{N..M}` (with `N` and `M` being numbers).
- `path_to_archive` - The relative path to a zip/tar/7z archive. Supports the same globs as `path`.
- `format` — The [format](/docs/en/interfaces/formats.md#formats) of the file.
- `structure` — Structure of the table. Format: `'column1_name column1_type, column2_name column2_type, ...'`.
- `compression` — The existing compression type when used in a `SELECT` query, or the desired compression type when used in an `INSERT` query. Supported compression types are `gz`, `br`, `xz`, `zst`, `lz4`, and `bz2`.


**Returned value**

A table for reading or writing data in a file.

## Examples for Writing to a File

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

### Partitioned write to multiple TSV files

If you specify a `PARTITION BY` expression when inserting data into a table function of type `file()`, then a separate file is created for each partition. Splitting the data into separate files helps to improve performance of read operations.

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

## Examples for Reading from a File

### SELECT from a CSV file

First, set `user_files_path` in the server configuration and prepare a file `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Then, read data from `test.csv` into a table and select its first two rows:

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

Reading data from `table.csv`, located in `archive1.zip` or/and `archive2.zip`:

``` sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

## Globs in path {#globs_in_path}

Paths may use globbing. Files must match the whole path pattern, not only the suffix or prefix.

- `*` — Represents arbitrarily many characters except `/` but including the empty string.
- `?` — Represents an arbitrary single character.
- `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`. The strings can contain the `/` symbol.
- `{N..M}` — Represents any number `>= N` and `<= M`.
- `**` - Represents all files inside a folder recursively.

Constructions with `{}` are similar to the [remote](remote.md) and [hdfs](hdfs.md) table functions.

**Example**

Suppose there are these files with the following relative paths:

- `some_dir/some_file_1`
- `some_dir/some_file_2`
- `some_dir/some_file_3`
- `another_dir/some_file_1`
- `another_dir/some_file_2`
- `another_dir/some_file_3`

Query the total number of rows in all files:

``` sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

An alternative path expression which achieves the same:

``` sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

:::note
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**Example**

Query the total number of rows in files named `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**Example**

Query the total number of rows from all files inside directory `big_dir/` recursively:

``` sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**Example**

Query the total number of rows from all files `file002` inside any folder in directory `big_dir/` recursively:

``` sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinalty(String)`.
- `_file` — Name of the file. Type: `LowCardinalty(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.

## Settings {#settings}

- [engine_file_empty_if_not_exists](/docs/en/operations/settings/settings.md#engine-file-empty_if-not-exists) - allows to select empty data from a file that doesn't exist. Disabled by default.
- [engine_file_truncate_on_insert](/docs/en/operations/settings/settings.md#engine-file-truncate-on-insert) - allows to truncate file before insert into it. Disabled by default.
- [engine_file_allow_create_multiple_files](/docs/en/operations/settings/settings.md#engine_file_allow_create_multiple_files) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [engine_file_skip_empty_files](/docs/en/operations/settings/settings.md#engine_file_skip_empty_files) - allows to skip empty files while reading. Disabled by default.
- [storage_file_read_method](/docs/en/operations/settings/settings.md#engine-file-empty_if-not-exists) - method of reading data from storage file, one of: read, pread, mmap (only for clickhouse-local). Default value: `pread` for clickhouse-server, `mmap` for clickhouse-local.


**See Also**

- [Virtual columns](/docs/en/engines/table-engines/index.md#table_engines-virtual_columns)
- [Rename files after processing](/docs/en/operations/settings/settings.md#rename_files_after_processing)
