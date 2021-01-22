---
toc_priority: 37
toc_title: file
---

# file {#file}

Creates a table from a file. This table function is similar to [url](../../sql-reference/table-functions/url.md) and [hdfs](../../sql-reference/table-functions/hdfs.md) ones.

``` sql
file(path, format, structure)
```

**Input parameters**

-   `path` — The relative path to the file from [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [format](../../interfaces/formats.md#formats) of the file.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Example**

Setting `user_files_path` and the contents of the file `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Table from`test.csv` and selection of the first two rows from it:

``` sql
SELECT *
FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

``` sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

**Globs in path**

Multiple path components can have globs. For being processed file should exists and matches to the whole path pattern (not only suffix or prefix).

-   `*` — Substitutes any number of any characters except `/` including empty string.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Constructions with `{}` are similar to the [remote table function](../../sql-reference/table-functions/remote.md)).

**Example**

1.  Suppose we have several files with the following relative paths:

-   ‘some_dir/some_file_1’
-   ‘some_dir/some_file_2’
-   ‘some_dir/some_file_3’
-   ‘another_dir/some_file_1’
-   ‘another_dir/some_file_2’
-   ‘another_dir/some_file_3’

1.  Query the amount of rows in these files:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  Query the amount of rows in all files of these two directories:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "Warning"
    If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.

**Example**

Query the data from files named `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Virtual Columns {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**See Also**

-   [Virtual columns](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[Original article](https://clickhouse.tech/docs/en/query_language/table_functions/file/) <!--hide-->
