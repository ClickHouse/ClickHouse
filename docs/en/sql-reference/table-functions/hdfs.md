---
toc_priority: 45
toc_title: hdfs
---

# hdfs {#hdfs}

Creates a table from files in HDFS. This table function is similar to [url](../../sql-reference/table-functions/url.md) and [file](../../sql-reference/table-functions/file.md) ones.

``` sql
hdfs(URI, format, structure)
```

**Input parameters**

-   `URI` — The relative URI to the file in HDFS. Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [format](../../interfaces/formats.md#formats) of the file.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Example**

Table from `hdfs://hdfs1:9000/test` and selection of the first two rows from it:

``` sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

**Globs in path**

Multiple path components can have globs. For being processed file should exists and matches to the whole path pattern (not only suffix or prefix).

-   `*` — Substitutes any number of any characters except `/` including empty string.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Constructions with `{}` are similar to the [remote table function](../../sql-reference/table-functions/remote.md)).

**Example**

1.  Suppose that we have several files with following URIs on HDFS:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

2.  Query the amount of rows in these files:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3.  Query the amount of rows in all files of these two directories:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "Warning"
    If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.

**Example**

Query the data from files named `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Virtual Columns {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**See Also**

-   [Virtual columns](../../engines/table-engines/index.md#table_engines-virtual_columns)

