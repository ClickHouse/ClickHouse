---
description: 'Creates a table from files in HDFS. This table function is similar to
  the url and file table functions.'
sidebar_label: 'hdfs'
sidebar_position: 80
slug: /sql-reference/table-functions/hdfs
title: 'hdfs'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# hdfs Table Function

Creates a table from files in HDFS. This table function is similar to the [url](../../sql-reference/table-functions/url.md) and [file](../../sql-reference/table-functions/file.md) table functions.

## Syntax {#syntax}

```sql
hdfs(URI, format, structure)
```

## Arguments {#arguments}

| Argument  | Description                                                                                                                                                              |
|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `URI`     | The relative URI to the file in HDFS. Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc', 'def'` — strings. |
| `format`  | The [format](/sql-reference/formats) of the file.                                                                                                                          |
| `structure`| Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                           |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

**example**

Table from `hdfs://hdfs1:9000/test` and selection of the first two rows from it:

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Globs in path {#globs_in_path}

Paths may use globbing. Files must match the whole path pattern, not only the suffix or prefix.

- `*` — Represents arbitrarily many characters except `/` but including the empty string.
- `**` — Represents all files inside a folder recursively.
- `?` — Represents an arbitrary single character.
- `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`. The strings can contain the `/` symbol.
- `{N..M}` — Represents any number `>= N` and `<= M`.

Constructions with `{}` are similar to the [remote](remote.md) and [file](file.md) table functions.

**Example**

1.  Suppose that we have several files with following URIs on HDFS:

- 'hdfs://hdfs1:9000/some_dir/some_file_1'
- 'hdfs://hdfs1:9000/some_dir/some_file_2'
- 'hdfs://hdfs1:9000/some_dir/some_file_3'
- 'hdfs://hdfs1:9000/another_dir/some_file_1'
- 'hdfs://hdfs1:9000/another_dir/some_file_2'
- 'hdfs://hdfs1:9000/another_dir/some_file_3'

2.  Query the amount of rows in these files:

<!-- -->

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3.  Query the amount of rows in all files of these two directories:

<!-- -->

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**Example**

Query the data from files named `file000`, `file001`, ... , `file999`:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## Hive-style partitioning {#hive-style-partitioning}

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.

**Example**

Use virtual column, created with Hive-style partitioning

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE _date > '2020-01-01' AND _country = 'Netherlands' AND _code = 42;
```

## Storage Settings {#storage-settings}

- [hdfs_truncate_on_insert](operations/settings/settings.md#hdfs_truncate_on_insert) - allows to truncate file before insert into it. Disabled by default.
- [hdfs_create_new_file_on_insert](operations/settings/settings.md#hdfs_create_new_file_on_insert) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [hdfs_skip_empty_files](operations/settings/settings.md#hdfs_skip_empty_files) - allows to skip empty files while reading. Disabled by default.

## Related {#related}

- [Virtual columns](../../engines/table-engines/index.md#table_engines-virtual_columns)
