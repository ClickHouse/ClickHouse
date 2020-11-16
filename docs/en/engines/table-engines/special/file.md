---
toc_priority: 37
toc_title: File
---

# File Table Engine {#table_engines-file}

The File table engine keeps the data in a file in one of the supported [file formats](../../../interfaces/formats.md#formats) (`TabSeparated`, `Native`, etc.).

Usage scenarios:

-   Data export from ClickHouse to file.
-   Convert data from one format to another.
-   Updating data in ClickHouse via editing a file on a disk.

## Usage in ClickHouse Server {#usage-in-clickhouse-server}

``` sql
File(Format)
```

The `Format` parameter specifies one of the available file formats. To perform
`SELECT` queries, the format must be supported for input, and to perform
`INSERT` queries – for output. The available formats are listed in the
[Formats](../../../interfaces/formats.md#formats) section.

ClickHouse does not allow to specify filesystem path for`File`. It will use folder defined by [path](../../../operations/server-configuration-parameters/settings.md) setting in server configuration.

When creating table using `File(Format)` it creates empty subdirectory in that folder. When data is written to that table, it’s put into `data.Format` file in that subdirectory.

You may manually create this subfolder and file in server filesystem and then [ATTACH](../../../sql-reference/statements/attach.md) it to table information with matching name, so you can query data from that file.

!!! warning "Warning"
    Be careful with this functionality, because ClickHouse does not keep track of external changes to such files. The result of simultaneous writes via ClickHouse and outside of ClickHouse is undefined.

## Example {#example}

**1.** Set up the `file_engine_table` table:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

By default ClickHouse will create folder `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Manually create `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` containing:

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Query the data:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Usage in ClickHouse-local {#usage-in-clickhouse-local}

In [clickhouse-local](../../../operations/utilities/clickhouse-local.md) File engine accepts file path in addition to `Format`. Default input/output streams can be specified using numeric or human-readable names like `0` or `stdin`, `1` or `stdout`.
**Example:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## Details of Implementation {#details-of-implementation}

-   Multiple `SELECT` queries can be performed concurrently, but `INSERT` queries will wait each other.
-   Supported creating new file by `INSERT` query.
-   If file exists, `INSERT` would append new values in it.
-   Not supported:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   Indices
    -   Replication

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/file/) <!--hide-->
