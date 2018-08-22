<a name="table_engines-file"></a>

# File(Format)

Manages data in a single file on disk in the specified format.

Usage examples:

- Downloading data from ClickHouse to a file.
- Converting data from one format to another.
- Updating data in ClickHouse by editing the file on disk.

## Using the engine in the ClickHouse server

```
File(Format)
```

`The format` must be one that ClickHouse can use both in `INSERT` queries and in `SELECT` queries. For the full list of supported formats, see [Formats](../../interfaces/formats.md#formats).

The ClickHouse server does not allow you to specify the path to the file tthat `File` will work with. It uses the path to the storage that is specified by the [path](../server_settings/settings.md#server_settings-path) parameter in the server configuration.

When creating a table using `File(Format)`, the ClickHouse server creates a directory with the name of the table in the storage, and puts the `data.Format` file there after it is added to the data table.

You can manually create the table's directory in storage, put the file there, and then use [ATTACH](../../query_language/misc.md#queries-attach) the ClickHouse server to add information about the table corresponding to the directory name and read data from the file.

!!! Warning:
Be careful with this functionality, because the ClickHouse server does not track external data changes. If the file will be written to simultaneously from the ClickHouse server and from an external source, the result is unpredictable.

**Example:**

**1.** Create a `file_engine_table` table on the server :

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

In the default configuration, the ClickHouse server creates the directory `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Manually create the file `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` with the contents:

```bash
$cat data.TabSeparated
one	1
two	2
```

**3.** Request data:

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Using the engine in clickhouse-local

In [clickhouse-local](../utils/clickhouse-local.md#utils-clickhouse-local) the engine takes the file path as a parameter, as well as the format. The standard input/output streams can be specified using numbers or letters: `0` or `stdin`, `1` or `stdout`.

**Example:**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## Details of Implementation

- Multi-stream reading and single-stream writing are supported.
- Not supported:
    - `ALTER` and `SELECT...SAMPLE` operations.
    - Indexes.
    - Replication.
