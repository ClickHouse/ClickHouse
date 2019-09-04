# HDFS {#table_engines-hdfs}

This engine provides integration with [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) ecosystem by allowing to manage data on [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.htmll)via ClickHouse. This engine is similar
to the [File](file.md) and [URL](url.md) engines, but provides Hadoop-specific features.

## Usage

```
ENGINE = HDFS(URI, format)
```
The `URI` parameter is the whole file URI in HDFS.
The `format` parameter specifies one of the available file formats. To perform
`SELECT` queries, the format must be supported for input, and to perform
`INSERT` queries -- for output. The available formats are listed in the
[Formats](../../interfaces/formats.md#formats) section.

**Example:**

**1.** Set up the `hdfs_engine_table` table:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Fill file:
``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Query the data:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Implementation Details

- Reads and writes can be parallel
- Not supported:
    - `ALTER` and `SELECT...SAMPLE` operations.
    - Indexes.
    - Replication.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/hdfs/) <!--hide-->
