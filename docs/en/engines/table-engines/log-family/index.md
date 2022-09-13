---
toc_folder_title: Log Family
toc_priority: 29
toc_title: Introduction
---

# Log Engine Family {#log-engine-family}

These engines were developed for scenarios when you need to quickly write many small tables (up to about 1 million rows) and read them later as a whole.

Engines of the family:

-   [StripeLog](../../../engines/table-engines/log-family/stripelog.md)
-   [Log](../../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../../engines/table-engines/log-family/tinylog.md)

`Log` family table engines can store data to [HDFS](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-hdfs) or [S3](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-s3) distributed file systems.

## Common Properties {#common-properties}

Engines:

-   Store data on a disk.

-   Append data to the end of file when writing.

-   Support locks for concurrent data access.

    During `INSERT` queries, the table is locked, and other queries for reading and writing data both wait for the table to unlock. If there are no data writing queries, any number of data reading queries can be performed concurrently.

-   Do not support [mutations](../../../sql-reference/statements/alter/index.md#alter-mutations).

-   Do not support indexes.

    This means that `SELECT` queries for ranges of data are not efficient.

-   Do not write data atomically.

    You can get a table with corrupted data if something breaks the write operation, for example, abnormal server shutdown.

## Differences {#differences}

The `TinyLog` engine is the simplest in the family and provides the poorest functionality and lowest efficiency. The `TinyLog` engine does not support parallel data reading by several threads in a single query. It reads data slower than other engines in the family that support parallel reading from a single query and it uses almost as many file descriptors as the `Log` engine because it stores each column in a separate file. Use it only in simple scenarios.

The `Log` and `StripeLog` engines support parallel data reading. When reading data, ClickHouse uses multiple threads. Each thread processes a separate data block. The `Log` engine uses a separate file for each column of the table. `StripeLog` stores all the data in one file. As a result, the `StripeLog` engine uses fewer file descriptors, but the `Log` engine provides higher efficiency when reading data.

[Original article](https://clickhouse.com/docs/en/operations/table_engines/log_family/) <!--hide-->
