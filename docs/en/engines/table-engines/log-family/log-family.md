---
toc_priority: 31
toc_title: Introduction
---

# Log Engine Family {#log-engine-family}

These engines were developed for scenarios when you need to quickly write many small tables (up to about 1 million rows) and read them later as a whole.

Engines of the family:

-   [StripeLog](stripelog.md)
-   [Log](log.md)
-   [TinyLog](tinylog.md)

## Common Properties {#common-properties}

Engines:

-   Store data on a disk.

-   Append data to the end of file when writing.

-   Support locks for concurrent data access.

    During `INSERT` queries, the table is locked, and other queries for reading and writing data both wait for the table to unlock. If there are no data writing queries, any number of data reading queries can be performed concurrently.

-   Do not support [mutation](../../../sql-reference/statements/alter.md#alter-mutations) operations.

-   Do not support indexes.

    This means that `SELECT` queries for ranges of data are not efficient.

-   Do not write data atomically.

    You can get a table with corrupted data if something breaks the write operation, for example, abnormal server shutdown.

## Differences {#differences}

The `TinyLog` engine is the simplest in the family and provides the poorest functionality and lowest efficiency. The `TinyLog` engine doesn’t support parallel data reading by several threads. It reads data slower than other engines in the family that support parallel reading and it uses almost as many descriptors as the `Log` engine because it stores each column in a separate file. Use it in simple low-load scenarios.

The `Log` and `StripeLog` engines support parallel data reading. When reading data, ClickHouse uses multiple threads. Each thread processes a separate data block. The `Log` engine uses a separate file for each column of the table. `StripeLog` stores all the data in one file. As a result, the `StripeLog` engine uses fewer descriptors in the operating system, but the `Log` engine provides higher efficiency when reading data.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/log_family/) <!--hide-->
