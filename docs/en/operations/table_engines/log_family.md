# Log Engine Family

These engines were developed for scenarios when you need to quickly write many small tables (up to about 1 million rows) and read them later as a whole.

Engines of the family:

- [StripeLog](stripelog.md)
- [Log](log.md)
- [TinyLog](tinylog.md)

## Common properties

Engines:

- Store data on a disk.
- Append data to the end of file when writing.
- Support locks for concurrent data access.
    
    During `INSERT` query the table is locked, and other queries for reading and writing data both wait for unlocking. If there are no writing data queries, any number of reading data queries can be performed concurrently.

- Do not support [mutation](../../query_language/alter.md#alter-mutations) operations.
- Do not support indexes.

    This means that `SELECT` queries for ranges of data are not efficient.

- Do not write data atomically.

    You can get a table with corrupted data if something breaks the write operation, for example, abnormal server shutdown.


## Differences

The `TinyLog` engine is the simplest in the family and provides the poorest functionality and lowest efficiency. The `TinyLog` engine does not support a parallel reading of data. It reads the data slower than other engines of the family that have parallel reading, and it uses almost as many descriptors as the `Log` engine because it stores each column in a separate file. Use it in simple low-load scenarios.

The `Log` and `StripeLog` engines support parallel reading of data. When reading data ClickHouse uses multiple threads. Each thread processes separated data block. The `Log` engine uses the separate file for each column of the table. The `StripeLog` stores all the data in one file. Thus the `StripeLog` engine uses fewer descriptors in the operating system, but the `Log` engine provides a more efficient reading of the data.


[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/log_family/) <!--hide-->
