---
toc_priority: 41
toc_title: CHECK
---

# CHECK TABLE Statement {#check-table}

Checks if the data in the table is corrupted.

``` sql
CHECK TABLE [db.]name
```

The `CHECK TABLE` query compares actual file sizes with the expected values which are stored on the server. If the file sizes do not match the stored values, it means the data is corrupted. This can be caused, for example, by a system crash during query execution.

The query response contains the `result` column with a single row. The row has a value of
[Boolean](../../sql-reference/data-types/boolean.md) type:

-   0 - The data in the table is corrupted.
-   1 - The data maintains integrity.

The `CHECK TABLE` query supports the following table engines:

-   [Log](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)

Performed over the tables with another table engines causes an exception.

Engines from the `*Log` family don’t provide automatic data recovery on failure. Use the `CHECK TABLE` query to track data loss in a timely manner.

For `MergeTree` family engines, the `CHECK TABLE` query shows a check status for every individual data part of a table on the local server.

**If the data is corrupted**

If the table is corrupted, you can copy the non-corrupted data to another table. To do this:

1.  Create a new table with the same structure as damaged table. To do this execute the query `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Set the [max_threads](../../operations/settings/settings.md#settings-max_threads) value to 1 to process the next query in a single thread. To do this run the query `SET max_threads = 1`.
3.  Execute the query `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. This request copies the non-corrupted data from the damaged table to another table. Only the data before the corrupted part will be copied.
4.  Restart the `clickhouse-client` to reset the `max_threads` value.
