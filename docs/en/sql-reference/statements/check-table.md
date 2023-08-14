---
slug: /en/sql-reference/statements/check-table
sidebar_position: 41
sidebar_label: CHECK TABLE
title: "CHECK TABLE Statement"
---

The `CHECK TABLE` query in ClickHouse is used to perform a validation check on a specific table or its partitions. It ensures the integrity of the data by verifying the checksums and other internal data structures.

Particularly it compares actual file sizes with the expected values which are stored on the server. If the file sizes do not match the stored values, it means the data is corrupted. This can be caused, for example, by a system crash during query execution.

:::note
The `CHECK TABLE`` query may read all the data in the table and hold some resources, making it resource-intensive.
Consider the potential impact on performance and resource utilization before executing this query.
:::

## Syntax

The basic syntax of the query is as follows:

```sql
CHECK TABLE table_name [PARTITION partition_expression] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]  ]
```

- `table_name`: Specifies the name of the table that you want to check.
- `partition_expression`: (Optional) If you want to check a specific partition of the table, you can use this expression to specify the partition.
- `FORMAT format`: (Optional) Allows you to specify the output format of the result.
- `SETTINGS`: (Optional) Allows additional settings.
	- **`check_query_single_value_result`**: (Optional) This setting allows you to toggle between a detailed result (`0`) or a summarized result (`1`).
	- Other settings (e.g. `max_threads` can be applied as well).


The query response depends on the value of contains `check_query_single_value_result` setting.
In case of `check_query_single_value_result = 1` only `result` column with a single row is returned. Value inside this row is `1` if the integrity check is passed and `0` if data is corrupted.

With `check_query_single_value_result = 0` the query returns the following columns:
    - `part_path`: Indicates the path to the data part or file name.
    - `is_passed`: Returns 1 if the check for this part is successful, 0 otherwise.
    - `message`: Any additional messages related to the check, such as errors or success messages.

The `CHECK TABLE` query supports the following table engines:

- [Log](../../engines/table-engines/log-family/log.md)
- [TinyLog](../../engines/table-engines/log-family/tinylog.md)
- [StripeLog](../../engines/table-engines/log-family/stripelog.md)
- [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)

Performed over the tables with another table engines causes an `NOT_IMPLEMETED` exception.

Engines from the `*Log` family do not provide automatic data recovery on failure. Use the `CHECK TABLE` query to track data loss in a timely manner.

## Examples

By default `CHECK TABLE` query shows the general table check status:

```sql
CHECK TABLE test_table;
```

```text
┌─result─┐
│      1 │
└────────┘
```

If you want to see the check status for every individual data part you may use `check_query_single_value_result` setting.

Also, to check a specific partition of the table, you can use the `PARTITION` keyword.

```sql
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

Output:

```text
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

### Receiving a 'Corrupted' Result

:::warning
Disclaimer: The procedure described here, including the manual manipulating or removing files directly from the data directory, is for experimental or development environments only. Do **not** attempt this on a production server, as it may lead to data loss or other unintended consequences.
:::

Remove the existing checksum file:

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0


Output:

```text
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

If the checksums.txt file is missing, it can be restored. It will be recalculated and rewritten during the execution of the CHECK TABLE command for the specific partition, and the status will still be reported as 'success.'"


## If the Data Is Corrupted

If the table is corrupted, you can copy the non-corrupted data to another table. To do this:

1.  Create a new table with the same structure as damaged table. To do this execute the query `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Set the `max_threads` value to 1 to process the next query in a single thread. To do this run the query `SET max_threads = 1`.
3.  Execute the query `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. This request copies the non-corrupted data from the damaged table to another table. Only the data before the corrupted part will be copied.
4.  Restart the `clickhouse-client` to reset the `max_threads` value.
