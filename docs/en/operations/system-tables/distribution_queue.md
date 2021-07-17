# system.distribution_queue {#system_tables-distribution_queue}

Contains information about local files that are in the queue to be sent to the shards. These local files contain new parts that are created by inserting new data into the Distributed table in asynchronous mode.

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database.

-   `table` ([String](../../sql-reference/data-types/string.md)) — Name of the table.

-   `data_path` ([String](../../sql-reference/data-types/string.md)) — Path to the folder with local files.

-   `is_blocked` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag indicates whether sending local files to the server is blocked.

-   `error_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of errors.

-   `data_files` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of local files in a folder.

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Size of compressed data in local files, in bytes.

-   `broken_data_files` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of files that has been marked as broken (due to an error).

-   `broken_data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Size of compressed data in broken files, in bytes.

-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — Text message about the last error that occurred (if any).

**Example**

``` sql
SELECT * FROM system.distribution_queue LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:              default
table:                 dist
data_path:             ./store/268/268bc070-3aad-4b1a-9cf2-4987580161af/default@127%2E0%2E0%2E2:9000/
is_blocked:            1
error_count:           0
data_files:            1
data_compressed_bytes: 499
last_exception:        
```

**See Also**

-   [Distributed table engine](../../engines/table-engines/special/distributed.md)

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/distribution_queue) <!--hide-->
