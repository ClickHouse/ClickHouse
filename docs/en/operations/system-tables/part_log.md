# part_log

The `system.part_log` table is created only if the [part_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-part-log) server setting is specified.

This table contains information about events that occurred with [data parts](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family tables, such as adding or merging data.

The `system.part_log` table contains the following columns:

-   `query_id` ([String](../../sql-reference/data-types/string.md)) — Identifier of the `INSERT` query that created this data part.
-   `event_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the event that occurred with the data part. Can have one of the following values:
    -   `NEW_PART` — Inserting of a new data part.
    -   `MERGE_PARTS` — Merging of data parts.
    -   `DOWNLOAD_PART` — Downloading a data part.
    -   `REMOVE_PART` — Removing or detaching a data part using [DETACH PARTITION](../../sql-reference/statements/alter/partition.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutating of a data part.
    -   `MOVE_PART` — Moving the data part from the one disk to another one.
-   `merge_reason` ([Enum8](../../sql-reference/data-types/enum.md)) — The reason for the event with type `MERGE_PARTS`. Can have one of the following values:
    -   `NOT_A_MERGE` — The current event has the type other than `MERGE_PARTS`.
    -   `REGULAR_MERGE` — Some regular merge.
    -   `TTL_DELETE_MERGE` — Cleaning up expired data.
    -   `TTL_RECOMPRESS_MERGE` — Recompressing data part with the.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds precision.
-   `duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Duration.
-   `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database the data part is in.
-   `table` ([String](../../sql-reference/data-types/string.md)) — Name of the table the data part is in.
-   `part_name` ([String](../../sql-reference/data-types/string.md)) — Name of the data part.
-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — ID of the partition that the data part was inserted to. The column takes the `all` value if the partitioning is by `tuple()`.
-   `path_on_disk` ([String](../../sql-reference/data-types/string.md)) — Absolute path to the folder with data part files.
-   `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of rows in the data part.
-   `size_in_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Size of the data part in bytes.
-   `merged_from` ([Array(String)](../../sql-reference/data-types/array.md)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Size of uncompressed bytes.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of rows was read during the merge.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of bytes was read during the merge.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `error` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The code number of the occurred error.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — Text message of the occurred error.

The `system.part_log` table is created after the first inserting data to the `MergeTree` table.

**Example**

``` sql
SELECT * FROM system.part_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
query_id:                      983ad9c7-28d5-4ae1-844e-603116b7de31
event_type:                    NewPart
merge_reason:                  NotAMerge
event_date:                    2021-02-02
event_time:                    2021-02-02 11:14:28
event_time_microseconds:                    2021-02-02 11:14:28.861919
duration_ms:                   35
database:                      default
table:                         log_mt_2
part_name:                     all_1_1_0
partition_id:                  all
path_on_disk:                  db/data/default/log_mt_2/all_1_1_0/
rows:                          115418
size_in_bytes:                 1074311
merged_from:                   []
bytes_uncompressed:            0
read_rows:                     0
read_bytes:                    0
peak_memory_usage:             0
error:                         0
exception:
```

[Original article](https://clickhouse.com/docs/en/operations/system-tables/part_log) <!--hide-->
