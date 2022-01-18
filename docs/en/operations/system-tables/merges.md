# system.merges {#system-merges}

Contains information about merges and part mutations currently in process for tables in the MergeTree family.

Columns:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (UInt8) — 1 if this process is a part mutation.
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.
-   `memory_usage` (UInt64) — Memory consumption of the merge process.
-   `thread_id` (UInt64) — Thread ID of the merge process.
-   `merge_type` — The type of current merge. Empty if it's an mutation.
-   `merge_algorithm` — The algorithm used in current merge. Empty if it's an mutation.

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/merges) <!--hide-->
