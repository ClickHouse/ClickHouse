# system.merges

Contains information about merges currently in process for tables in the MergeTree family.

Columns:

- `database String` — Name of the database the table is located in.
- `table String` — Name of the table.
- `elapsed Float64` — Time in seconds since the merge started.
- `progress Float64` — Percent of progress made, from 0 to 1.
- `num_parts UInt64` — Number of parts to merge.
- `result_part_name String` — Name of the part that will be formed as the result of the merge.
- `total_size_bytes_compressed UInt64` — Total size of compressed data in the parts being merged.
- `total_size_marks UInt64` — Total number of marks in the parts being merged.
- `bytes_read_uncompressed UInt64` — Amount of bytes read, decompressed.
- `rows_read UInt64` — Number of rows read.
- `bytes_written_uncompressed UInt64` — Amount of bytes written, uncompressed.
- `rows_written UInt64` — Number of rows written.
