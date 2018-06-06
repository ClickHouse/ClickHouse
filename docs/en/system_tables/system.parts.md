# system.parts

Contains information about parts of a table in the [MergeTree](../table_engines/mergetree.md#table_engines-mergetree) family.

Each row describes one part of the data.

Columns:

- partition (String) – The partition name. YYYYMM format. To learn what a partition is, see the description of the [ALTER](../query_language/queries.md#query_language_queries_alter) query.
- name (String) – Name of the data part.
- active (UInt8) – Indicates whether the part is active. If a part is active, it is used in a table; otherwise, it will be deleted. Inactive data parts remain after merging.
- marks (UInt64) – The number of marks. To get the approximate number of rows in a data part, multiply ``marks``  by the index granularity (usually 8192).
- marks_size (UInt64) – The size of the file with marks.
- rows (UInt64) – The number of rows.
- bytes (UInt64) – The number of bytes when compressed.
- modification_time (DateTime) – The modification time of the directory with the data part. This usually corresponds to the time of data part creation.|
- remove_time (DateTime) – The time when the data part became inactive.
- refcount (UInt32) – The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.
- min_date (Date) – The minimum value of the date key in the data part.
- max_date (Date) – The maximum value of the date key in the data part.
- min_block_number (UInt64) – The minimum number of data parts that make up the current part after merging.
- max_block_number (UInt64) – The maximum number of data parts that make up the current part after merging.
- level (UInt32) – Depth of the merge tree. If a merge was not performed, ``level=0``.
- primary_key_bytes_in_memory (UInt64) – The amount of memory (in bytes) used by primary key values.
- primary_key_bytes_in_memory_allocated (UInt64) – The amount of memory (in bytes) reserved for primary key values.
- database (String) – Name of the database.
- table (String) – Name of the table.
- engine (String) – Name of the table engine without parameters.

