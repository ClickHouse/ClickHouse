# system.tables {#system-tables}

Contains metadata of each table that the server knows about. Detached tables are not shown in `system.tables`.

This table contains the following columns (the column type is shown in brackets):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (UInt8) - Flag that indicates whether the table is temporary.

-   `data_path` (String) - Path to the table data in the file system.

-   `metadata_path` (String) - Path to the table metadata in the file system.

-   `metadata_modification_time` (DateTime) - Time of latest modification of the table metadata.

-   `dependencies_database` (Array(String)) - Database dependencies.

-   `dependencies_table` (Array(String)) - Table dependencies ([MaterializedView](../../engines/table-engines/special/materializedview.md) tables based on the current table).

-   `create_table_query` (String) - The query that was used to create the table.

-   `engine_full` (String) - Parameters of the table engine.

-   `partition_key` (String) - The partition key expression specified in the table.

-   `sorting_key` (String) - The sorting key expression specified in the table.

-   `primary_key` (String) - The primary key expression specified in the table.

-   `sampling_key` (String) - The sampling key expression specified in the table.

-   `storage_policy` (String) - The storage policy:

    -   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distributed](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64)) - Total number of rows, if it is possible to quickly determine exact number of rows in the table, otherwise `Null` (including underying `Buffer` table).

-   `total_bytes` (Nullable(UInt64)) - Total number of bytes, if it is possible to quickly determine exact number of bytes for the table on storage, otherwise `Null` (**does not** includes any underlying storage).

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   If the table stores data in memory, returns approximated number of used bytes in memory.

-   `lifetime_rows` (Nullable(UInt64)) - Total number of rows INSERTed since server start (only for `Buffer` tables).

-   `lifetime_bytes` (Nullable(UInt64)) - Total number of bytes INSERTed since server start (only for `Buffer` tables).

The `system.tables` table is used in `SHOW TABLES` query implementation.

```sql
:) SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   system
name:                       aggregate_function_combinators
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     SystemAggregateFunctionCombinators
is_temporary:               0
data_paths:                 []
metadata_path:              /var/lib/clickhouse/metadata/system/aggregate_function_combinators.sql
metadata_modification_time: 1970-01-01 03:00:00
dependencies_database:      []
dependencies_table:         []
create_table_query:         
engine_full:                
partition_key:              
sorting_key:                
primary_key:                
sampling_key:               
storage_policy:             
total_rows:                 ᴺᵁᴸᴸ
total_bytes:                ᴺᵁᴸᴸ

Row 2:
──────
database:                   system
name:                       asynchronous_metrics
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     SystemAsynchronousMetrics
is_temporary:               0
data_paths:                 []
metadata_path:              /var/lib/clickhouse/metadata/system/asynchronous_metrics.sql
metadata_modification_time: 1970-01-01 03:00:00
dependencies_database:      []
dependencies_table:         []
create_table_query:         
engine_full:                
partition_key:              
sorting_key:                
primary_key:                
sampling_key:               
storage_policy:             
total_rows:                 ᴺᵁᴸᴸ
total_bytes:                ᴺᵁᴸᴸ

2 rows in set. Elapsed: 0.004 sec. 
```

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/tables) <!--hide-->
