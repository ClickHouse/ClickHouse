# system.tables {#system-tables}

Contains metadata of each table that the server knows about. 

[Detached](../../sql-reference/statements/detach.md) tables are not shown in `system.tables`.

[Temporary tables](../../sql-reference/statements/create/table.md#temporary-tables) are visible in the `system.tables` only in those session where they have been created. They are shown with the empty `database` field and with the `is_temporary` flag switched on. 

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in. 

-   `name` ([String](../../sql-reference/data-types/string.md)) — Table name. 

-   `engine` ([String](../../sql-reference/data-types/string.md)) — Table engine name (without parameters). 

-   `is_temporary` ([UInt8](../../sql-reference/data-types/int-uint.md)) - Flag that indicates whether the table is temporary. 

-   `data_path` ([String](../../sql-reference/data-types/string.md)) - Path to the table data in the file system. 

-   `metadata_path` ([String](../../sql-reference/data-types/string.md)) - Path to the table metadata in the file system. 

-   `metadata_modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) - Time of latest modification of the table metadata.

-   `dependencies_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - Database dependencies.

-   `dependencies_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - Table dependencies ([MaterializedView](../../engines/table-engines/special/materializedview.md) tables based on the current table).

-   `create_table_query` ([String](../../sql-reference/data-types/string.md)) - The query that was used to create the table.

-   `engine_full` ([String](../../sql-reference/data-types/string.md)) - Parameters of the table engine. 

-   `partition_key` ([String](../../sql-reference/data-types/string.md)) - The partition key expression specified in the table. 

-   `sorting_key` ([String](../../sql-reference/data-types/string.md)) - The sorting key expression specified in the table. 

-   `primary_key` ([String](../../sql-reference/data-types/string.md)) - The primary key expression specified in the table. 

-   `sampling_key` ([String](../../sql-reference/data-types/string.md)) - The sampling key expression specified in the table. 

-   `storage_policy` ([String](../../sql-reference/data-types/string.md)) - The storage policy:

    -   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distributed](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - Total number of rows, if it is possible to quickly determine exact number of rows in the table, otherwise `NULL` (including underying `Buffer` table). 

-   `total_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - Total number of bytes, if it is possible to quickly determine exact number of bytes for the table on storage, otherwise `NULL` (does not includes any underlying storage). 

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   If the table stores data in memory, returns approximated number of used bytes in memory.

-   `lifetime_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - Total number of rows INSERTed since server start (only for `Buffer` tables). 

-   `lifetime_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - Total number of bytes INSERTed since server start (only for `Buffer` tables). 

The `system.tables` table is used in `SHOW TABLES` query implementation.

**Example**

```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
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
```

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/tables) <!--hide-->
