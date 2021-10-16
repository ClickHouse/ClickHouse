# system.mutations {#system_tables-mutations}

The table contains information about [mutations](../../sql-reference/statements/alter/index.md#mutations) of [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables and their progress. Each mutation command is represented by a single row. 

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database to which the mutation was applied.

-   `table` ([String](../../sql-reference/data-types/string.md)) — The name of the table to which the mutation was applied.

-   `mutation_id` ([String](../../sql-reference/data-types/string.md)) — The ID of the mutation. For replicated tables these IDs correspond to znode names in the `<table_path_in_zookeeper>/mutations/` directory in ZooKeeper. For non-replicated tables the IDs correspond to file names in the data directory of the table.

-   `command` ([String](../../sql-reference/data-types/string.md)) — The mutation command string (the part of the query after `ALTER TABLE [db.]table`).

-   `create_time` ([Datetime](../../sql-reference/data-types/datetime.md)) —  Date and time when the mutation command was submitted for execution.

-   `block_numbers.partition_id` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — For mutations of replicated tables, the array contains the partitions' IDs (one record for each partition). For mutations of non-replicated tables the array is empty.

-   `block_numbers.number` ([Array](../../sql-reference/data-types/array.md)([Int64](../../sql-reference/data-types/int-uint.md))) — For mutations of replicated tables, the array contains one record for each partition, with the block number that was acquired by the mutation. Only parts that contain blocks with numbers less than this number will be mutated in the partition. 
    
    In non-replicated tables, block numbers in all partitions form a single sequence. This means that for mutations of non-replicated tables, the column will contain one record with a single block number acquired by the mutation.

-   `parts_to_do_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — An array of names of data parts that need to be mutated for the mutation to complete.

-   `parts_to_do` ([Int64](../../sql-reference/data-types/int-uint.md)) — The number of data parts that need to be mutated for the mutation to complete.

-   `is_done` ([UInt8](../../sql-reference/data-types/int-uint.md)) — The flag whether the mutation is done or not. Possible values: 
    -   `1` if the mutation is completed,
    -   `0` if the mutation is still in process. 

!!! info "Note"
    Even if `parts_to_do = 0` it is possible that a mutation of a replicated table is not completed yet because of a long-running `INSERT` query, that will create a new data part needed to be mutated.

If there were problems with mutating some data parts, the following columns contain additional information:

-   `latest_failed_part` ([String](../../sql-reference/data-types/string.md)) — The name of the most recent part that could not be mutated.

-   `latest_fail_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — The date and time of the most recent part mutation failure.

-   `latest_fail_reason` ([String](../../sql-reference/data-types/string.md)) — The exception message that caused the most recent part mutation failure.

**See Also**

-   [Mutations](../../sql-reference/statements/alter/index.md#mutations)
-   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table engine
-   [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) family

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/mutations) <!--hide-->
