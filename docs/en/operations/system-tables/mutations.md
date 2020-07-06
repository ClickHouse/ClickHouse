# system.mutations {#system_tables-mutations}

The table contains information about [mutations](../../sql-reference/statements/alter.md#alter-mutations) of MergeTree tables and their progress. Each mutation command is represented by a single row. The table has the following columns:

**database**, **table** - The name of the database and table to which the mutation was applied.

**mutation\_id** - The ID of the mutation. For replicated tables these IDs correspond to znode names in the `<table_path_in_zookeeper>/mutations/` directory in ZooKeeper. For unreplicated tables the IDs correspond to file names in the data directory of the table.

**command** - The mutation command string (the part of the query after `ALTER TABLE [db.]table`).

**create\_time** - When this mutation command was submitted for execution.

**block\_numbers.partition\_id**, **block\_numbers.number** - A nested column. For mutations of replicated tables, it contains one record for each partition: the partition ID and the block number that was acquired by the mutation (in each partition, only parts that contain blocks with numbers less than the block number acquired by the mutation in that partition will be mutated). In non-replicated tables, block numbers in all partitions form a single sequence. This means that for mutations of non-replicated tables, the column will contain one record with a single block number acquired by the mutation.

**parts\_to\_do** - The number of data parts that need to be mutated for the mutation to finish.

**is\_done** - Is the mutation done? Note that even if `parts_to_do = 0` it is possible that a mutation of a replicated table is not done yet because of a long-running INSERT that will create a new data part that will need to be mutated.

If there were problems with mutating some parts, the following columns contain additional information:

**latest\_failed\_part** - The name of the most recent part that could not be mutated.

**latest\_fail\_time** - The time of the most recent part mutation failure.

**latest\_fail\_reason** - The exception message that caused the most recent part mutation failure.
