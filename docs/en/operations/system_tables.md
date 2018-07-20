# System tables

System tables are used for implementing part of the system's functionality, and for providing access to information about how the system is working.
You can't delete a system table (but you can perform DETACH).
System tables don't have files with data on the disk or files with metadata. The server creates all the system tables when it starts.
System tables are read-only.
They are located in the 'system' database.

<a name="system_tables-system.asynchronous_metrics"></a>

## system.asynchronous_metrics

Contain metrics used for profiling and monitoring.
They usually reflect the number of events currently in the system, or the total resources consumed by the system.
Example: The number of SELECT queries currently running; the amount of memory in use.`system.asynchronous_metrics`and`system.metrics` differ in their sets of metrics and how they are calculated.

## system.clusters

Contains information about clusters available in the config file and the servers in them.
Columns:

```text
cluster String      – Cluster name.
shard_num UInt32    – Number of a shard in the cluster, starting from 1.
shard_weight UInt32 – Relative weight of a shard when writing data.
replica_num UInt32  – Number of a replica in the shard, starting from 1.
host_name String    – Host name as specified in the config.
host_address String – Host's IP address obtained from DNS.
port UInt16         – The port used to access the server.
user String         – The username to use for connecting to the server.
```
## system.columns

Contains information about the columns in all tables.
You can use this table to get information similar to `DESCRIBE TABLE`, but for multiple tables at once.

```text
database String           - Name of the database the table is located in.
table String              - Table name.
name String               - Column name.
type String               - Column type.
default_type String       - Expression type (DEFAULT, MATERIALIZED, ALIAS) for the default value, or an empty string if it is not defined.
default_expression String - Expression for the default value, or an empty string if it is not defined.
```

## system.databases

This table contains a single String column called 'name' – the name of a database.
Each database that the server knows about has a corresponding entry in the table.
This system table is used for implementing the `SHOW DATABASES` query.

## system.dictionaries

Contains information about external dictionaries.

Columns:

- `name String` – Dictionary name.
- `type String` – Dictionary type: Flat, Hashed, Cache.
- `origin String` – Path to the config file where the dictionary is described.
- `attribute.names Array(String)` – Array of attribute names provided by the dictionary.
- `attribute.types Array(String)` – Corresponding array of attribute types provided by the dictionary.
- `has_hierarchy UInt8` – Whether the dictionary is hierarchical.
- `bytes_allocated UInt64` – The amount of RAM used by the dictionary.
- `hit_rate Float64` – For cache dictionaries, the percent of usage for which the value  was in the cache.
- `element_count UInt64` – The number of items stored in the dictionary.
- `load_factor Float64` – The filled percentage of the dictionary (for a hashed dictionary, it is the filled percentage of the hash table).
- `creation_time DateTime` – Time spent for the creation or last successful reload of the dictionary.
- `last_exception String` – Text of an error that occurred when creating or reloading the dictionary, if the dictionary couldn't be created.
- `source String` – Text describing the data source for the dictionary.

Note that the amount of memory used by the dictionary is not proportional to the number of items stored in it. So for flat and cached dictionaries, all the memory cells are pre-assigned, regardless of how full the dictionary actually is.
<a name="system_tables-system.events"></a>

## system.events

Contains information about the number of events that have occurred in the system. This is used for profiling and monitoring purposes.
Example: The number of processed SELECT queries.
Columns: 'event String' – the event name, and 'value UInt64' – the quantity.

## system.functions

Contains information about normal and aggregate functions.

Columns:

- `name` (`String`) – Function name.
- `is_aggregate` (`UInt8`) – Whether it is an aggregate function.
## system.merges

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
<a name="system_tables-system.metrics"></a>

## system.metrics

## system.numbers

This table contains a single UInt64 column named 'number' that contains almost all the natural numbers starting from zero.
You can use this table for tests, or if you need to do a brute force search.
Reads from this table are not parallelized.

## system.numbers_mt

The same as 'system.numbers' but reads are parallelized. The numbers can be returned in any order.
Used for tests.

## system.one

This table contains a single row with a single 'dummy' UInt8 column containing the value 0.
This table is used if a SELECT query doesn't specify the FROM clause.
This is similar to the DUAL table found in other DBMSs.

## system.parts

Contains information about parts of a table in the [MergeTree](../operations/table_engines/mergetree.md#table_engines-mergetree) family.

Each row describes one part of the data.

Columns:

- partition (String) – The partition name. It's in YYYYMM format in case of old-style partitioning and is arbitary serialized value in case of custom partitioning. To learn what a partition is, see the description of the [ALTER](../query_language/alter.md#query_language_queries_alter) query.
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

## system.processes

This system table is used for implementing the `SHOW PROCESSLIST` query.
Columns:

```text
user String              – Name of the user who made the request. For distributed query processing, this is the user who helped the requestor server send the query to this server, not the user who made the distributed request on the requestor server.

address String           – The IP address that the query was made from. The same is true for distributed query processing.

elapsed Float64          –  The time in seconds since request execution started.

rows_read UInt64         – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.

bytes_read UInt64        – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.

UInt64 total_rows_approx – The approximate total number of rows that must be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.

memory_usage UInt64 – Memory consumption by the query. It might not include some types of dedicated memory.

query String – The query text. For INSERT, it doesn't include the data to insert.

query_id – Query ID, if defined.
```

## system.replicas

Contains information and status for replicated tables residing on the local server.
This table can be used for monitoring. The table contains a row for every Replicated\* table.

Example:

```sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

```text
Row 1:
──────
database:           merge
table:              visits
engine:             ReplicatedCollapsingMergeTree
is_leader:          1
is_readonly:        0
is_session_expired: 0
future_parts:       1
parts_to_check:     0
zookeeper_path:     /clickhouse/tables/01-06/visits
replica_name:       example01-06-1.yandex.ru
replica_path:       /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:    9
queue_size:         1
inserts_in_queue:   0
merges_in_queue:    1
log_max_index:      596273
log_pointer:        596274
total_replicas:     2
active_replicas:    2
```

Columns:

```text
database:           database name
table:              table name
engine:             table engine name

is_leader:          whether the replica is the leader

Only one replica at a time can be the leader. The leader is responsible for selecting background merges to perform.
Note that writes can be performed to any replica that is available and has a session in ZK, regardless of whether it is a leader.

is_readonly:        Whether the replica is in read-only mode.
This mode is turned on if the config doesn't have sections with ZK, if an unknown error occurred when reinitializing sessions in ZK, and during session reinitialization in ZK.

is_session_expired: Whether the ZK session expired.
Basically, the same thing as is_readonly.

future_parts: The number of data parts that will appear as the result of INSERTs or merges that haven't been done yet. 

parts_to_check: The number of data parts in the queue for verification.
A part is put in the verification queue if there is suspicion that it might be damaged.

zookeeper_path: The path to the table data in ZK. 
replica_name: Name of the replica in ZK. Different replicas of the same table have different names. 
replica_path: The path to the replica data in ZK. The same as concatenating zookeeper_path/replicas/replica_path.

columns_version: Version number of the table structure.
Indicates how many times ALTER was performed. If replicas have different versions, it means some replicas haven't made all of the ALTERs yet.

queue_size:         Size of the queue for operations waiting to be performed.
Operations include inserting blocks of data, merges, and certain other actions.
Normally coincides with future_parts.

inserts_in_queue: Number of inserts of blocks of data that need to be made.
Insertions are usually replicated fairly quickly. If the number is high, something is wrong.

merges_in_queue: The number of merges waiting to be made. 
Sometimes merges are lengthy, so this value may be greater than zero for a long time.

The next 4 columns have a non-null value only if the ZK session is active.

log_max_index:     Maximum entry number in the log of general activity.
log_pointer:        Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one.
If log_pointer is much smaller than log_max_index, something is wrong.

total_replicas:     Total number of known replicas of this table.
active_replicas:    Number of replicas of this table that have a ZK session (the number of active replicas).
```

If you request all the columns, the table may work a bit slowly, since several reads from ZK are made for each row.
If you don't request the last 4 columns (log_max_index, log_pointer, total_replicas, active_replicas), the table works quickly.

For example, you can check that everything is working correctly like this:

```sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

If this query doesn't return anything, it means that everything is fine.

## system.settings

Contains information about settings that are currently in use.
I.e. used for executing the query you are using to read from the system.settings table).

Columns:

```text
name String   – Setting name.
value String  – Setting value.
changed UInt8 - Whether the setting was explicitly defined in the config or explicitly changed.
```

Example:

```sql
SELECT *
FROM system.settings
WHERE changed
```

```text
┌─name───────────────────┬─value───────┬─changed─┐
│ max_threads            │ 8           │       1 │
│ use_uncompressed_cache │ 0           │       1 │
│ load_balancing         │ random      │       1 │
│ max_memory_usage       │ 10000000000 │       1 │
└────────────────────────┴─────────────┴─────────┘
```

## system.tables

This table contains the String columns 'database', 'name', and 'engine'.
The table also contains three virtual columns: metadata_modification_time (DateTime type), create_table_query, and engine_full (String type).
Each table that the server knows about is entered in the 'system.tables' table.
This system table is used for implementing SHOW TABLES queries.

## system.zookeeper

This table presents when ZooKeeper is configured. It allows reading data from the ZooKeeper cluster defined in the config.
The query must have a 'path' equality condition in the WHERE clause. This is the path in ZooKeeper for the children that you want to get data for.

The query `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` outputs data for all children on the `/clickhouse` node.
To output data for all root nodes, write path = '/'.
If the path specified in 'path' doesn't exist, an exception will be thrown.

Columns:

- `name String` — Name of the node.
- `path String` — Path to the node.
- `value String` — Value of the node.
- `dataLength Int32` — Size of the value.
- `numChildren Int32` — Number of children.
- `czxid Int64` — ID of the transaction that created the node.
- `mzxid Int64` — ID of the transaction that last changed the node.
- `pzxid Int64` — ID of the transaction that last added or removed children.
- `ctime DateTime` — Time of node creation.
- `mtime DateTime` — Time of the last node modification.
- `version Int32` — Node version - the number of times the node was changed.
- `cversion Int32` — Number of added or removed children.
- `aversion Int32` — Number of changes to ACL.
- `ephemeralOwner Int64` — For ephemeral nodes, the ID of the session that owns this node.


Example:

```sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

```text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```
