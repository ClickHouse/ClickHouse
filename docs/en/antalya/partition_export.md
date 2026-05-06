# ALTER TABLE EXPORT PARTITION

## Overview

The `ALTER TABLE EXPORT PARTITION` command exports entire partitions from Replicated*MergeTree tables to object storage (S3, Azure Blob Storage, etc.) or data lakes like Apache Iceberg tables (with and without catalogs), typically in Parquet format. This feature coordinates export part operations across all replicas using ZooKeeper.

The set of parts that are exported is based on the list of parts the replica that received the export command sees. The other replicas will assist in the export process if they have those parts locally. Otherwise they will ignore it.

The partition export tasks can be observed through `system.replicated_partition_exports`. Querying this table results in a query to ZooKeeper, so it must be used with care. Individual part export progress can be observed as usual through `system.exports`.

The same partition can not be exported to the same destination more than once. There are two ways to override this behavior: either by setting the `export_merge_tree_partition_force_export` setting or waiting for the task to expire.

The export task can be killed by issuing the kill command: `KILL EXPORT PARTITION <where predicate for system.replicated_partition_exports>`.

The task is persistent - it should be resumed after crashes, failures and etc.

### On Apache Iceberg storage exports:

Each MergeTree part will become a separate file (or more depending on `max_bytes` and `max_rows` settings) following the engine naming convention. Once all parts have been exported, new snapshots / manifest files are generated and the data is comitted using the Apache Iceberg commit mechanism.

The manifest file produced by the commit contains a summary field `clickhouse.export-partition-transaction-id` that stores the transaction id. This field is used to implement idempotency and avoid data duplication. Some Apache Iceberg storage managers employ old manifests cleanup, ClickHouse does not.

**IMPORTANT**: In case the storage is managed by a 3rd party application that cleans up old manifest files, it is important that the TTL of such files are greater than the timeout of export partition tasks. If it is not configured in such a way, it is possible to accidentally duplicate data in the extremely rare case a ClickHouse node is the only node working on a given export task, commits the data to Iceberg, crashes before marking the task as done and only boots up after the manifest cleanup has deleted the commit manifest. In such scenario, ClickHouse would attempt to commit those files again producing duplicates. The task timeout on ClickHouse side is controlled by the setting `export_merge_tree_partition_task_timeout_seconds`.

The Iceberg manifest files contain statistics about the data. Exporting a merge tree partition is a non ephemeral long running task, in which nodes can be turned off and turned on. This means the stats of individual files need to be persisted somewhere in order to produce the final manifest. This is implemented through sidecars. Each data file exported will contain a "sibling" sidecar file named `<data_file_name>_clickhouse_export_part_sidecar.avro`. ClickHouse does not clean up these files, and they can be safely deleted once the data is comitted.

### On plain object storage exports:

Each MergeTree part will become a separate file with the following name convention: `<table_directory>/<partitioning>/<data_part_name>_<merge_tree_part_checksum>.<format>`. To ensure atomicity, a commit file containing the relative paths of all exported parts is also shipped. A data file should only be considered part of the dataset if a commit file references it. The commit file will be named using the following convention: `<table_directory>/commit_<partition_id>_<transaction_id>`.

## Syntax

```sql
ALTER TABLE [database.]table_name 
EXPORT PARTITION ID 'partition_id' 
TO TABLE [destination_database.]destination_table 
[SETTINGS setting_name = value, ...]
```

### Parameters

- **`table_name`**: The source Replicated*MergeTree table containing the partition to export
- **`partition_id`**: The partition identifier to export (e.g., `'2020'`, `'2021'`)
- **`destination_table`**: The target table for the export (typically an S3, Azure, or other object storage table)

## Settings

### Server Settings

#### `allow_experimental_export_merge_tree_partition` (Required)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: Enable export replicated merge tree partition feature. It is experimental and not yet ready for production use.

### Query Settings

#### `export_merge_tree_partition_force_export` (Optional)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: Ignore existing partition export and overwrite the ZooKeeper entry. Allows re-exporting a partition to the same destination before the manifest expires. **IMPORTANT:** this is dangerous because it can lead to duplicated data, use it with caution.

#### `export_merge_tree_partition_max_retries` (Optional)

- **Type**: `UInt64`
- **Default**: `3`
- **Description**: Maximum number of retries for exporting a merge tree part in an export partition task. If it exceeds, the entire task fails.

#### `export_merge_tree_partition_manifest_ttl` (Optional)

- **Type**: `UInt64`
- **Default**: `180` (seconds)
- **Description**: Determines how long the manifest will live in ZooKeeper. It prevents the same partition from being exported twice to the same destination. This setting does not affect or delete in-progress tasks; it only cleans up completed ones.

#### `export_merge_tree_part_file_already_exists_policy` (Optional)

- **Type**: `MergeTreePartExportFileAlreadyExistsPolicy`
- **Default**: `skip`
- **Description**: Policy for handling files that already exist during export. Possible values:
  - `skip` - Skip the file if it already exists
  - `error` - Throw an error if the file already exists
  - `overwrite` - Overwrite the file

### `export_merge_tree_part_throw_on_pending_mutations` (Optional)

- **Type**: `bool`
- **Default**: `true`
- **Description**: If set to true, throws if pending mutations exists for a given part. Note that by default mutations are applied to all parts, which means that if a mutation in practice would only affetct part/partition x, all the other parts/partition will throw upon export. The exception is when the `IN PARTITION` clause was used in the mutation command. Note the `IN PARTITION` clause is not properly implemented for plain MergeTree tables.

### `export_merge_tree_part_throw_on_pending_patch_parts` (Optional)

- **Type**: `bool`
- **Default**: `true`
- **Description**: If set to true, throws if pending patch parts exists for a given part. Note that by default mutations are applied to all parts, which means that if a mutation in practice would only affetct part/partition x, all the other parts/partition will throw upon export. The exception is when the `IN PARTITION` clause was used in the mutation command. Note the `IN PARTITION` clause is not properly implemented for plain MergeTree tables.

### `export_merge_tree_part_filename_pattern` (Optional)

- **Type**: `String`
- **Default**: `{part_name}_{checksum}`
- **Description**: Pattern for the filename of the exported merge tree part. The `part_name` and `checksum` are calculated and replaced on the fly. Additional macros are supported.

### `export_merge_tree_partition_task_timeout_seconds` (Optional)

- **Type**: `UInt64`
- **Default**: `3600`
- **Description**: The timeout is measured from the manifest's create_time. Set to 0 to disable the timeout.
When the timeout is exceeded the task transitions to KILLED (same terminal state as `KILL QUERY ... EXPORT PARTITION`), and `last_exception` is populated with a timeout reason.

Notes:
- Enforcement is best-effort: actual kill latency is bounded by one manifest-updater poll cycle (~30s) plus ZooKeeper watch propagation.
- Since both this timeout and `export_merge_tree_partition_manifest_ttl` are measured from `create_time`, keep `export_merge_tree_partition_manifest_ttl` greater than `export_merge_tree_partition_task_timeout_seconds` if you want the KILLED entry to remain visible in `system.replicated_partition_exports` after the timeout fires.

## Examples

### Basic Export to S3

```sql
CREATE TABLE rmt_table (id UInt64, year UInt16) 
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt_table', 'replica1') 
PARTITION BY year ORDER BY tuple();

CREATE TABLE s3_table (id UInt64, year UInt16) 
ENGINE = S3(s3_conn, filename='data', format=Parquet, partition_strategy='hive') 
PARTITION BY year;

INSERT INTO rmt_table VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021);

ALTER TABLE rmt_table EXPORT PARTITION ID '2020' TO TABLE s3_table;

## Killing Exports

You can cancel in-progress partition exports using the `KILL EXPORT PARTITION` command:

```sql
KILL EXPORT PARTITION 
WHERE partition_id = '2020' 
  AND source_table = 'rmt_table' 
  AND destination_table = 's3_table'
```

The `WHERE` clause filters exports from the `system.replicated_partition_exports` table. You can use any columns from that table in the filter.

## Monitoring

### Active and Completed Exports

Monitor partition exports using the `system.replicated_partition_exports` table:

```sql
arthur :) select * from system.replicated_partition_exports Format Vertical;

SELECT *
FROM system.replicated_partition_exports
FORMAT Vertical

Query id: 9efc271a-a501-44d1-834f-bc4d20156164

Row 1:
──────
source_database:      default
source_table:         replicated_source
destination_database: default
destination_table:    replicated_destination
create_time:          2025-11-21 18:21:51
partition_id:         2022
transaction_id:       7397746091717128192
source_replica:       r1
parts:                ['2022_0_0_0','2022_1_1_0','2022_2_2_0']
parts_count:          3
parts_to_do:          0
status:               COMPLETED
exception_replica:    
last_exception:       
exception_part:       
exception_count:      0

Row 2:
──────
source_database:      default
source_table:         replicated_source
destination_database: default
destination_table:    replicated_destination
create_time:          2025-11-21 18:20:35
partition_id:         2021
transaction_id:       7397745772618674176
source_replica:       r1
parts:                ['2021_0_0_0']
parts_count:          1
parts_to_do:          0
status:               COMPLETED
exception_replica:    
last_exception:       
exception_part:       
exception_count:      0

2 rows in set. Elapsed: 0.019 sec. 

arthur :) 
```

Status values include:
- `PENDING` - Export is queued / in progress
- `COMPLETED` - Export finished successfully
- `FAILED` - Export failed
- `KILLED` - Export was cancelled

## Related Features

- [ALTER TABLE EXPORT PART](/docs/en/engines/table-engines/mergetree-family/part_export.md) - Export individual parts (non-replicated)

