# ALTER TABLE EXPORT PARTITION

## Overview

The `ALTER TABLE EXPORT PARTITION` command exports entire partitions from Replicated*MergeTree tables to object storage (S3, Azure Blob Storage, etc.), typically in Parquet format. This feature coordinates export part operations across all replicas using ZooKeeper.

Each MergeTree part will become a separate file with the following name convention: `<table_directory>/<partitioning>/<data_part_name>_<merge_tree_part_checksum>.<format>`. To ensure atomicity, a commit file containing the relative paths of all exported parts is also shipped. A data file should only be considered part of the dataset if a commit file references it. The commit file will be named using the following convention: `<table_directory>/commit_<partition_id>_<transaction_id>`.

The set of parts that are exported is based on the list of parts the replica that received the export command sees. The other replicas will assist in the export process if they have those parts locally. Otherwise they will ignore it.

The partition export tasks can be observed through `system.replicated_partition_exports`. Querying this table results in a query to ZooKeeper, so it must be used with care. Individual part export progress can be observed as usual through `system.exports`.

The same partition can not be exported to the same destination more than once. There are two ways to override this behavior: either by setting the `export_merge_tree_partition_force_export` setting or waiting for the task to expire.

The export task can be killed by issuing the kill command: `KILL EXPORT PARTITION <where predicate for system.replicated_partition_exports>`.

The task is persistent - it should be resumed after crashes, failures and etc.

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

#### `enable_experimental_export_merge_tree_partition_feature` (Required)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: Enable export replicated merge tree partition feature. It is experimental and not yet ready for production use.

### Query Settings

#### `export_merge_tree_partition_force_export` (Optional)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: Ignore existing partition export and overwrite the ZooKeeper entry. Allows re-exporting a partition to the same destination before the manifest expires.

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

### export_merge_tree_part_throw_on_pending_mutations

- **Type**: `bool`
- **Default**: `true`
- **Description**: If set to true, throws if pending mutations exists for a given part. Note that by default mutations are applied to all parts, which means that if a mutation in practice would only affetct part/partition x, all the other parts/partition will throw upon export. The exception is when the `IN PARTITION` clause was used in the mutation command. Note the `IN PARTITION` clause is not properly implemented for plain MergeTree tables.

### export_merge_tree_part_throw_on_pending_patch_parts

- **Type**: `bool`
- **Default**: `true`
- **Description**: If set to true, throws if pending patch parts exists for a given part. Note that by default mutations are applied to all parts, which means that if a mutation in practice would only affetct part/partition x, all the other parts/partition will throw upon export. The exception is when the `IN PARTITION` clause was used in the mutation command. Note the `IN PARTITION` clause is not properly implemented for plain MergeTree tables.

### export_merge_tree_part_filename_pattern

- **Type**: `String`
- **Default**: `{part_name}_{checksum}`
- **Description**: Pattern for the filename of the exported merge tree part. The `part_name` and `checksum` are calculated and replaced on the fly. Additional macros are supported.

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

