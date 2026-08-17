# ALTER TABLE EXPORT PARTITION

## Overview

The `ALTER TABLE EXPORT PARTITION` command exports entire partitions from Replicated*MergeTree tables to object storage (S3, Azure Blob Storage, etc.) or data lakes like Apache Iceberg tables (with and without catalogs), typically in Parquet format. This feature coordinates export part operations across all replicas using ZooKeeper.

The set of parts that are exported is based on the list of parts the replica that received the export command sees. The other replicas will assist in the export process if they have those parts locally. Otherwise they will ignore it.

The partition export tasks can be observed through `system.replicated_partition_exports`. The table is served from each replica's in-memory mirror, so queries do not contact ZooKeeper and are cheap to run. The mirror is refreshed on the manifest-updater poll cycle and on every status change, so a freshly written exception or terminal state may take up to one poll interval to appear. Individual part export progress can be observed as usual through `system.exports`.

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

## Requirements

`EXPORT PARTITION` exports each part via the same mechanism as [`EXPORT PART`](/docs/en/antalya/part_export.md#requirements), so the source and destination tables must satisfy the same compatibility requirements. Column names may differ (columns are matched by position, not by name), and column types may differ as long as they are safely castable (or `export_merge_tree_part_allow_lossy_cast = 1` is set). Beyond that, the following must match:

1. **Column count** - source and destination must have the same number of columns.
2. **`PARTITION BY` expressions** - for destinations other than data lakes, the source and destination `PARTITION BY` expressions must be identical. For Apache Iceberg destinations, the source partition key must match the destination partition fields and transforms.
3. **Partition key column positions and layouts** - every top-level column that provides a column or subcolumn used by the source table's partition key must have the same name at the same position in the destination table's schema. Named `Tuple` elements within such a column must also be declared in the same order, including tuples nested inside `Array` or `Map`. This applies even if both tables' `PARTITION BY` expressions are textually identical. See [`EXPORT PART` requirements](/docs/en/antalya/part_export.md#requirements) for a worked example and the corresponding exception message.

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
- **Description**: Ignore existing partition export and overwrite the ZooKeeper entry. Allows re-exporting a partition that was already exported to the same destination. **IMPORTANT:** this is dangerous because it can lead to duplicated data, use it with caution.

#### `export_merge_tree_partition_retry_initial_backoff_seconds` (Optional)

- **Type**: `UInt64`
- **Default**: `5`
- **Description**: Initial delay (in seconds) before retrying a failed part export. The delay grows exponentially with the per-replica retry count (`delay = min(initial << (attempts - 1), max)`). The back-off is per-replica in-memory state: it only spaces this replica's retries out in time and never prevents another replica from attempting the same part. Retryable failures (transient memory/network/object-storage/Keeper errors) are retried until the task succeeds or `export_merge_tree_partition_task_timeout_seconds` elapses, while non-retryable failures (e.g. schema/type incompatibilities) fail the task immediately.

#### `export_merge_tree_partition_retry_max_backoff_seconds` (Optional)

- **Type**: `UInt64`
- **Default**: `300`
- **Description**: Maximum delay (in seconds) between retries of a failed part export. Caps the exponential growth controlled by `export_merge_tree_partition_retry_initial_backoff_seconds`.

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
When the timeout is exceeded the task transitions to KILLED (same terminal state as `KILL QUERY ... EXPORT PARTITION`), and a `last_exception_per_replica` entry on the replica that fires the timeout is populated with a timeout reason.

Notes:
- Enforcement is best-effort: actual kill latency is bounded by one manifest-updater poll cycle (~30s) plus ZooKeeper watch propagation.

### `export_merge_tree_part_allow_lossy_cast` (Optional)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: Allow `EXPORT PART`/`EXPORT PARTITION` to apply lossy (non-value-preserving) casts when the source and destination column types differ. When disabled, an export that would require a lossy cast throws instead.

  When exporting to Apache Iceberg, the partition value written to the metadata is derived from the source partition columns by casting them to the destination partition-field types and applying the destination partition transform — the same computation the exported data files use. This keeps the Iceberg metadata consistent with the data files.

  **Warning:** A lossy cast on a partition column remains semantically truncating. For example, if a table is partitioned by an `Int64` column and some partition values do not fit into a destination `Int32` partition column, both the data files and the Iceberg metadata will contain the truncated `Int32` value (they agree with each other, but the original `Int64` value is lost). Such casts require `export_merge_tree_part_allow_lossy_cast = 1`.

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
source_database:            default
source_table:               replicated_source
destination_database:       default
destination_table:          s3_destination
create_time:                2025-11-21 18:21:51
partition_id:               2022
transaction_id:             9b2c1e5a-3f47-4c8e-8a1d-6f0b2d4e7c31
query_id:                   3fa3c8d3-7d6b-4f8b-9aa2-2c1f1ad0a111
source_replica:             r1
parts:                      ['2022_0_0_0','2022_1_1_0','2022_2_2_0']
parts_count:                3
parts_to_do:                0
status:                     COMPLETED
last_exception_per_replica: []
exception_count:            0
destination_file_paths:     {'2022_0_0_0':['data/year=2022/2022_0_0_0_<hash>.parquet'],'2022_1_1_0':['data/year=2022/2022_1_1_0_<hash>.parquet'],'2022_2_2_0':['data/year=2022/2022_2_2_0_<hash>.parquet']}
committed_metadata_file:
committed_manifest_list:
committed_manifest_file:
committed_marker_file:      data/commit_2022_9b2c1e5a-3f47-4c8e-8a1d-6f0b2d4e7c31

Row 2:
──────
source_database:            default
source_table:               replicated_source
destination_database:       default
destination_table:          iceberg_destination
create_time:                2025-11-21 18:20:35
partition_id:               2021
transaction_id:             d0e4f7a2-8c19-4b6d-9e3a-1f5c7b2e9d40
query_id:                   1c8e0fd0-6a3a-4d6e-9bd6-bdf64adfe118
source_replica:             r2
parts:                      ['2021_0_0_0']
parts_count:                1
parts_to_do:                0
status:                     COMPLETED
last_exception_per_replica: [('r1','Code: 999. Coordination::Exception: Session expired','2021_0_0_0','2025-11-21 18:20:42',1)]
exception_count:            1
destination_file_paths:     {'2021_0_0_0':['data/year=2021/2021_0_0_0_<hash>.parquet']}
committed_metadata_file:    data/metadata/v3.metadata.json
committed_manifest_list:    data/metadata/snap-4029103741930112856-1-<uuid>.avro
committed_manifest_file:    data/metadata/<uuid>-m0.avro
committed_marker_file:

2 rows in set. Elapsed: 0.019 sec. 

arthur :) 
```

Status values include:
- `PENDING` - Export is queued / in progress
- `COMPLETED` - Export finished successfully
- `FAILED` - Export failed
- `KILLED` - Export was cancelled

### Exception columns

- `last_exception_per_replica` is an `Array(Tuple(replica String, message String, part String, time DateTime, count UInt64))`. Each tuple is the most recent exception observed by a single replica plus a best-effort within-replica `count`. Replicas that have never reported an exception are omitted.
- `exception_count` is the sum of every `count` in `last_exception_per_replica`. Each replica owns its own counter, so cross-replica updates do not race; the sum is exact w.r.t. the snapshot returned. Within a single replica concurrent failing writers may under-count by one.

### Per-part destination file paths

- `destination_file_paths` is a `Map(String, Array(String))` keyed by source part name. Each value is the list of file paths written to the destination object storage when that part was exported (a single part can produce multiple files depending on `max_bytes` / `max_rows`). If a refresh cannot read a processed entry from ZooKeeper, the affected key holds the sentinel `<failed to read from zk>` instead of silently under-counting.

### Commit info columns

These columns surface paths produced by the destination storage during commit, so it is possible to inspect what was written without consulting the destination directly:

- `committed_metadata_file` — for Iceberg destinations: path of the new `vN.metadata.json` written by the commit. Empty for non-Iceberg destinations and before the commit lands. If the commit was already finished by a previous run (detected via the transaction id stored in the snapshot summary), this column carries a human-readable sentinel string instead of a path because the original committer's paths are not recoverable from inside the impl.
- `committed_manifest_list` — for Iceberg destinations: path of the manifest list file (`snap-*.avro`) referenced by the new snapshot. Empty under the same conditions as `committed_metadata_file`.
- `committed_manifest_file` — for Iceberg destinations: path of the manifest file referenced by `committed_manifest_list`. Empty under the same conditions as `committed_metadata_file`.
- `committed_marker_file` — for plain object storage destinations: path of the per-transaction commit marker file written by the destination. Empty for Iceberg destinations and for tasks that have not committed yet.

To pick the latest exception across replicas:

```sql
SELECT
    arraySort(x -> -x.time, last_exception_per_replica)[1] AS latest_exception
FROM system.replicated_partition_exports
WHERE source_table = 'rmt_table' AND destination_table = 's3_table';
```

## Related Features

- [ALTER TABLE EXPORT PART](/docs/en/antalya/part_export.md) - Export individual parts (non-replicated)
