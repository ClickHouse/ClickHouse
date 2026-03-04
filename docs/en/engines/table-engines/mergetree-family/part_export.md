# ALTER TABLE EXPORT PART

## Overview

The `ALTER TABLE EXPORT PART` command exports individual MergeTree data parts to object storage (S3, Azure Blob Storage, etc.), typically in Parquet format. A commit file is shipped to the same destination directory containing all data files exported within that transaction.

**Key Characteristics:**
- **Experimental feature** - must be enabled via `allow_experimental_export_merge_tree_part` setting
- **Asynchronous** - executes in the background, returns immediately
- **Ephemeral** - no automatic retry mechanism; manual retry required on failure
- **Idempotent** - safe to re-export the same part (skips by default if file exists)
- **Preserves sort order** from the source table

## Syntax

```sql
ALTER TABLE [database.]table_name 
EXPORT PART 'part_name' 
TO TABLE [destination_database.]destination_table 
SETTINGS allow_experimental_export_merge_tree_part = 1 
         [, setting_name = value, ...]
```

## Syntax with table function

```sql
ALTER TABLE [database.]table_name
EXPORT PART 'part_name'
TO TABLE FUNCTION s3(s3_conn, filename='table_function', partition_strategy...)
SETTINGS allow_experimental_export_merge_tree_part = 1
         [, setting_name = value, ...]
```

### Parameters

- **`table_name`**: The source MergeTree table containing the part to export
- **`part_name`**: The exact name of the data part to export (e.g., `'2020_1_1_0'`, `'all_1_1_0'`)
- **`destination_table`**: The target table for the export (typically an S3, Azure, or other object storage table)

## Requirements

Source and destination tables must be 100% compatible:

1. **Identical schemas** - same columns, types, and order
2. **Matching partition keys** - partition expressions must be identical

In case a table function is used as the destination, the schema can be omitted and it will be inferred from the source table.

## Settings

### `allow_experimental_export_merge_tree_part` (Required)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: Must be set to `true` to enable the experimental feature.

### `export_merge_tree_part_overwrite_file_if_exists` (Optional)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: If set to `true`, it will overwrite the file. Otherwise, fails with exception.

### `export_merge_tree_part_max_bytes_per_file` (Optional)

- **Type**: `UInt64`
- **Default**: `0`
- **Description**: Maximum number of bytes to write to a single file when exporting a merge tree part. 0 means no limit. This is not a hard limit, and it highly depends on the output format granularity and input source chunk size. Using this might break idempotency, use it with care.

### `export_merge_tree_part_max_rows_per_file` (Optional)

- **Type**: `UInt64`
- **Default**: `0`
- **Description**: Maximum number of rows to write to a single file when exporting a merge tree part. 0 means no limit. This is not a hard limit, and it highly depends on the output format granularity and input source chunk size. Using this might break idempotency, use it with care.

### export_merge_tree_part_throw_on_pending_mutations

- **Type**: `bool`
- **Default**: `true`
- **Description**: If set to true, throws if pending mutations exists for a given part. Note that by default mutations are applied to all parts, which means that if a mutation in practice would only affetct part/partition x, all the other parts/partition will throw upon export. The exception is when the `IN PARTITION` clause was used in the mutation command. Note the `IN PARTITION` clause is not properly implemented for plain MergeTree tables.

### export_merge_tree_part_throw_on_pending_patch_parts

- **Type**: `bool`
- **Default**: `true`
- **Description**: If set to true, throws if pending patch parts exists for a given part. Note that by default mutations are applied to all parts, which means that if a mutation in practice would only affetct part/partition x, all the other parts/partition will throw upon export. The exception is when the `IN PARTITION` clause was used in the mutation command. Note the `IN PARTITION` clause is not properly implemented for plain MergeTree tables.

## Examples

### Basic Export to S3

```sql
-- Create source and destination tables
CREATE TABLE mt_table (id UInt64, year UInt16) 
ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();

CREATE TABLE s3_table (id UInt64, year UInt16) 
ENGINE = S3(s3_conn, filename='data', format=Parquet, partition_strategy='hive') 
PARTITION BY year;

-- Insert and export
INSERT INTO mt_table VALUES (1, 2020), (2, 2020), (3, 2021);

ALTER TABLE mt_table EXPORT PART '2020_1_1_0' TO TABLE s3_table 
SETTINGS allow_experimental_export_merge_tree_part = 1;

ALTER TABLE mt_table EXPORT PART '2021_2_2_0' TO TABLE s3_table 
SETTINGS allow_experimental_export_merge_tree_part = 1;
```

### Table function export

```sql
-- Create source and destination tables
CREATE TABLE mt_table (id UInt64, year UInt16)
ENGINE = MergeTree() PARTITION BY year ORDER BY tuple();

-- Insert and export
INSERT INTO mt_table VALUES (1, 2020), (2, 2020), (3, 2021);

ALTER TABLE mt_table EXPORT PART '2020_1_1_0' TO TABLE FUNCTION s3(s3_conn, filename='table_function', format=Parquet, partition_strategy='hive') PARTITION BY year
SETTINGS allow_experimental_export_merge_tree_part = 1;
```

## Monitoring

### Active Exports

Active exports can be found in the `system.exports` table. As of now, it only shows currently executing exports. It will not show pending or finished exports.

```sql
arthur :) select * from system.exports;

SELECT *
FROM system.exports

Query id: 2026718c-d249-4208-891b-a271f1f93407

Row 1:
──────
source_database:               default
source_table:                  source_mt_table
destination_database:          default
destination_table:             destination_table
create_time:                   2025-11-19 09:09:11
part_name:                     20251016-365_1_1_0
destination_file_paths:        ['table_root/eventDate=2025-10-16/retention=365/20251016-365_1_1_0_17B2F6CD5D3C18E787C07AE3DAF16EB1.1.parquet']
elapsed:                       2.04845441
rows_read:                     1138688 -- 1.14 million
total_rows_to_read:            550961374 -- 550.96 million
total_size_bytes_compressed:   37619147120 -- 37.62 billion
total_size_bytes_uncompressed: 138166213721 -- 138.17 billion
bytes_read_uncompressed:       316892925 -- 316.89 million
memory_usage:                  596006095 -- 596.01 million
peak_memory_usage:             601239033 -- 601.24 million
```

### Export History

You can query succeeded or failed exports in `system.part_log`. For now, it only keeps track of completion events (either success or fails).

```sql
arthur :) select * from system.part_log where event_type='ExportPart' and table = 'replicated_source' order by event_time desc limit 1;

SELECT *
FROM system.part_log
WHERE (event_type = 'ExportPart') AND (`table` = 'replicated_source')
ORDER BY event_time DESC
LIMIT 1

Query id: ae1c1cd3-c20e-4f20-8b82-ed1f6af0237f

Row 1:
──────
hostname:                arthur
query_id:                
event_type:              ExportPart
merge_reason:            NotAMerge
merge_algorithm:         Undecided
event_date:              2025-11-19
event_time:              2025-11-19 09:08:31
event_time_microseconds: 2025-11-19 09:08:31.974701
duration_ms:             4
database:                default
table:                   replicated_source
table_uuid:              78471c67-24f4-4398-9df5-ad0a6c3daf41
part_name:               2021_0_0_0
partition_id:            2021
partition:               2021
part_type:               Compact
disk_name:               default
path_on_disk:            
remote_file_paths        ['year=2021/2021_0_0_0_78C704B133D41CB0EF64DD2A9ED3B6BA.1.parquet']
rows:                    1
size_in_bytes:           272
merged_from:             ['2021_0_0_0']
bytes_uncompressed:      86
read_rows:               1
read_bytes:              6
peak_memory_usage:       22
error:                   0
exception:               
ProfileEvents:           {}
```

### Profile Events

- `PartsExports` - Successful exports
- `PartsExportFailures` - Failed exports
- `PartsExportDuplicated` - Number of part exports that failed because target already exists.
- `PartsExportTotalMilliseconds` - Total time

### Split large files

```sql
alter table big_table export part '2025_0_32_3' to table replicated_big_destination SETTINGS export_merge_tree_part_max_bytes_per_file=10000000, output_format_parquet_row_group_size_bytes=5000000;

arthur :) select * from system.exports;

SELECT *
FROM system.exports

Query id: d78d9ce5-cfbc-4957-b7dd-bc8129811634

Row 1:
──────
source_database:               default
source_table:                  big_table
destination_database:          default
destination_table:             replicated_big_destination
create_time:                   2025-12-15 13:12:48
part_name:                     2025_0_32_3
destination_file_paths:        ['replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.1.parquet','replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.2.parquet','replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.3.parquet','replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.4.parquet']
elapsed:                       14.360427274
rows_read:                     10256384 -- 10.26 million
total_rows_to_read:            10485760 -- 10.49 million
total_size_bytes_compressed:   83779395 -- 83.78 million
total_size_bytes_uncompressed: 10611691600 -- 10.61 billion
bytes_read_uncompressed:       10440998912 -- 10.44 billion
memory_usage:                  89795477 -- 89.80 million
peak_memory_usage:             107362133 -- 107.36 million

1 row in set. Elapsed: 0.014 sec. 

arthur :) select * from system.part_log where event_type = 'ExportPart' order by event_time desc limit 1 format Vertical;

SELECT *
FROM system.part_log
WHERE event_type = 'ExportPart'
ORDER BY event_time DESC
LIMIT 1
FORMAT Vertical

Query id: 95128b01-b751-4726-8e3e-320728ac6af7

Row 1:
──────
hostname:                arthur
query_id:                
event_type:              ExportPart
merge_reason:            NotAMerge
merge_algorithm:         Undecided
event_date:              2025-12-15
event_time:              2025-12-15 13:13:03
event_time_microseconds: 2025-12-15 13:13:03.197492
duration_ms:             14673
database:                default
table:                   big_table
table_uuid:              a3eeeea0-295c-41a3-84ef-6b5463dbbe8c
part_name:               2025_0_32_3
partition_id:            2025
partition:               2025
part_type:               Wide
disk_name:               default
path_on_disk:            ./store/a3e/a3eeeea0-295c-41a3-84ef-6b5463dbbe8c/2025_0_32_3/
remote_file_paths:       ['replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.1.parquet','replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.2.parquet','replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.3.parquet','replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.4.parquet']
rows:                    10485760 -- 10.49 million
size_in_bytes:           83779395 -- 83.78 million
merged_from:             ['2025_0_32_3']
bytes_uncompressed:      10611691600 -- 10.61 billion
read_rows:               10485760 -- 10.49 million
read_bytes:              10674503680 -- 10.67 billion
peak_memory_usage:       107362133 -- 107.36 million
error:                   0
exception:               
ProfileEvents:           {}

1 row in set. Elapsed: 0.044 sec.

arthur :) select _path, formatReadableSize(_size) as _size from s3(s3_conn, filename='**', format=One);

SELECT
    _path,
    formatReadableSize(_size) AS _size
FROM s3(s3_conn, filename = '**', format = One)

Query id: c48ae709-f590-4d1b-8158-191f8d628966

   ┌─_path────────────────────────────────────────────────────────────────────────────────┬─_size─────┐
1. │ test/replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.1.parquet │ 17.36 MiB │
2. │ test/replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.2.parquet │ 17.32 MiB │
3. │ test/replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.4.parquet │ 5.04 MiB  │
4. │ test/replicated_big/year=2025/2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7.3.parquet │ 17.40 MiB │
5. │ test/replicated_big/year=2025/commit_2025_0_32_3_E439C23833C39C6E5104F6F4D1048BE7    │ 320.00 B  │
   └──────────────────────────────────────────────────────────────────────────────────────┴───────────┘

5 rows in set. Elapsed: 0.072 sec. 
```
