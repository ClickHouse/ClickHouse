# ALTER TABLE EXPORT PART

## Overview

The `ALTER TABLE EXPORT PART` command exports individual MergeTree data parts to object storage (S3, Azure Blob Storage, etc.) or data lakes like Apache Iceberg tables (with and without catalogs), typically in Parquet format.

**Key Characteristics:**
- **Experimental feature** - must be enabled via `allow_experimental_export_merge_tree_part` setting
- **Asynchronous** - executes in the background, returns immediately
- **Ephemeral** - no automatic retry mechanism; manual retry required on failure
- **Idempotent** - safe to re-export the same part (skips by default if file exists)
- **Preserves sort order** from the source table

### On Apache Iceberg storage exports:

Each MergeTree part will become a separate file (or more depending on `max_bytes` and `max_rows` settings) following the engine naming convention. Once the part has been exported, new snapshots / manifest files are generated and the data is committed using the Apache Iceberg commit mechanism.

### On plain object storage exports:

A commit file is shipped to the same destination directory containing all data files exported within that transaction.

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

Source and destination tables must support positional schema conversion. The following differences between the two schemas are allowed:

- **Column names** may differ between source and destination for non-partition-key columns - columns are matched by position, similar to `INSERT INTO dest SELECT * FROM src`, not by name.
- **Column types** may differ, as long as the source type is safely castable to the destination type. Set `export_merge_tree_part_allow_lossy_cast = 1` to also permit lossy casts.
- **`Tuple` element names** may differ if either the source or destination declares the tuple without named elements: an unnamed `Tuple` (e.g. `Tuple(Int32, Int32)`) is matched against the destination by element position and type only, not by name. For example, exporting from `t Tuple(Int32, Int32)` to `t Tuple(x Int32, y Int32)` is allowed as long as element types match positionally.

The following must match between source and destination:

1. **Column count** - source and destination must have the same number of columns by default. A mismatch in either direction throws `NUMBER_OF_COLUMNS_DOESNT_MATCH`. Set `export_merge_tree_part_schema_mismatch_mode = 'ignore_extra_source_columns_by_position'` to allow a source table with extra trailing columns; the destination having more columns than the source is still rejected in this mode.
2. **`PARTITION BY` expressions** - for destinations other than data lakes, the source and destination `PARTITION BY` expressions must be identical. For Apache Iceberg destinations, the source partition key must be representable as an Iceberg partition spec and must match the destination partition fields and transforms.
3. **The position of every column backing the partition key** - it is not enough for the `PARTITION BY` expressions to be textually identical: every top-level column that provides a column or subcolumn used by the source table's partition key must have the same name at the same position in the destination table's schema. If such a column contains a named `Tuple`, its element names must also be declared in the same order (an unnamed `Tuple` on either side is exempt from this, per the allowance above). This comparison is recursive through nested tuples and through container types such as `Array` and `Map`.

  For example, `CREATE TABLE src (a Int32, b Int32) ... PARTITION BY a` and `CREATE TABLE dst (b Int32, a Int32) ... PARTITION BY a` both have the expression `PARTITION BY a`, but `a` is at position 0 in `src` and position 1 in `dst`. The export is rejected with a `BAD_ARGUMENTS` exception whose message includes `Cannot export to <destination>: partition key column 'a' is at position 0 in the source table, but the destination's column at that position is named 'b'`.

  This position check applies only to partition-key columns. A mismatch in the position of a non-partition-key column is allowed by name (see above) and is only rejected if the resulting types aren't castable. If two non-partition-key columns happen to have swapped positions but compatible types, the export succeeds and silently writes values into the wrong destination column, so keep the intended column order rather than relying on type compatibility alone.

  For `PARTITION BY t.a`, this rule applies to the top-level owning column `t`. Exporting from `t Tuple(a Int32, b Int32)` to `t Tuple(b Int32, a Int32)` is rejected, even though `a` is accessed by name. Requiring a stable layout for every partition-key owner also protects positional expressions such as `tupleElement(t, 1)` from changing their meaning after conversion.

  The same rule applies when the named tuple is nested inside a container. For example, `arr Array(Tuple(a Int32, b Int32))` and `arr Array(Tuple(b Int32, a Int32))` are incompatible when `arr` provides an input to the partition key. Likewise, tuple layouts in both the key and value types of `Map` are checked recursively.

  In this case, the export throws a `BAD_ARGUMENTS` exception whose message includes `partition key column 't' has a different Tuple element layout in the source (Tuple(a Int32, b Int32)) and destination (Tuple(b Int32, a Int32)). Tuple element names must be declared in the same order in both tables`.

  For partition expressions containing functions, the check applies to their input columns. For example, `PARTITION BY (toYYYYMM(ts), category)` requires both `ts` and `category` to have the same names at the same top-level positions in both tables.

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

### `export_merge_tree_part_allow_lossy_cast` (Optional)

- **Type**: `Bool`
- **Default**: `false`
- **Description**: Allow `EXPORT PART`/`EXPORT PARTITION` to apply lossy (non-value-preserving) casts when the source and destination column types differ. When disabled, an export that would require a lossy cast throws instead.

  When exporting to Apache Iceberg, the partition value written to the metadata is derived from the source partition columns by casting them to the destination partition-field types and applying the destination partition transform — the same computation the exported data files use. This keeps the Iceberg metadata consistent with the data files.

  **Warning:** A lossy cast on a partition column remains semantically truncating. For example, if a table is partitioned by an `Int64` column and some partition values do not fit into a destination `Int32` partition column, both the data files and the Iceberg metadata will contain the truncated `Int32` value (they agree with each other, but the original `Int64` value is lost). Such casts require `export_merge_tree_part_allow_lossy_cast = 1`.

### `export_merge_tree_part_schema_mismatch_mode` (Optional)

- **Type**: `MergeTreePartExportSchemaMismatchMode`
- **Default**: `strict`
- **Description**: Controls whether `EXPORT PART`/`EXPORT PARTITION` allows a column-count mismatch between the source `MergeTree` table and the destination table. Columns are matched positionally, like `INSERT INTO dest SELECT * FROM src`. Possible values:
  - `strict` - the source and destination must have the same number of columns. A mismatch in either direction throws `NUMBER_OF_COLUMNS_DOESNT_MATCH`.
  - `ignore_extra_source_columns_by_position` - the source may have more columns than the destination. The extra trailing source columns (by position) are dropped and not exported. The destination having more columns than the source is still rejected in this mode.

  The extra trailing source columns are still read and evaluated (including `MATERIALIZED`/`ALIAS` columns, and any column another kept column's `ALIAS`/`MATERIALIZED` expression depends on) before being dropped, so this setting only changes which columns end up in the destination, not what is computed while reading the part.


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
