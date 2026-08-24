Feature Design: `ALTER TABLE EXPORT PART` and `ALTER TABLE EXPORT PARTITION`
============================================================================

**Status:** draft
**Author(s):** Arthur Passos
**Related issues/PRs:**
https://github.com/Altinity/ClickHouse/pull/1618

**Last updated:** 2026-04-21

---

## 1. Requirements

### Motivation

Cost of storing data is a growing problem for large analytic systems
that use open source ClickHouse and replicated block storage. The core
problem is that block storage is (a) expensive and (b) replication makes
multiple copies.  Project Antalya solves the storage cost problem using
the Hybrid Table Engine.  Hybrid tables allow users to split tables
into segments, placing hot data on replicated block storage and
cold data on shared Iceberg tables using Parquet data files.

The hybrid table approach requires a robust mechanism to export table
data from `MergeTree` tables into shared storage. The mechanism must
be fast, use machine resources efficiently, handle failures
automatically, and be easy to monitor. This design covers two new
ClickHouse commands to export data so users can populate hybrid 
tables and move data to them at regular intervals. 

* `ALTER TABLE EXPORT PART` -- Exports a single part to a destination.

* `ALTER TABLE EXPORT PARTITION` -- Exports one or more partitions to a destination.

Both commands accept two destination families:

1. **Plain object storage** (`S3`, `AzureBlobStorage`, equivalents).
   Output is Parquet laid out in hive-partitioned directories. Atomicity
   is provided by a sidecar *commit file* that enumerates the data files
   written in the transaction; readers that want atomicity filter by
   commit. An external catalog (Glue, REST, Nessie, Lakekeeper, ...) can
   register this layout as an Iceberg table afterward, but the export
   commands themselves do not interact with any catalog in this mode.

2. **Apache Iceberg tables, with or without a catalog** (`Iceberg*`
   engines, `iceberg*` table functions, `DatabaseIceberg`). Output is
   Parquet data files plus per-file Avro *statistics sidecars*; on
   commit the EXPORT process assembles a new Iceberg manifest,
   writes a new `metadata.json`, and swaps the catalog pointer (or the
   warehouse `metadata.json` pointer when catalog-less). Atomicity is
   native — the snapshot either exists or it does not.

These commands replace `INSERT INTO ... SELECT FROM` pipelines that
select rows and write them out to one or more Parquet files. That
approach uses resources for sorting, does not coordinate across replicas,
and does not take advantage of existing partitioning and sorting in
`MergeTree`. 

### Requirements

1. **SQL only.** All operations related to export are available in SQL.
   There should be no need to use non-SQL tools or directly access
   storage to run exports or clean up problems.

2. **Efficient, order-preserving writes.** Write a specified `MergeTree`
   part (or every part of a specified partition) to an object-storage
   destination in Parquet, preserving the source part's sort order,
   without using a `SELECT ORDER BY` pass. Exporting a part should use the
   same or less RAM than doing an `INSERT...SELECT...ORDER BY` on the same
   data. (For example, it's not uncommon for the latter command to run 
   out of memory on large parts when the part ordering is added to the
   SELECT ORDER BY.)

3. **Output file management.** Allow users to break exported parts
   into smaller Parquet files, which helps ensure good performance
   when scanning Iceberg data.

4. **Data type equivalence.** Map ClickHouse types to Iceberg types
   that cast back without data loss to the original ClickHouse types
   when selecting data. Applications that access exported data through
   a Hybrid table should be able to read the data back from Iceberg
   without requiring changes.

5. **Atomic transfer.** Readers should never see a partial export.
   The mechanism depends on the destination family:
   - **Plain object storage.** Each transaction writes a sidecar
     commit file that lists every data file produced by the transaction.
     Readers that want atomicity filter by commit; a crash before the
     commit file lands leaves only orphaned data files.
   - **Iceberg destinations.** Atomicity is provided by Iceberg's own
     snapshot-commit protocol — the new snapshot either becomes the
     current metadata pointer or it does not. A crash before the
     pointer swap leaves only orphaned data files and sidecars.

6. **Distributed operation.**
   - `EXPORT PART` always runs locally on the ClickHouse host where
     it is invoked.
   - `EXPORT PARTITION` from non-replicated `MergeTree` tables runs
     locally on the ClickHouse host where it is invoked.
   - `EXPORT PARTITION` is cluster-coordinated on `Replicated*MergeTree`
     tables: any replica that has a given part contributes to the
     export; the task is persistent and resumes after restarts.

7. **Observability.** 
   It must be possible for users to track the following from system tables: 
   - Export part request status.
   - Export partition request status.
   - Relevant profile events related to export. 

8. **Error recovery.**
   - **Idempotence.** Re-issuing the same export to the same
     destination is a no-op while the export is running. (There should
     be a way to track 'recent' exports so that they are idempotent as
     well.)
   - **Clean-up.** If file or metadata clean-up is required before resubmitting a failed
     export, it must be possible to do so using only SQL commands. 
   - **Automatic restart.** `EXPORT PARTITION` task is persistent and
     resumes after restarts.

9. **Killable.** It must be possible to terminate any `ALTER TABLE EXPORT` command. 
   The command should be idempotent and must throw a clear exception on failure
   rather than hanging. 

### Open questions and future requirements

The design should address the following topics in the near future. 

- EXPORT PARTITION for MergeTree tables. Must work without Keeper installation. 
- Export history. Provide a system table to track the history of part exports. 
- Flexible casting that addresses issues like the following. 
  - Handling potentially lossy casts like INSERT SELECT: int64 -> int32. 
  - Export to tables that are missing columns. 
  - How to map column names--by position or by name? (e.g, is id, name, age compatible with id, age, name)?

### Out of scope requirements

- Non-Parquet output file formats. Only `Parquet` is targeted in this iteration. Later 
  iterations may add new output file formats. 
- Exporting to arbitrary table functions. Only those backed by an object-storage engine that
  supports exports (e.g. `s3`, `azure`) are valid; others throw `NOT_IMPLEMENTED`.
- Non-matching Iceberg schema, sorting or partitioning. Not supported. The source 
  `MergeTree` schema and partition keys must be compatible with the destination 
  Iceberg table's current `schema-id` and `partition-spec-id`. Destination partition values 
  are derived directly from the source part's partition key; we do not recompute them 
  from row data.
- Any read/query path over exported files — consumption happens via normal `S3` / `s3` /
  external-engine reads.
- Synchronous exports. Not supported. EXPORT commands return immediately to client after
  starting the export task; completion is polled via system tables.
- Importing parts back from object storage (that is tracked separately).

### Constraints

- Experimental gate: `allow_experimental_export_merge_tree_part` (query-level) for `EXPORT PART`;
  `allow_experimental_export_merge_tree_partition_feature` (server-level) for `EXPORT PARTITION`.
- For best results `EXPORT PARTITION` requires a ZooKeeper / `clickhouse-keeper` ensemble with 
  the `multi_read` feature flag. This reduces API calls and ensures transactional consistency 
  when reading multiple fields. 
  governed by the destination table's Iceberg partition spec instead.
- No change to `MergeTree` on-disk part format; only the Keeper schema under the table's
  replication path is extended. The extension is tranparent to users. 

### References

- `docs/en/antalya/part_export.md`
- `docs/en/antalya/partition_export.md`
- `tests/queries/0_stateless/03572_export_merge_tree_part_basic.sh`
- `tests/queries/0_stateless/03572_export_merge_tree_part_to_object_storage_simple.sql`
- `tests/queries/0_stateless/03572_export_merge_tree_part_limits_and_table_functions.sh`
- `tests/queries/0_stateless/03572_export_merge_tree_part_special_columns.sh`
- `tests/queries/0_stateless/03572_export_replicated_merge_tree_part_to_object_storage.sh`
- `tests/queries/0_stateless/03572_export_replicated_merge_tree_part_to_object_storage_simple.sql`
- `tests/queries/0_stateless/03604_export_merge_tree_partition.sh`
- `tests/queries/0_stateless/03608_export_merge_tree_part_filename_pattern.sh`
- `tests/integration/test_export_merge_tree_part_to_object_storage/test.py`
- `tests/integration/test_export_replicated_mt_partition_to_object_storage/test.py`

---

## 2. Functional specification

### User-facing behavior
A user points `ALTER TABLE` at a `MergeTree` source and a destination (table or table function).
The command returns immediately with no rows. The export runs in the background; progress lives
in `system.exports` and, for partition exports, `system.replicated_partition_exports`. Successful
exports append to `system.part_log` with `event_type = 'ExportPart'`.

Output shape depends on the destination family:

- **Plain object storage.** One Parquet data file per part (or per chunk when split by
  size/rows) plus one commit file per transaction — `<dest>/<partition>/<part>_<checksum>.<N>.parquet`
  and `<dest>/commit_<...>`. Readers that want atomicity filter by commit. This is the default path, 
  but it can be customized to handle sharding, which is not covered by the default case. See the
  ``

- **Iceberg destination.** One Parquet data file per part (or per chunk) plus a sibling
  Avro statistics sidecar `<data_file>_clickhouse_export_part_sidecar.avro` carrying
  `record_count`, `file_size_in_bytes`, `column_sizes`, `null_value_counts`, `lower_bounds`,
  and `upper_bounds`. The per-part task does not modify Iceberg metadata. On final commit
  the initiating replica reads every sidecar, assembles a new manifest and manifest list,
  writes a new `metadata.json`, and atomically swaps the pointer (via the catalog when one
  is configured; otherwise via the warehouse `metadata.json` pointer). The manifest summary
  contains `clickhouse.export-partition-transaction-id`, checked before every commit attempt
  to prevent a double-commit after a post-commit / pre-status-update crash. Sidecar files
  are not referenced from any Iceberg manifest and can be deleted safely after the commit
  lands; ClickHouse does not reap them.

### SQL syntax / API

```sql
-- Export a single part to a destination table
ALTER TABLE [db.]table
    EXPORT PART 'part_name'
    TO TABLE [dest_db.]dest_table
    [SETTINGS ...];

-- Export a single part to a destination table function
ALTER TABLE [db.]table
    EXPORT PART 'part_name'
    TO TABLE FUNCTION s3(...) PARTITION BY <expr>
    [SETTINGS ...];

-- Export every active part of a partition (Replicated*MergeTree only)
ALTER TABLE [db.]table
    EXPORT PARTITION ID 'partition_id'
    TO TABLE [dest_db.]dest_table
    [SETTINGS ...];

-- Export every active part of all partitions (Replicated*MergeTree only)
ALTER TABLE [db.]table
    EXPORT PARTITION ALL
    TO TABLE [dest_db.]dest_table
    [SETTINGS ...];

-- Cancel one or more partition exports
KILL EXPORT PARTITION WHERE <predicate on system.replicated_partition_exports>;
```

### Individual command examples
These are derived from `tests/queries/0_stateless/03572_*` and `03604_export_merge_tree_partition.sh`.

```sql
-- Part export to S3 table
ALTER TABLE mt_table EXPORT PART '2020_1_1_0' TO TABLE s3_table
SETTINGS allow_experimental_export_merge_tree_part = 1;

-- Part export to S3 table function (schema inferred from source)
ALTER TABLE mt_table EXPORT PART '2020_1_1_0'
TO TABLE FUNCTION s3(s3_conn, filename='tf', format='Parquet', partition_strategy='hive')
PARTITION BY year
SETTINGS allow_experimental_export_merge_tree_part = 1;

-- Split large part across multiple Parquet files
ALTER TABLE big EXPORT PART '2025_0_32_3' TO TABLE big_dest
SETTINGS allow_experimental_export_merge_tree_part = 1,
         export_merge_tree_part_max_bytes_per_file = 10000000,
         output_format_parquet_row_group_size_bytes = 5000000;
-- (See note on settings below. Iceberg table engine now has built-in
-- settings for Parquet files.) 

-- Partition export across a Replicated cluster. This currently
-- selects the parts on the replica that receives the plan. This 
-- means the result may vary if new parts are arriving on other 
-- replicas. 
ALTER TABLE rmt_table EXPORT PARTITION ID '2020' TO TABLE s3_table;

-- Cancel by filter. The WHERE uses the same filter used to read from `system.replicated_partition_exports`.
KILL EXPORT PARTITION
WHERE partition_id = '2020'
  AND source_table = 'rmt_table'
  AND destination_table = 's3_table';
```

### End-to-end examples

Two parallel walkthroughs illustrate each destination family. Both
begin from the same `ReplicatedMergeTree` source.

```sql
-- Source table (shared by both examples).
CREATE TABLE events
(
    id    UInt64,
    ts    DateTime,
    year  UInt16
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/events', 'r1')
PARTITION BY year
ORDER BY (year, id);

-- Seed two partitions (2024, 2025) straight from `system.numbers`.
INSERT INTO events
SELECT
    number                                                 AS id,
    toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS ts,
    2024                                                   AS year
FROM system.numbers
LIMIT 1000000;

INSERT INTO events
SELECT
    number                                                 AS id,
    toDateTime('2025-01-01 00:00:00') + INTERVAL number SECOND AS ts,
    2025                                                   AS year
FROM system.numbers
LIMIT 1000000;
```

#### Plain object storage (hive layout)

Export to a hive-partitioned S3 destination. The on-disk shape is what
an external Iceberg catalog (Glue, REST, Nessie, Lakekeeper, ...) would
register as an Iceberg table; the `EXPORT PARTITION` command itself
does not touch any catalog in this mode.

```sql
-- 1. Destination: S3 with hive partition layout.
CREATE TABLE events_s3
(
    id    UInt64,
    ts    DateTime,
    year  UInt16
)
ENGINE = S3(s3_conn, filename='warehouse/events', format = Parquet, partition_strategy = 'hive')
PARTITION BY year;

-- 2. Export the 2024 partition. Returns immediately; runs in the background.
ALTER TABLE events EXPORT PARTITION ID '2024' TO TABLE events_s3;

-- 3. Watch progress (Keeper round-trip — use sparingly).
SELECT status, parts_count, parts_to_do, last_exception
FROM system.replicated_partition_exports
WHERE source_table = 'events' AND partition_id = '2024';

-- 4. When status = 'COMPLETED', the destination bucket contains:
--      warehouse/events/year=2024/<part_name>_<checksum>.1.parquet    (one per part)
--      warehouse/events/commit_2024_<tx_id>                           (atomicity manifest)
--    Readers that filter by commit see either the full partition or nothing.
SELECT count() FROM events_s3 WHERE year = 2024;

-- 5. Or inspect the layout directly.
SELECT _path
FROM s3(s3_conn, filename = 'warehouse/events/year=2024/**', format = 'One')
ORDER BY _path;
```

#### Iceberg destination

Export directly to an Iceberg table. Unlike the plain-object-storage
flow, `EXPORT PARTITION` writes native Iceberg metadata on commit, so
the result is queryable as an Iceberg table immediately — no external
registration step. The example uses `IcebergS3` without a catalog;
swap in `DatabaseIceberg` or an `iceberg(...)` catalog-backed table
function to route through REST / Glue / Unity.

**Open Issue** We need to specify how to commit via a catalog. 

```sql
-- 1. Destination: an Iceberg table backed by S3. No catalog required
--    for this form; the warehouse metadata.json pointer is managed
--    by ClickHouse directly.
CREATE TABLE events_iceberg
(
    id    UInt64,
    ts    DateTime,
    year  UInt16
)
ENGINE = IcebergS3(s3_conn, filename='warehouse/events_iceberg/')
PARTITION BY year;

-- 2. Export the 2024 partition. Returns immediately; runs in the background.
ALTER TABLE events EXPORT PARTITION ID '2024' TO TABLE events_iceberg;

-- 3. Watch progress. You can use the system.exports table for this. 
SELECT status, parts_count, parts_to_do, last_exception
FROM system.replicated_partition_exports
WHERE source_table = 'events' AND partition_id = '2024';

-- 4. When status = 'COMPLETED', the destination contains a fully-formed
--    Iceberg table. Readers see the snapshot atomically — either the
--    full partition or nothing.
SELECT count() FROM events_iceberg WHERE year = 2024;

-- 5. Object layout: each data file has a sidecar carrying per-file stats
--    used at commit time. These are ClickHouse-private and unreferenced
--    from any Iceberg manifest; safe to delete after COMPLETED.
SELECT _path
FROM s3(s3_conn, filename = 'warehouse/events_iceberg/data/year=2024/**', format = 'One')
ORDER BY _path;
--  warehouse/events_iceberg/data/year=2024/<part>_<checksum>.1.parquet
--  warehouse/events_iceberg/data/year=2024/<part>_<checksum>.1.parquet_clickhouse_export_part_sidecar.avro
--  ...
--  warehouse/events_iceberg/metadata/v<N>.metadata.json
--  warehouse/events_iceberg/metadata/snap-<snapshot_id>-*.avro
--  warehouse/events_iceberg/metadata/<manifest_uuid>.avro

-- 6. The commit carries an idempotency marker in the manifest summary
--    (clickhouse.export-partition-transaction-id). If the initiator
--    crashes post-commit / pre-status-update, a retry sees its own
--    transaction id in the target's latest manifest and skips instead
--    of double-committing.
```

The EXPORT PARTITION flow initially only works on ReplicatedMergeTree tables and 
requires Keeper. Future iterations will support MergeTree tables as a source. 

### Operational notes

The following notes expand on expected behavior of commands. 

1. When writing to object storage using `partition_strategy = 'wildcard'`, either wildcard
   or 'hive' arguments are permitted. (This setting has no impact on Iceberg, 
   which records file partitions using metadata.)

2. By default `ALTER TABLE t EXPORT PART 'p' TO TABLE s3_t` writes
   `<dir>/<partition>/<part>_<checksum>.1.parquet` plus
   `<dir>/commit_<part>_<checksum>`. You can read this using 
   `SELECT * FROM s3(...)`. 

3. `ALTER TABLE rmt EXPORT PARTITION ID 'p' TO TABLE s3_t` exports
   every active part of partition `p` across all replicas that host
   it; `system.replicated_partition_exports` converges to `COMPLETED`.

4. Re-issuing the same `EXPORT PARTITION` within
   `export_merge_tree_partition_manifest_ttl` is a no-op (no
   duplicate files) unless `export_merge_tree_partition_force_export = 1`. This
   behavior avoids accidentally exporting the same data twice. Note, however
   that forcing the operation is dangerous if ClickHouse can't clean up the
   previous operation. In this case you'll potentially commit files twice. 

5. Killing an in-flight partition export via `KILL EXPORT PARTITION`
   transitions status to `KILLED` and stops all replicas' contributions.

6. Exception during part export is counted in `PartsExportFailures`;
   retry behavior honors `export_merge_tree_partition_max_retries`. The
   same budget also bounds per-task commit retries for Iceberg
   destinations; the task fails terminally if commit retries alone
   exceed the budget.

7. A partition export that remains in `PENDING` longer than
   `export_merge_tree_partition_task_timeout_seconds` (default 3600s;
   `0` disables) is auto-killed by the background cleanup loop and
   transitions to `KILLED` with a timeout reason recorded in
   `last_exception`. Enforcement is best-effort: actual kill latency is
   bounded by one manifest-updater poll cycle (~30s) plus ZooKeeper
   watch propagation. This is primarily a backstop against tasks stuck
   on missing parts, a missing destination table, or an infinite
   commit-retry loop against Iceberg.

8. **Third-party Iceberg catalog manifest cleanup.** If the catalog
   reaps old manifest files, its retention window MUST exceed
   `export_merge_tree_partition_task_timeout_seconds`. Otherwise the
   rare "sole node commits to Iceberg, crashes before the `COMPLETED`
   status reaches Keeper, boots back up after the reaper has deleted
   its commit manifest" scenario can produce duplicate data when the
   recovered node retries the commit. ClickHouse does not reap its own
   export artifacts; the Iceberg sidecars (`*_clickhouse_export_part_sidecar.avro`)
   are safe to delete once the commit has landed.

9. **Settings in `PART` vs `PARTIION` export.** There is a subtle difference in 
   how settings are handled between PART and PARTITION export.
   - For part export, the settings used at the moment of the query are preserved 
     and re-stored in the background export task.
   - For partition export, it is slightly harder to preserve the settings because 
     we need to serialize them in ZooKeeper. This is done for a few very important
     settings including export_merge_tree_part_max_bytes_per_file. This will be 
     cleaned up in future iterations. 

### Settings

| Setting | Scope | Default | Range / Values | Applies to | Description |
| --- | --- | --- | --- | --- | --- |
| `allow_experimental_export_merge_tree_part` | query | `false` | `Bool` | `EXPORT PART` | Experimental gate; required. |
| `allow_experimental_export_merge_tree_partition_feature` | server | `false` | `Bool` | `EXPORT PARTITION` | Experimental gate; required. |
| `export_merge_tree_part_overwrite_file_if_exists` | query | `false` | `Bool` | `EXPORT PART` | Overwrite existing destination file; otherwise throws. |
| `export_merge_tree_part_max_bytes_per_file` | query | `0` | `UInt64` (`0`=unlimited) | both | Soft cap per output file. Non-zero values can break idempotency. (SEE NOTE 1 below.)|
| `export_merge_tree_part_max_rows_per_file` | query | `0` | `UInt64` (`0`=unlimited) | both | Soft cap per output file. Non-zero values can break idempotency. |
| `export_merge_tree_part_throw_on_pending_mutations` | query | `true` | `Bool` | both | Refuse to export parts with pending mutations (unless mutation was `IN PARTITION`). |
| `export_merge_tree_part_throw_on_pending_patch_parts` | query | `true` | `Bool` | both | Refuse to export parts with pending patch parts. |
| `export_merge_tree_part_filename_pattern` | query | `{part_name}_{checksum}` | `String` | both | Filename template; supports `{part_name}`, `{checksum}`, `{database}`, `{table}`, server macros. |
| `export_merge_tree_partition_force_export` | query | `false` | `Bool` | `EXPORT PARTITION` | Overwrite a live Keeper manifest for the same `(source, destination, partition_id)`. Dangerous — can produce duplicate data on the destination; use with caution. |
| `export_merge_tree_partition_max_retries` | query | `3` | `UInt64` | `EXPORT PARTITION` | Retry budget applied to both per-part export attempts and per-task commit attempts (Iceberg). The task fails terminally if commit retries alone exceed the budget. |
| `export_merge_tree_partition_manifest_ttl` | query | `180` (seconds) | `UInt64` | `EXPORT PARTITION` | Live-manifest TTL; acts as the idempotency window. Does not interrupt in-flight tasks. Keep this greater than `export_merge_tree_partition_task_timeout_seconds` if you want the `KILLED` entry to remain visible in `system.replicated_partition_exports` after the timeout fires. |
| `export_merge_tree_partition_task_timeout_seconds` | query | `3600` (seconds) | `UInt64` (`0`=disable) | `EXPORT PARTITION` | Wall-clock cap for `PENDING` tasks; on expiry transitions to `KILLED` with a timeout reason. Measured from manifest `create_time`. Enforcement latency ≈ one manifest-updater poll cycle (~30s) plus Keeper watch propagation. |
| `export_merge_tree_partition_system_table_prefer_remote_information` | query | `false` | `Bool` | `EXPORT PARTITION` | When `true`, `system.replicated_partition_exports` fetches fresh state from Keeper (requires the `MULTI_READ` feature flag); when `false`, uses local cached state. **Default flipped from `true` to `false` in this release** — Keeper round-trips were more expensive than warranted for the typical observability workload. (See NOTE 2.)|
| `export_merge_tree_part_file_already_exists_policy` | query | `skip` | `skip` / `error` / `overwrite` | `EXPORT PARTITION` | Per-file policy during partition export. |

Default-value impact: all new settings default to "off" or to
conservative values (pending-mutation guards default to throwing). One
default *changed* in this release:
`export_merge_tree_partition_system_table_prefer_remote_information`
flipped from `true` to `false`, so `system.replicated_partition_exports`
now serves local cached state by default instead of querying Keeper.
Users who relied on always-fresh results must set it back to `true`
explicitly.

**NOTE 1:** `export_merge_tree_part_max_bytes_per_file` overrides `iceberg_insert_max_bytes_in_data_file` or 
other more specific file size parameters. 

EXPORT should also observe the following existing settings for export to Iceberg / Parquet:
- `iceberg_insert_max_bytes_in_data_file` (as above, overridden by `export_merge_tree_part_max_bytes_per_file` if specified)
- `iceberg_insert_max_rows_in_data_file`
- `output_format_parquet_row_group_size`
- `output_format_parquet_row_group_size_bytes`
- `output_format_parquet_data_page_size`
- `output_format_parquet_compression_method`
- `output_format_parquet_version`

**NOTE 2:** `export_merge_tree_partition_system_table_prefer_remote_information` may be dropped. 
Querying Keeper from the user path is complex and has side effects. 

### System tables / metrics / log messages / observability

- `system.exports` — rows for currently-executing part exports (source/destination tables,
  `part_name`, destination paths, `elapsed`, rows/bytes counters, memory counters). Dropped when
  the export completes.
- `system.replicated_partition_exports` — rows for `EXPORT PARTITION` tasks. Backed by Keeper;
  querying it is a Keeper round-trip and should be used sparingly. Columns include
  `source_database`, `source_table`, `destination_database`, `destination_table`, `create_time`,
  `partition_id`, `transaction_id`, `source_replica`, `parts`, `parts_count`, `parts_to_do`,
  `status`, `exception_replica`, `last_exception`, `exception_part`, `exception_count`.
- `system.part_log` — each completed part export appends one row with `event_type = 'ExportPart'`,
  filled `remote_file_paths`, `merged_from = [part_name]`, plus standard timing / size / error
  fields.
- `ProfileEvents`:
  - `PartsExports` — successful part-export completions.
  - `PartsExportFailures` — failures.
  - `PartsExportDuplicated` — skipped because destination file already existed.
  - `PartsExportTotalMilliseconds` — cumulative wall time.
- Status enum on `system.replicated_partition_exports`: `PENDING`, `COMPLETED`, `FAILED`, `KILLED`.

### Error behavior

- Missing experimental flag: `SUPPORT_IS_DISABLED` (exact code TBD — confirm against
  `src/Common/ErrorCodes.cpp`).
- Destination schema mismatch (columns / types / order; source `EPHEMERAL` column present in
  destination): `INCOMPATIBLE_COLUMNS`.
- Destination engine doesn't support exports (e.g. `url`, unknown engine): `NOT_IMPLEMENTED`.
- Destination is an unknown table function: `UNKNOWN_FUNCTION`.
- Pending mutations or patch parts when the guard is enabled: `BAD_ARGUMENTS` (TBD — confirm).
- Part not found on any replica: `NO_SUCH_DATA_PART` (TBD — confirm).
- Destination file already exists and policy is `error` / `export_merge_tree_part_overwrite_file_if_exists = 0`:
  `FILE_ALREADY_EXISTS` (TBD — confirm).
- Duplicate live manifest without `..._force_export`: `DUPLICATE_EXPORT_TASK` or equivalent
  (TBD — confirm).
- Commit-retry budget exhausted (Iceberg destination): terminal `FAILED` with
  `last_exception` populated; the task is not retried further.
- Task timeout exceeded (`export_merge_tree_partition_task_timeout_seconds`):
  terminal `KILLED` with a timeout reason in `last_exception`.

All of the above **throw exceptions** or surface through `last_exception` on the
task row; they do not crash the server.

### Backward compatibility
- **Older client → newer server:** harmless — the client issues the new `ALTER` text; the server
  parses it. No wire-protocol change.
- **Newer client → older server:** older server fails parse on `EXPORT PART` / `EXPORT PARTITION`
  with `SYNTAX_ERROR`. Acceptable.
- **Mixed-version cluster (replication):** `EXPORT PART` is local to one replica; no cross-replica
  effect. `EXPORT PARTITION` stores its manifest under the table's Keeper path in a new
  subtree; replicas on older versions ignore unknown nodes but will NOT contribute parts to the
  export — the initiating replica (which must be on the new version) completes alone if it holds
  all the parts; otherwise the task stalls on `parts_to_do > 0`. An upgrade-ordering note for
  operators is required (section 4 / Rollout).
- **On-disk format:** unchanged. Parts are read as-is; Parquet is produced on the fly.
- **Default-value changes:** `export_merge_tree_partition_system_table_prefer_remote_information`
  flipped from `true` to `false`. Users running dashboards that read
  `system.replicated_partition_exports` and require always-fresh state must set this
  back to `true` explicitly (and ensure the Keeper `MULTI_READ` feature flag is enabled).
  No other defaults that affect existing workloads (see settings table).

---

## 3. Implementation

This design only covers user visible behavior. It does not internal implementatation 
details. The implementation section is omitted. 

## 4. Test plan

### Functional tests — `tests/queries/0_stateless`

Existing coverage to retain:

- `03572_export_merge_tree_part_basic.sh` — golden path, idempotent re-export, wildcard +
  hive partition strategies.
- `03572_export_merge_tree_part_to_object_storage_simple.sql` — error cases
  (`INCOMPATIBLE_COLUMNS`, `NOT_IMPLEMENTED`, `UNKNOWN_FUNCTION`, `EPHEMERAL` collision).
- `03572_export_merge_tree_part_limits_and_table_functions.sh` — `max_bytes_per_file`,
  `max_rows_per_file`, table-function destination with schema inheritance / explicit structure.
- `03572_export_merge_tree_part_special_columns.sh` — `ALIAS`, `MATERIALIZED`, `EPHEMERAL`,
  mixed / complex expressions.
- `03572_export_replicated_merge_tree_part_to_object_storage.sh` +
  `03572_export_replicated_merge_tree_part_to_object_storage_simple.sql` — part-level export
  from `ReplicatedMergeTree`.
- `03604_export_merge_tree_partition.sh` — basic `EXPORT PARTITION ID`.
- `03608_export_merge_tree_part_filename_pattern.sh` — default and custom
  `export_merge_tree_part_filename_pattern` including `{database}` / `{table}` macros.

New tests to add:

- `NNNN_export_merge_tree_part_pending_mutations.sh` — with/without the
  `..._throw_on_pending_mutations` / `..._throw_on_pending_patch_parts` guards and `IN PARTITION`
  mutations.
- `NNNN_export_merge_tree_part_commit_file.sh` — verify a `commit_<part>_<checksum>` file exists
  alongside every successful export and references every written data file; verify that a
  partial run (simulated by killing before commit) does not produce a commit file.
- `NNNN_export_merge_tree_part_overwrite_policy.sh` — all three values of
  `export_merge_tree_part_file_already_exists_policy` (`skip`, `error`, `overwrite`) plus
  `export_merge_tree_part_overwrite_file_if_exists`.
- `NNNN_export_merge_tree_part_profile_events.sh` — assert `PartsExports`,
  `PartsExportFailures`, `PartsExportDuplicated`, `PartsExportTotalMilliseconds` move as
  expected.

Note: Iceberg-destination coverage is deliberately in integration (next section), not in
`tests/queries/0_stateless`, because it requires a live warehouse and — for the catalog
path — a REST / Glue fixture. The commit-file test above only exercises plain
object-storage atomicity.

Do not add `no-parallel` to any new test unless explicitly required by shared S3 bucket paths;
`03604` currently has the tag and should be re-examined to see whether unique per-run paths
remove the need.

### Integration tests — `tests/integration`

**Keep (modified in this PR):**

- `test_export_merge_tree_part_to_object_storage/` — part export in a multi-node setup.
  PR 1618 makes minor adjustments.
- `test_export_replicated_mt_partition_to_object_storage/` — partition export across
  replicas, including `wait_for_export_status`, retry counting, and replica failure
  scenarios. PR 1618 removes the `s3_retries.xml` config and reshapes several test cases
  against the new shared helpers.

**New in PR 1618:**

- `test_export_merge_tree_part_to_iceberg/` — per-part export to an Iceberg destination,
  covering golden path, sidecar emission, manifest shape, and error paths.
- `test_export_replicated_mt_partition_to_iceberg/` — distributed partition export to
  Iceberg across replicas, including `test_export_task_timeout_kills_stuck_pending_task`
  (uses the `export_partition_commit_always_throw` failpoint to exhaust the commit path,
  then asserts the timeout transitions the task to `KILLED`).
- `test_storage_iceberg_with_spark/test_export_partition_iceberg.py` — catalog-less
  Iceberg round-trip; Spark reads ClickHouse-written data and verifies schema, partition
  layout, and snapshot atomicity.
- `test_storage_iceberg_with_spark/test_export_partition_iceberg_catalog.py` —
  catalog-backed (REST) round-trip; exercises the `commitExportPartitionTransaction`
  path against a real catalog.
- Shared helpers:
  - `tests/integration/helpers/export_partition_helpers.py` — shared
    `wait_for_export_status`, manifest-inspection utilities.
  - `tests/integration/helpers/iceberg_export_stats.py` — sidecar decoders and stats
    assertion helpers.

**Remaining gaps to add:**

- Initiating-replica dies mid-commit (post-data-file-write, pre-catalog-CAS) — asserts a
  surviving replica completes via the Keeper-stashed `metadata.json` snapshot, and the
  `clickhouse.export-partition-transaction-id` idempotency check prevents double-commit.
- Experimental feature disabled on one replica
  (`disable_experimental_export_partition.xml` config) and enabled on the rest — task
  still completes via the enabled replicas.
- `KILL EXPORT PARTITION` against an Iceberg task in mid-commit: status transitions to
  `KILLED`, no dangling half-written `metadata.json`, data files and sidecars remain as
  orphans (cleanup is user responsibility per Operational note 7 in § 2).
- Mixed-version cluster: upgrade scenario where only some replicas know about
  `EXPORT PARTITION` or the new Keeper manifest fields.

Invocation:
`python -m ci.praktika run "integration" --test test_export_merge_tree_part_to_object_storage,test_export_replicated_mt_partition_to_object_storage,test_export_merge_tree_part_to_iceberg,test_export_replicated_mt_partition_to_iceberg,test_storage_iceberg_with_spark`.

### Failpoints

The following failpoints are registered for deterministic testing of crash windows and
retry logic. Enable via `SYSTEM ENABLE FAILPOINT <name>` from test harnesses.

| Failpoint | Kind | What it tests |
| --- | --- | --- |
| `iceberg_writes_non_retry_cleanup` | ONCE | Cleanup path when an Iceberg write fails in a non-retryable way. |
| `iceberg_writes_post_publish_throw` | ONCE | Commit succeeded in object storage but the publish step throws — exercises the recovery path that must not double-commit. |
| `iceberg_export_after_commit_before_zk_completed` | ONCE | Crash window between a successful Iceberg commit and the `COMPLETED` Keeper status update — the idempotency marker (`clickhouse.export-partition-transaction-id`) must prevent a second commit on recovery. |
| `export_partition_commit_always_throw` | REGULAR | Every commit attempt throws — used to exhaust `max_retries` and drive the task-timeout path. |
| `export_partition_status_change_throw` | ONCE | Throws during a manifest status transition — exercises the status-drain lock invariant (decision #11 in § 3) and the manifest-updating task's retry logic. |

These failpoints replace the "simulate a crash before commit" phrasing in the
functional-tests section above; prefer them over process kills for deterministic CI
behavior.

### Performance tests — `tests/performance`

Add `export_merge_tree_part.xml`: compare `ALTER TABLE ... EXPORT PART` vs.
`INSERT INTO s3_t SELECT * FROM mt WHERE _part = ...` on a ~1 GB Wide part; track wall time and
peak memory. Hot path is the Parquet encoder, which warrants a guard against regressions.

- Iceberg-destination benchmark: the Iceberg commit is O(data files), not per-row, so the
  per-part write path performance should match plain object storage within noise. A
  secondary benchmark comparing `EXPORT PARTITION` to Iceberg vs. to hive-layout S3 over
  a ≥1000-part partition would catch regressions in the sidecar / manifest-assembly code
  path specifically.

### Manual verification

- Roundtrip: export partition → read via `SELECT * FROM s3(...)` → create new
  `ReplicatedMergeTree` from the S3 data → row counts / checksums match the source.
- `system.replicated_partition_exports` behaviour under a crashed initiator (cluster restart).
- Object-storage layout inspection via `s3(..., format=One)` listing: exactly N data files + 1
  commit file per transaction.
- Iceberg roundtrip with an external reader: export a partition to an Iceberg destination
  → read it back through the same catalog → row counts and column checksums match the
  source. DuckDB is the preferred external reader here (lightweight, fast to stand up,
  mature Iceberg support); Spark or Trino may be substituted where a specific catalog
  integration needs to be exercised. Confirms the on-disk metadata we write is actually
  interoperable, not just self-consistent.

### Rollout / risk

- **Risk (Keeper schema extension):** the `partition_exports` subtree is write-once; a
  partially-rolled-out cluster where only some replicas understand the subtree — or the
  new manifest fields (`task_timeout_seconds`, `commit_attempts`, Iceberg `metadata.json`
  snapshot, `write_full_path_in_iceberg_metadata`) — will stall partition exports
  (`parts_to_do > 0`) rather than corrupt data. Acceptable but must be documented in the
  upgrade notes.
- **Risk (object-storage cost / accidental large exports):** mitigated by the experimental
  gates (default off) and the manifest idempotency window.
- **Risk (Iceberg catalog manifest retention):** if the catalog reaps old manifest files
  with a retention window shorter than `export_merge_tree_partition_task_timeout_seconds`,
  the rare "sole-node commits, crashes, recovers after reaper deleted the commit manifest"
  scenario can duplicate data. Operators MUST verify their catalog's retention before
  enabling Iceberg destinations (see Operational note 7 in § 2).
- **Risk (default flip on `_prefer_remote_information`):** dashboards that read
  `system.replicated_partition_exports` now see local cached state by default instead of
  Keeper-fresh state. Existing users must set the flag back to `true` explicitly if they
  rely on always-fresh results (and ensure `MULTI_READ` is enabled).
- **Flag strategy:** ship with `allow_experimental_export_merge_tree_part` (query, default
  `false`) and `allow_experimental_export_merge_tree_partition_feature` (server, default
  `false`). Both destination families ride on these gates — no separate Iceberg gate.
  Flip defaults to `true` only after: (a) the open questions in § 3 are resolved, (b) the
  remaining integration gaps listed above are closed, (c) one release cycle of customer
  feedback.
- **Watch in production:**
  - `PartsExportFailures` (existing).
  - `exception_count` on `system.replicated_partition_exports` (existing).
  - Keeper watch counts under the `partition_exports` subtree (existing).
  - Object-storage request-error rates (existing).
  - `commit_attempts` values approaching `max_retries` — signal of a catalog or network
    issue throttling commits.
  - Rate of `KILLED` transitions with timeout reason — signal of tasks stuck on missing
    parts, missing destination, or commit backpressure.
  - For Iceberg destinations: `metadata/` prefix write-error rate (CAS contention,
    vended-credential expiry) and catalog-API error rate.
