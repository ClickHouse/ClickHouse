/// Full reference-page templates keyed by the table name in the `system` database.
/// Keep the live column and catalog data as placeholders; `system.documentation`
/// replaces them when the table is read.
    {"aggregate_function_combinators",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all available aggregate function combinators, which could be applied to aggregate functions and change the way they work.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"aggregated_zookeeper_log",
        {R"DOCS_MD(
## Description {#description}

This table contains aggregated statistics of ZooKeeper operations (e.g. number of operations, average latency, errors) grouped by `(session_id, parent_path, operation, component, is_subrequest)` and periodically flushed to disk.

Unlike [system.zookeeper_log](/reference/system-tables/zookeeper_log) which logs every individual request and response, this table aggregates operations into groups, making it much more lightweight and therefore more suitable for production workloads.

Operations that are part of a `Multi` or `MultiRead` batch are tracked separately via the `is_subrequest` column. Subrequests have zero latency because the total latency is attributed to the enclosing `Multi`/`MultiRead` operation.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See also {#see-also}

- [system.zookeeper_log](/reference/system-tables/zookeeper_log) — Detailed per-request ZooKeeper log.
- [ZooKeeper](/guides/oss/best-practices/tips#zookeeper)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"asynchronous_insert_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about async inserts. Each entry represents an insert query buffered into an async insert query.

To start logging configure parameters in the [asynchronous_insert_log](/reference/settings/server-settings/settings/asynchronous#asynchronous_insert_log) section.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [asynchronous_insert_log](/reference/settings/server-settings/settings/asynchronous#asynchronous_insert_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.asynchronous_insert_log LIMIT 1 \G;
```

```text title="Response"
hostname:                clickhouse.eu-central1.internal
event_date:              2023-06-08
event_time:              2023-06-08 10:08:53
event_time_microseconds: 2023-06-08 10:08:53.199516
query:                   INSERT INTO public.data_guess (user_id, datasource_id, timestamp, path, type, num, str) FORMAT CSV
database:                public
table:                   data_guess
format:                  CSV
query_id:                b46cd4c4-0269-4d0b-99f5-d27668c6102e
bytes:                   133223
exception:
status:                  Ok
flush_time:              2023-06-08 10:08:55
flush_time_microseconds: 2023-06-08 10:08:55.139676
flush_query_id:          cd2c1e43-83f5-49dc-92e4-2fbc7f8d3716
```

## See Also {#see-also}

- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_inserts](/reference/system-tables/asynchronous_inserts) — This table contains information about pending asynchronous inserts in queue.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"asynchronous_inserts",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about pending asynchronous inserts in queue.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.asynchronous_inserts LIMIT 1 \G;
```

```text title="Response"
Row 1:
──────
query:            INSERT INTO public.data_guess (user_id, datasource_id, timestamp, path, type, num, str) FORMAT CSV
database:         public
table:            data_guess
format:           CSV
first_update:     2023-06-08 10:08:54.199606
total_bytes:      133223
entries.query_id: ['b46cd4c4-0269-4d0b-99f5-d27668c6102e']
entries.bytes:    [133223]
```

## See also {#see-also}

- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_insert_log](/reference/system-tables/asynchronous_insert_log) — This table contains information about async inserts performed.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"asynchronous_loader",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information and status for recent asynchronous jobs (e.g. for tables loading). The table contains a row for every job. There is a tool for visualizing information from this table `utils/async_loader_graph`.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
A pending job might be in one of the following states:
- `is_executing` (`UInt8`) - The job is currently being executed by a worker.
- `is_blocked` (`UInt8`) - The job waits for its dependencies to be done.
- `is_ready` (`UInt8`) - The job is ready to be executed and waits for a worker.
- `elapsed` (`Float64`) - Seconds elapsed since start of execution. Zero if job is not started. Total execution time if job finished.

Every job has a pool associated with it and is started in this pool. Each pool has a constant priority and a mutable maximum number of workers. Higher priority (lower `priority` value) jobs are run first. No job with lower priority is started while there is at least one higher priority job ready or executing. Job priority can be elevated (but cannot be lowered) by prioritizing it. For example jobs for a table loading and startup will be prioritized if incoming query required this table. It is possible prioritize a job during its execution, but job is not moved from its `execution_pool` to newly assigned `pool`. The job uses `pool` for creating new jobs to avoid priority inversion. Already started jobs are not preempted by higher priority jobs and always run to completion after start.
- `pool_id` (`UInt64`) - ID of a pool currently assigned to the job.
- `pool` (`String`) - Name of `pool_id` pool.
- `priority` (`Int64`) - Priority of `pool_id` pool.
- `execution_pool_id` (`UInt64`) - ID of a pool the job is executed in. Equals initially assigned pool before execution starts.
- `execution_pool` (`String`) - Name of `execution_pool_id` pool.
- `execution_priority` (`Int64`) - Priority of `execution_pool_id` pool.

- `ready_seqno` (`Nullable(UInt64)`) - Not null for ready jobs. Worker pulls the next job to be executed from a ready queue of its pool. If there are multiple ready jobs, then job with the lowest value of `ready_seqno` is picked.
- `waiters` (`UInt64`) - The number of threads waiting on this job.
- `exception` (`Nullable(String)`) - Not null for failed and canceled jobs. Holds error message raised during query execution or error leading to cancelling of this job along with dependency failure chain of job names.

Time instants during job lifetime:
- `schedule_time` (`DateTime64`) - Time when job was created and scheduled to be executed (usually with all its dependencies).
- `enqueue_time` (`Nullable(DateTime64)`) - Time when job became ready and was enqueued into a ready queue of its pool. Null if the job is not ready yet.
- `start_time` (`Nullable(DateTime64)`) - Time when worker dequeues the job from ready queue and start its execution. Null if the job is not started yet.
- `finish_time` (`Nullable(DateTime64)`) - Time when job execution is finished. Null if the job is not finished yet.

## Example {#example}

```sql
SELECT *
FROM system.asynchronous_loader
LIMIT 1
FORMAT Vertical
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"asynchronous_metric_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains the historical values for `system.asynchronous_metrics`, which are saved once per time interval (one second by default). Enabled by default.

Key-value metrics of `system.asynchronous_metrics` (those broken down per CPU core, block device, network interface, or disk) are logged as one row per key, with the key in the `key` column. For scalar metrics the `key` column is empty.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.asynchronous_metric_log LIMIT 3 \G
```

```text
Row 1:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:07
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0.001

Row 2:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:08
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0

Row 3:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:09
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0
```

**See Also**

- [asynchronous_metric_log setting](/reference/settings/server-settings/settings/asynchronous#asynchronous_metric_log) — Enabling and disabling the setting.
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains metrics, calculated periodically in the background.
- [system.metric_log](/reference/system-tables/metric_log) — Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"asynchronous_metrics",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains metrics that are calculated periodically in the background. For example, the amount of RAM in use.

A metric is either scalar (a single number in the `value` column) or key-value: broken down per entity, such as per CPU core, block device, network interface, or disk. Key-value metrics are represented as a single row with a [Map](/reference/data-types/map) in the `key_values` column (for example, `CPUFrequencyMHz` maps every CPU core number to its frequency), and their `value` column is `NaN`.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT metric, value, key_values
FROM system.asynchronous_metrics
WHERE metric IN ('MemoryResident', 'OSUserTime', 'DiskTotal', 'BlockReadBytes', 'NetworkReceiveBytes')
ORDER BY metric
```

```text
┌─metric──────────────┬─────────────value─┬─key_values──────────────────────────────────────┐
│ BlockReadBytes      │               nan │ {'nvme0n1':67420160}                            │
│ DiskTotal           │               nan │ {'backups':34359738368,'default':5199475388416} │
│ MemoryResident      │         970743808 │ {}                                              │
│ NetworkReceiveBytes │               nan │ {'docker0':0,'ens66':334554363}                 │
│ OSUserTime          │ 32.11807291562506 │ {}                                              │
└─────────────────────┴───────────────────┴─────────────────────────────────────────────────┘
```

A single entity of a key-value metric can be extracted with the map subscript:

```sql
SELECT key_values['default'] AS default_disk_total
FROM system.asynchronous_metrics
WHERE metric = 'DiskTotal'
```

## Metric descriptions {#metric-descriptions}

The descriptions below are generated from the C++ source by `utils/generate-async-metrics-docs`. The single source of truth is the string literal next to each metric registration in `src/Common/AsynchronousMetrics.cpp`, `src/Interpreters/ServerAsynchronousMetrics.cpp`, and `src/Coordination/KeeperAsynchronousMetrics.cpp`. Metric names that include a variable part (currently only the HTTP connection pool group) are shown with a `*name*` placeholder; the running server reports them with the concrete name substituted in.

{/*AUTOGENERATED_METRICS_START*/}
{{ASYNCHRONOUS_METRICS}}
{/*AUTOGENERATED_METRICS_END*/}

**See Also**

- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that have occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"azure_queue_log",
        {R"DOCS_MD(
Contains log entries with information about files processed by the AzureQueue engine.

It is safe to truncate or drop this table at any time.

Columns:

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"azure_queue_metadata",
        {R"DOCS_MD(
## Description {#description}

Contains the current number of processed, processing and failed nodes in keeper for each AzureQueue metadata object and, on demand, their contents. Unlike system.azure_queue_metadata_cache, which shows the in-memory cache, this table reads the state directly from keeper.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"azure_queue_metadata_cache",
        {R"DOCS_MD(
## Description {#description}

Contains in-memory state of AzureQueue metadata and currently processed rows per file.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"azure_queue_settings",
        {R"DOCS_MD(
## Description {#description}

Contains information about settings of [AzureQueue](/reference/engines/table-engines/integrations/azure-queue) tables.
Available from `24.10` server version.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"background_schedule_pool",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about tasks in background schedule pools. Background schedule pools are used for executing periodic tasks such as distributed sends, buffer flushes, message broker operations, streaming queries background jobs, and Iceberg table metadata refresh.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.background_schedule_pool LIMIT 5 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
pool:        distributed
database:    default
table:       data
table_uuid:  00000000-0000-0000-0000-000000000000
query_id:
elapsed_ms:  0
log_name:    BackgroundJobsAssignee:DataProcessing
deactivated: 0
scheduled:   1
delayed:     0
executing:   0
```

## See also {#see-also}

- [system.background_schedule_pool_log](/reference/system-tables/background_schedule_pool_log) — Contains history of background schedule pool task executions.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"background_schedule_pool_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

The `system.background_schedule_pool_log` table is created only if the [background_schedule_pool_log](/reference/settings/server-settings/settings/background-schedule#background_schedule_pool_log) server setting is specified.

This table contains the history of background schedule pool task executions. Background schedule pools are used for executing periodic tasks such as distributed sends, buffer flushes, and message broker operations.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
The `system.background_schedule_pool_log` table is created after the first background task execution.

## Example {#example}

```sql
SELECT * FROM system.background_schedule_pool_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-12-18
event_time:              2025-12-18 10:30:15
event_time_microseconds: 2025-12-18 10:30:15.123456
query_id:
database:                default
table:                   data
table_uuid:              00000000-0000-0000-0000-000000000000
log_name:                default.data
duration_ms:             42
error:                   0
exception:
```

## See also {#see-also}

- [system.background_schedule_pool](/reference/system-tables/background_schedule_pool) — Contains information about currently scheduled tasks in background schedule pools.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"backup_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains logging entries with information about `BACKUP` and `RESTORE` operations.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
BACKUP TABLE test_db.my_table TO Disk('backups_disk', '1.zip')
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```

```sql
SELECT hostname, event_date, event_time_microseconds, id, name, status, error, start_time, end_time, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read FROM system.backup_log WHERE id = 'e5b74ecb-f6f1-426a-80be-872f90043885' ORDER BY event_date, event_time_microseconds \G
```

```response
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time_microseconds: 2023-08-19 11:05:21.998566
id:                      e5b74ecb-f6f1-426a-80be-872f90043885
name:                    Disk('backups_disk', '1.zip')
status:                  CREATING_BACKUP
error:
start_time:              2023-08-19 11:05:21
end_time:                1970-01-01 03:00:00
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

Row 2:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time:              2023-08-19 11:08:56
event_time_microseconds: 2023-08-19 11:08:56.916192
id:                      e5b74ecb-f6f1-426a-80be-872f90043885
name:                    Disk('backups_disk', '1.zip')
status:                  BACKUP_CREATED
error:
start_time:              2023-08-19 11:05:21
end_time:                2023-08-19 11:08:56
num_files:               57
total_size:              4290364870
num_entries:             46
uncompressed_size:       4290362365
compressed_size:         3525068304
files_read:              0
bytes_read:              0
```
```sql
RESTORE TABLE test_db.my_table FROM Disk('backups_disk', '1.zip')
```

```response
┌─id───────────────────────────────────┬─status───┐
│ cdf1f731-52ef-42da-bc65-2e1bfcd4ce90 │ RESTORED │
└──────────────────────────────────────┴──────────┘
```

```sql
SELECT hostname, event_date, event_time_microseconds, id, name, status, error, start_time, end_time, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read FROM system.backup_log WHERE id = 'cdf1f731-52ef-42da-bc65-2e1bfcd4ce90' ORDER BY event_date, event_time_microseconds \G
```

```response
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time_microseconds: 2023-08-19 11:09:19.718077
id:                      cdf1f731-52ef-42da-bc65-2e1bfcd4ce90
name:                    Disk('backups_disk', '1.zip')
status:                  RESTORING
error:
start_time:              2023-08-19 11:09:19
end_time:                1970-01-01 03:00:00
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

Row 2:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time_microseconds: 2023-08-19 11:09:29.334234
id:                      cdf1f731-52ef-42da-bc65-2e1bfcd4ce90
name:                    Disk('backups_disk', '1.zip')
status:                  RESTORED
error:
start_time:              2023-08-19 11:09:19
end_time:                2023-08-19 11:09:29
num_files:               57
total_size:              4290364870
num_entries:             46
uncompressed_size:       4290362365
compressed_size:         4290362365
files_read:              57
bytes_read:              4290364870
```

This is essentially the same information that is written in the system table `system.backups`:

```sql
SELECT id, name, status, error, start_time, end_time, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read FROM system.backups ORDER BY start_time
```

```response
┌─id───────────────────────────────────┬─name──────────────────────────┬─status─────────┬─error─┬──────────start_time─┬────────────end_time─┬─num_files─┬─total_size─┬─num_entries─┬─uncompressed_size─┬─compressed_size─┬─files_read─┬─bytes_read─┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ Disk('backups_disk', '1.zip') │ BACKUP_CREATED │       │ 2023-08-19 11:05:21 │ 2023-08-19 11:08:56 │        57 │ 4290364870 │          46 │        4290362365 │      3525068304 │          0 │          0 │
│ cdf1f731-52ef-42da-bc65-2e1bfcd4ce90 │ Disk('backups_disk', '1.zip') │ RESTORED       │       │ 2023-08-19 11:09:19 │ 2023-08-19 11:09:29 │        57 │ 4290364870 │          46 │        4290362365 │      4290362365 │         57 │ 4290364870 │
└──────────────────────────────────────┴───────────────────────────────┴────────────────┴───────┴─────────────────────┴─────────────────────┴───────────┴────────────┴─────────────┴───────────────────┴─────────────────┴────────────┴────────────┘
```

## See Also {#see-also}

- [Backup and Restore](/concepts/features/backup-restore/overview)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"backups",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all `BACKUP` or `RESTORE` operations with their current states and other properties. Note, that table is not persistent and it shows only operations executed after the last server restart.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Restore atomicity {#restore-atomicity}

`RESTORE` is not transactional and does not roll back on failure. For each table, all selected parts are copied before any are attached, but the attach phase itself is not transactional — parts are made visible one at a time. Tables are processed independently.

**Tables are independent.** A table whose restore completes stays in place even if another table in the same command later fails:

```sql
RESTORE TABLE db.t0, TABLE db.t1
FROM S3('<endpoint>', '<access_key>', '<secret_key>')
SETTINGS
    allow_non_empty_tables = true;
```

If this command fails after `db.t0` has been fully restored but `db.t1` has not finished, `db.t0` remains restored.

**The `PARTITIONS` clause is not a commit boundary.** It only selects which parts of a table are restored:

```sql
RESTORE TABLE db.t0 PARTITIONS '2026-06-01', '2026-06-02', '2026-06-03'
FROM S3('<endpoint>', '<access_key>', '<secret_key>')
SETTINGS
    allow_non_empty_tables = true;
```

All selected parts of the table are copied first and attached only once every one of them is ready. So if this command fails during the copy phase — e.g. after partition `2026-06-01` has been fully copied but `2026-06-02` and `2026-06-03` have not finished — then `2026-06-01` is **not** committed and the table is left with no restored data from this command. Once the copy phase completes and the attach step begins, parts are committed one at a time, so a failure during attach can leave the table partially restored, without rollback.

To commit partitions independently (so a completed partition survives a later failure and can be retried in isolation), run a separate `RESTORE` per partition, using `SETTINGS allow_non_empty_tables = true` after the first.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"blob_storage_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains logging entries with information about various blob storage operations such as uploads and deletes.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

Suppose a blob storage operation uploads a file, and an event is logged:

```sql
SELECT * FROM system.blob_storage_log WHERE query_id = '7afe0450-504d-4e4b-9a80-cd9826047972' ORDER BY event_date, event_time_microseconds \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-10-31
event_time:              2023-10-31 16:03:40
event_time_microseconds: 2023-10-31 16:03:40.481437
event_type:              Upload
query_id:                7afe0450-504d-4e4b-9a80-cd9826047972
thread_id:               2381740
disk_name:               disk_s3
bucket:                  bucket1
remote_path:             rrr/kxo/tbnqtrghgtnxkzgtcrlutwuslgawe
local_path:              store/654/6549e8b3-d753-4447-8047-d462df6e6dbe/tmp_insert_all_1_1_0/checksums.txt
data_size:               259
error:
```

In this example, upload operation was associated with the `INSERT` query with ID `7afe0450-504d-4e4b-9a80-cd9826047972`. The local metadata file `store/654/6549e8b3-d753-4447-8047-d462df6e6dbe/tmp_insert_all_1_1_0/checksums.txt` refers to remote path `rrr/kxo/tbnqtrghgtnxkzgtcrlutwuslgawe` in bucket `bucket1` on disk `disk_s3`, with a size of 259 bytes.

## See also {#see-also}

- [External Disks for Storing Data](/concepts/features/configuration/server-config/storing-data)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"build_options",
        {R"DOCS_MD(
## Description {#description}

Contains information about the ClickHouse server's build options.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.build_options LIMIT 5
```

```text
┌─name─────────────┬─value─┐
│ USE_BROTLI       │ 1     │
│ USE_BZIP2        │ 1     │
│ USE_CAPNP        │ 1     │
│ USE_CASSANDRA    │ 1     │
│ USE_DATASKETCHES │ 1     │
└──────────────────┴───────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"certificates",
        {R"DOCS_MD(
## Description {#description}

Contains information about available certificates and their sources.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"clusters",
        {R"DOCS_MD(
## Description {#description}

Contains information about clusters available in the config file and the servers in them.

## Columns {#Columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.clusters LIMIT 2 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_cluster_two_shards
shard_num:               1
shard_name:              shard_01
shard_weight:            1
replica_num:             1
host_name:               127.0.0.1
host_address:            127.0.0.1
port:                    9000
is_local:                1
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
database_shard_name:
database_replica_name:
is_active:               NULL

Row 2:
──────
cluster:                 test_cluster_two_shards
shard_num:               2
shard_name:              shard_02
shard_weight:            1
replica_num:             1
host_name:               127.0.0.2
host_address:            127.0.0.2
port:                    9000
is_local:                0
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
database_shard_name:
database_replica_name:
is_active:               NULL
```

## See also {#see-also}

- [Table engine Distributed](/reference/engines/table-engines/special/distributed)
- [distributed_replica_error_cap setting](/reference/settings/session-settings/distributed-replica#distributed_replica_error_cap)
- [distributed_replica_error_half_life setting](/reference/settings/session-settings/distributed-replica#distributed_replica_error_half_life)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"codecs",
        {R"DOCS_MD(
## Description {#description}

Contains information about compression and encryption codecs.

You can use this table to get information about the available compression and encryption codecs

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.codecs WHERE name='LZ4'
```

```text title="Response"
Row 1:
──────
name:                   LZ4
method_byte:            130
is_compression:         1
is_generic_compression: 1
is_encryption:          0
is_timeseries_codec:    0
is_experimental:        0
description:            Extremely fast; good compression; balanced speed and efficiency.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"collations",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all available collations for alphabetical comparison of strings.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"columns",
        {R"DOCS_MD(
## Description {#description}

Contains information about columns in all tables.

You can use this table to get information similar to the [DESCRIBE TABLE](/reference/statements/describe-table) query, but for multiple tables at once.

Columns from [temporary tables](/reference/statements/create/table/temporary-table) are visible in the `system.columns` only in those session where they have been created. They are shown with the empty `database` field.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.columns LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_catalog
type:                    String
position:                1
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ

Row 2:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_schema
type:                    String
position:                2
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"completions",
        {R"DOCS_MD(
## Description {#description}

Contains a list of completion tokens.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"constraints",
        {R"DOCS_MD(
## Description {#description}

Contains information about existing constraints in all tables.

Constraints defined on [temporary tables](/reference/statements/create/table/temporary-table) are visible in `system.constraints` only in the session where they were created. They are shown with an empty `database` field.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.constraints LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       hits
name:        check_hits
type:        CHECK
expression:  CounterID > 0

Row 2:
──────
database:    default
table:       hits
name:        assume_positive
type:        ASSUME
expression:  WatchID > 0
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"contributors",
        {R"DOCS_MD(
## Description {#description}

Contains information about contributors. The order is random at query execution time.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.contributors LIMIT 10
```

```text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

To find out yourself in the table, use a query:

```sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

```text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"crash_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about stack traces for fatal errors. The table does not exist in the database by default, it is created only when fatal errors occur.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.crash_log ORDER BY event_time DESC LIMIT 1;
```

```text title="Response"
Row 1:
──────
hostname:     clickhouse.eu-central1.internal
event_date:   2020-10-14
event_time:   2020-10-14 15:47:40
timestamp_ns: 1602679660271312710
signal:       11
thread_id:    23624
query_id:     428aab7c-8f5c-44e9-9607-d16b44467e69
trace:        [188531193,...]
trace_full:   ['3. DB::(anonymous namespace)::FunctionFormatReadableTimeDelta::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> >&, std::__1::vector<unsigned long, std::__1::allocator<unsigned long> > const&, unsigned long, unsigned long) const @ 0xb3cc1f9 in /home/username/work/ClickHouse/build/programs/clickhouse',...]
version:      ClickHouse 20.11.1.1
revision:     54442
build_id:
```

## See also {#see-also}

- [trace_log](/reference/system-tables/trace_log) system table
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"current_roles",
        {R"DOCS_MD(
## Description {#description}

Contains active roles of a current user. `SET ROLE` changes the contents of this table.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dashboards",
        {R"DOCS_MD(
## Description {#description}

Contains queries used by `/dashboard` page accessible though [HTTP interface](/concepts/features/interfaces/http).
This table can be useful for monitoring and troubleshooting. The table contains a row for every chart in a dashboard.

<Note>
`/dashboard` page can render queries not only from `system.dashboards`, but from any table with the same schema.
This can be useful to create custom dashboards.
</Note>

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT *
FROM system.dashboards
WHERE title ILIKE '%CPU%'
```

```text
Row 1:
──────
dashboard: overview
title:     CPU Usage (cores)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000
FROM system.metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 2:
──────
dashboard: overview
title:     CPU Wait
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000
FROM system.metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 3:
──────
dashboard: overview
title:     OS CPU Usage (Userspace)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM system.asynchronous_metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 4:
──────
dashboard: overview
title:     OS CPU Usage (Kernel)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM system.asynchronous_metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"data_skipping_index_types",
        {R"DOCS_MD(
## Description {#description}

Contains the list of data skipping index types supported by the server, along with embedded documentation for each type. A data skipping index type is specified in the `TYPE` of an `INDEX` declaration in a `CREATE TABLE` query and lets ClickHouse skip granules that cannot match a query's condition.

Note that this table lists the available index *types*, whereas [`system.data_skipping_indices`](/reference/system-tables/data_skipping_indices) lists the index instances defined on existing tables.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT name, syntax
FROM system.data_skipping_index_types
WHERE name IN ('minmax', 'set')
ORDER BY name
```

```text title="Response"
┌─name───┬─syntax───────────────────────────────────────┐
│ minmax │ INDEX name expr TYPE minmax GRANULARITY n     │
│ set    │ INDEX name expr TYPE set(max_rows) GRANULARITY n │
└────────┴──────────────────────────────────────────────┘
```

## See also {#see-also}

- [Data skipping indices](/concepts/features/performance/skip-indexes/skipping-indexes) — Information about data skipping indexes.
- [`system.data_skipping_indices`](/reference/system-tables/data_skipping_indices) — The index instances defined on existing tables.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"data_skipping_indices",
        {R"DOCS_MD(
## Description {#description}

Contains information about existing data skipping indices in all the tables.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.data_skipping_indices LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                default
table:                   user_actions
name:                    clicks_idx
type:                    minmax
type_full:               minmax
expr:                    clicks
creation:                Explicit
granularity:             1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks_bytes:             48

Row 2:
──────
database:                default
table:                   users
name:                    contacts_null_idx
type:                    minmax
type_full:               minmax
expr:                    assumeNotNull(contacts_null)
creation:                Explicit
granularity:             1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks_bytes:             48
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"data_type_families",
        {R"DOCS_MD(
## Description {#description}

Contains information about supported [data types](/reference/data-types/index).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT name, case_insensitive, alias_to FROM system.data_type_families WHERE alias_to = 'String'
```

```text
┌─name───────┬─case_insensitive─┬─alias_to─┐
│ LONGBLOB   │                1 │ String   │
│ LONGTEXT   │                1 │ String   │
│ TINYTEXT   │                1 │ String   │
│ TEXT       │                1 │ String   │
│ VARCHAR    │                1 │ String   │
│ MEDIUMBLOB │                1 │ String   │
│ BLOB       │                1 │ String   │
│ TINYBLOB   │                1 │ String   │
│ CHAR       │                1 │ String   │
│ MEDIUMTEXT │                1 │ String   │
└────────────┴──────────────────┴──────────┘
```

## See also {#see-also}

- [Syntax](/reference/syntax) — Information about supported syntax.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"database_engines",
        {R"DOCS_MD(
## Description {#description}

Contains the list of database engines supported by the server.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT name
FROM system.database_engines
WHERE name IN ('Atomic', 'Ordinary')
ORDER BY name
```

```text title="Response"
┌─name─────┐
│ Atomic   │
│ Ordinary │
└──────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"database_replicas",
        {R"DOCS_MD(
## Description {#description}

Contains information of each Replicated database replicas.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.database_replicas FORMAT Vertical;
```

```text
Row 1:
──────
database:            db_2
is_readonly:         0
max_log_ptr:         2
replica_name:        replica1
replica_path:        /test/db_2/replicas/shard1|replica1
zookeeper_path:      /test/db_2
shard_name:          shard1
log_ptr:             2
total_replicas:      1
zookeeper_exception:
is_session_expired:  0
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"databases",
        {R"DOCS_MD(
## Description {#description}

Contains information about the databases that are available to the current user.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
The `name` column from this system table is used for implementing the `SHOW DATABASES` query.

## Example {#example}

Create a database.

```sql title="Query"
CREATE DATABASE test;
```

Check all of the available databases to the user.

```sql title="Query"
SELECT * FROM system.databases;
```

```text title="Response"
┌─name────────────────┬─engine─────┬─data_path────────────────────┬─metadata_path─────────────────────────────────────────────────────────┬─uuid─────────────────────────────────┬─engine_full────────────────────────────────────────────┬─comment─┐
│ INFORMATION_SCHEMA  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ default             │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/f97/f97a3ceb-2e8a-4912-a043-c536e826a4d4/ │ f97a3ceb-2e8a-4912-a043-c536e826a4d4 │ Atomic                                                 │         │
│ information_schema  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ replicated_database │ Replicated │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/da8/da85bb71-102b-4f69-9aad-f8d6c403905e/ │ da85bb71-102b-4f69-9aad-f8d6c403905e │ Replicated('some/path/database', 'shard1', 'replica1') │         │
│ system              │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/b57/b5770419-ac7a-4b67-8229-524122024076/ │ b5770419-ac7a-4b67-8229-524122024076 │ Atomic                                                 │         │
│ test                │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/2a1/2a1b3c4d-5e6f-7890-abcd-ef1234567890/ │ 2a1b3c4d-5e6f-7890-abcd-ef1234567890 │ Atomic                                                 │         │
└─────────────────────┴────────────┴──────────────────────────────┴───────────────────────────────────────────────────────────────────────┴──────────────────────────────────────┴────────────────────────────────────────────────────────┴─────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dead_letter_queue",
        {R"DOCS_MD(
## Description {#description}

Contains information about messages received via a streaming engine and parsed with errors. Currently implemented for Kafka and RabbitMQ.

Logging is enabled by specifying `dead_letter_queue` for the engine specific `handle_error_mode` setting.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [dead_letter_queue](/reference/settings/server-settings/settings/other#dead_letter_queue) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.dead_letter_queue LIMIT 1 \G;
```

```text title="Response"
Row 1:
──────
table_engine:                  Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.910773
database:                      default
table:                         kafka
error:                         Cannot parse input: expected '\t' before: 'qwertyuiop': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "qwertyuiop" is not like UInt64
raw_message:                   qwertyuiop
kafka_topic_name:              TSV_dead_letter_queue_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:

Row 2:
──────
table_engine:                  Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.910944
database:                      default
table:                         kafka
error:                         Cannot parse input: expected '\t' before: 'asdfghjkl': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "asdfghjkl" is not like UInt64
raw_message:                   asdfghjkl
kafka_topic_name:              TSV_dead_letter_queue_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:

Row 3:
──────
table_engine:                  Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.911092
database:                      default
table:                         kafka
error:                         Cannot parse input: expected '\t' before: 'zxcvbnm': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "zxcvbnm" is not like UInt64
raw_message:                   zxcvbnm
kafka_topic_name:              TSV_dead_letter_queue_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:
 (test.py:78, dead_letter_queue_test)

```

## See also {#see-also}

- [Kafka](/reference/engines/table-engines/integrations/kafka) - Kafka Engine
- [system.kafka_consumers](/reference/system-tables/kafka_consumers) — Description of the `kafka_consumers` system table which contains information like statistics and errors about Kafka consumers.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"delta_lake_metadata_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#purpose}

The `system.delta_lake_metadata_log` table records metadata access and parsing events for Delta Lake tables read by ClickHouse. It provides detailed information about each metadata file, which is useful for debugging, auditing, and understanding Delta table structure evolution.

This table logs every metadata file read from Delta Lake tables. It helps users trace how ClickHouse interprets Delta table metadata and diagnose issues related to schema evolution, snapshot resolution, or query planning.

<Note>
This table is primarily intended for debugging purposes.
</Note>

### Controlling log verbosity {#controlling-log-verbosity}

You can control which metadata events are logged using the [`delta_lake_log_metadata`](/reference/settings/session-settings/delta-lake#delta_lake_log_metadata) setting.

To log all metadata used in the current query:

```sql
SELECT * FROM my_delta_table SETTINGS delta_lake_log_metadata = 1;

SYSTEM FLUSH LOGS delta_lake_metadata_log;

SELECT *
FROM system.delta_lake_metadata_log
WHERE query_id = '{previous_query_id}';
```

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"detached_parts",
        {R"DOCS_MD(
## Description {#purpose}

Contains information about detached parts of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables. The `reason` column specifies why the part was detached.

For user-detached parts, the reason is empty. Such parts can be attached with [ALTER TABLE ATTACH PARTITION\|PART](/reference/statements/alter/partition#attach-partitionpart) command.

For the description of other columns, see [system.parts](/reference/system-tables/parts).

If part name is invalid, values of some columns may be `NULL`. Such parts can be deleted with [ALTER TABLE DROP DETACHED PART](/reference/statements/alter/partition#drop-detached-partitionpart).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"detached_tables",
        {R"DOCS_MD(
## Description {#description}

Contains information about each detached table.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.detached_tables FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
table:                      t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
is_permanently:             1
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dictionaries",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about [dictionaries](/reference/statements/create/dictionary).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

Configure the dictionary:

```sql
CREATE DICTIONARY dictionary_with_comment
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'The temporary dictionary';
```

Make sure that the dictionary is loaded.

```sql
SELECT * FROM system.dictionaries LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:                    default
name:                        dictionary_with_comment
uuid:                        4654d460-0d03-433a-8654-d4600d03d33a
status:                      NOT_LOADED
origin:                      4654d460-0d03-433a-8654-d4600d03d33a
type:
key.names:                   ['id']
key.types:                   ['UInt64']
attribute.names:             ['value']
attribute.types:             ['String']
bytes_allocated:             0
query_count:                 0
hit_rate:                    0
found_rate:                  0
element_count:               0
load_factor:                 0
source:
lifetime_min:                0
lifetime_max:                0
loading_start_time:          1970-01-01 00:00:00
last_successful_update_time: 1970-01-01 00:00:00
loading_duration:            0
last_exception:
comment:                     The temporary dictionary
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dictionary_layouts",
        {R"DOCS_MD(
## Description {#description}

Contains the list of dictionary layouts supported by the server, along with embedded documentation for each layout. A dictionary layout determines how a dictionary is stored in memory (or on disk) and how it is looked up; it is specified in the `LAYOUT` clause of a `CREATE DICTIONARY` query.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT name, is_complex, syntax
FROM system.dictionary_layouts
WHERE name IN ('flat', 'hashed', 'complex_key_hashed')
ORDER BY name
```

```text title="Response"
┌─name───────────────┬─is_complex─┬─syntax───────────────────────────────────────────────────┐
│ complex_key_hashed │          1 │ LAYOUT(COMPLEX_KEY_HASHED())                             │
│ flat               │          0 │ LAYOUT(FLAT([INITIAL_ARRAY_SIZE n] [MAX_ARRAY_SIZE n]))  │
│ hashed             │          0 │ LAYOUT(HASHED())                                         │
└────────────────────┴────────────┴──────────────────────────────────────────────────────────┘
```

## See also {#see-also}

- [Dictionary layouts](/reference/statements/create/dictionary/layouts/overview) — Information about dictionaries and their layouts.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dictionary_sources",
        {R"DOCS_MD(
## Description {#description}

Contains the list of dictionary sources supported by the server, along with embedded documentation for each source. A dictionary source determines where the dictionary data is loaded from; it is specified in the `SOURCE` clause of a `CREATE DICTIONARY` query.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT name, syntax
FROM system.dictionary_sources
WHERE name IN ('clickhouse', 'file')
ORDER BY name
```

```text title="Response"
┌─name───────┬─syntax─────────────────────────────────────────────────────────────────────────┐
│ clickhouse │ SOURCE(CLICKHOUSE(host 'host' port 9000 user 'default' password '' db 'db' table 'table')) │
│ file       │ SOURCE(FILE(path '/path/to/file' format 'CSV'))                                  │
└────────────┴────────────────────────────────────────────────────────────────────────────────┘
```

## See also {#see-also}

- [Dictionary sources](/reference/statements/create/dictionary/sources/overview) — Information about dictionaries and their sources.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dimensional_metrics",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table contains dimensional metrics that can be calculated instantly and exported in the Prometheus format. It is always up to date.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

You can use a query like this to export all the dimensional metrics in the Prometheus format.
```sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'gauge' AS type
FROM system.dimensional_metrics
FORMAT Prometheus
```

## Metric descriptions {#metric_descriptions}

### merge_failures {#merge_failures}
Number of all failed merges since startup.

### startup_scripts_failure_reason {#startup_scripts_failure_reason}
Indicates startup scripts failures by error type. Set to 1 when a startup script fails, labelled with the error name.

### merge_tree_parts {#merge_tree_parts}
Number of merge tree data parts, labelled by part state, part type, and whether it is a projection part.

### `filesystem_cache_evictions_total` {#filesystem-cache-evictions-total}
Number of file segments evicted from a filesystem cache, labelled by cache name. Disabled by default; enable with `expose_prometheus_eviction_metrics`.

### `filesystem_cache_evicted_bytes_total` {#filesystem-cache-evicted-bytes-total}
Total bytes of file segments evicted from a filesystem cache, labelled by cache name. Disabled by default; enable with `expose_prometheus_eviction_metrics`.

### `filesystem_cache_evictions_by_user_total` {#filesystem-cache-evictions-by-user-total}
Number of file segments evicted from a filesystem cache, labelled by cache name and user id. Disabled by default; enable with `expose_prometheus_eviction_metrics` and `expose_prometheus_eviction_metrics_per_user`.

### `filesystem_cache_evicted_bytes_by_user_total` {#filesystem-cache-evicted-bytes-by-user-total}
Total bytes of file segments evicted from a filesystem cache, labelled by cache name and user id. Disabled by default; enable with `expose_prometheus_eviction_metrics` and `expose_prometheus_eviction_metrics_per_user`.

### `object_storage_queue_failures_total` {#object-storage-queue-failures-total}
Number of `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) failures, labelled by database, table, processing stage (`read`, `set_processing`, `insert`, `commit`) and error code.

### `object_storage_queue_permanently_failed_files_total` {#object-storage-queue-permanently-failed-files-total}
Number of `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) files given up on for good after exhausting retries (or with retries disabled), labelled by database and table. Each of these represents a file whose data will never be processed.

### `object_storage_queue_newest_seen_object_timestamp_seconds` {#object-storage-queue-newest-seen-object-timestamp-seconds}
Unix timestamp of the last-modified time of the newest object seen so far by an `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) table, labelled by database and table.

### `object_storage_queue_newest_committed_object_timestamp_seconds` {#object-storage-queue-newest-committed-object-timestamp-seconds}
Unix timestamp of the last-modified time of the newest object fully processed so far by an `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) table, labelled by database and table.

## See also {#see-also}

- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"disk_types",
        {R"DOCS_MD(
## Description {#description}

Contains the list of disk types supported by the server, along with embedded documentation for each type. A disk type is specified in the `type` of a disk configuration and determines where and how a disk stores its data (local filesystem, object storage, a cache over another disk, and so on).

Note that this table lists the available disk *types*, whereas [`system.disks`](/reference/system-tables/disks) lists the disk instances configured on the server.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Configuration examples {#configuration-examples}

A disk can be configured in two ways: **statically**, in the server configuration files (XML or YAML), or **dynamically**, in the settings of a `CREATE`/`ATTACH` query using the `disk` function. The same disk type and parameters are accepted in both cases.

### Static configuration {#static-configuration}

Disks are defined under `storage_configuration` in the server configuration. The following example defines an `s3` disk and a storage policy that uses it.

```xml title="config.xml"
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_disk>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3_disk>
        </disks>
        <policies>
            <s3_policy>
                <volumes>
                    <main>
                        <disk>s3_disk</disk>
                    </main>
                </volumes>
            </s3_policy>
        </policies>
    </storage_configuration>
</clickhouse>
```

The same configuration in YAML:

```yaml title="config.yaml"
storage_configuration:
  disks:
    s3_disk:
      type: s3
      endpoint: https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/
      use_environment_credentials: 1
  policies:
    s3_policy:
      volumes:
        main:
          disk: s3_disk
```

A table can then use the disk through its storage policy:

```sql title="Query"
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3_policy';
```

### Dynamic configuration {#dynamic-configuration}

A disk can also be defined directly in the settings of a `CREATE`/`ATTACH` query, without a predefined disk in the configuration files, using the `disk` function:

```sql title="Query"
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    type = s3,
    endpoint = 'https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/',
    use_environment_credentials = 1
);
```

See [Configuring external storage](/concepts/features/configuration/server-config/storing-data) for the full list of parameters of each disk type.

## Example {#example}

```sql title="Query"
SELECT name, description
FROM system.disk_types
WHERE name IN ('local', 'object_storage')
ORDER BY name
```

## See also {#see-also}

- [`system.disks`](/reference/system-tables/disks) — The disk instances configured on the server.
- [`system.storage_policies`](/reference/system-tables/storage_policies) — Storage policies and volumes.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"disks",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about disks defined in the [server configuration](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes_configure).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.disks;
```

```response
┌─name────┬─path─────────────────┬───free_space─┬──total_space─┬─keep_free_space─┐
│ default │ /var/lib/clickhouse/ │ 276392587264 │ 490652508160 │               0 │
└─────────┴──────────────────────┴──────────────┴──────────────┴─────────────────┘

1 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"distributed_ddl_queue",
        {R"DOCS_MD(
## Description {#description}

Contains information about [distributed ddl queries (ON CLUSTER clause)](/reference/statements/distributed-ddl) that were executed on a cluster.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT *
FROM system.distributed_ddl_queue
WHERE cluster = 'test_cluster'
LIMIT 2
FORMAT Vertical

Query id: f544e72a-6641-43f1-836b-24baa1c9632a

Row 1:
──────
entry:             query-0000000000
entry_version:     5
initiator_host:    clickhouse01
initiator_port:    9000
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
settings:          {'max_threads':'16','use_uncompressed_cache':'0'}
query_create_time: 2023-09-01 16:15:14
host:              clickhouse-01
port:              9000
status:            Finished
exception_code:    0
exception_text:
query_finish_time: 2023-09-01 16:15:14
query_duration_ms: 154

Row 2:
──────
entry:             query-0000000001
entry_version:     5
initiator_host:    clickhouse01
initiator_port:    9000
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
settings:          {'max_threads':'16','use_uncompressed_cache':'0'}
query_create_time: 2023-09-01 16:15:14
host:              clickhouse-01
port:              9000
status:            Finished
exception_code:    630
exception_text:    Code: 630. DB::Exception: Cannot drop or rename test_db, because some tables depend on it:
query_finish_time: 2023-09-01 16:15:14
query_duration_ms: 154

2 rows in set. Elapsed: 0.025 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"distribution_queue",
        {R"DOCS_MD(
## Description {#description}

Contains information about local files that are in the queue to be sent to the shards. These local files contain new parts that are created by inserting new data into the Distributed table in asynchronous mode.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.distribution_queue LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:              default
table:                 dist
data_path:             ./store/268/268bc070-3aad-4b1a-9cf2-4987580161af/default@127%2E0%2E0%2E2:9000/
is_blocked:            1
error_count:           0
data_files:            1
data_compressed_bytes: 499
last_exception:
```

## See also {#see-also}

- [Distributed table engine](/reference/engines/table-engines/special/distributed)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dns_cache",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about cached DNS records.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.dns_cache;
```

| hostname | ip\_address | ip\_family | cached\_at |
| :--- | :--- | :--- | :--- |
| localhost | ::1 | IPv6 | 2024-02-11 17:04:40 |
| localhost | 127.0.0.1 | IPv4 | 2024-02-11 17:04:40 |

## See also {#see-also}

- [disable_internal_dns_cache setting](/reference/settings/server-settings/settings/disable#disable_internal_dns_cache)
- [dns_cache_max_entries setting](/reference/settings/server-settings/settings/dns-cache#dns_cache_max_entries)
- [dns_cache_update_period setting](/reference/settings/server-settings/settings/dns-cache#dns_cache_update_period)
- [dns_max_consecutive_failures setting](/reference/settings/server-settings/settings/other#dns_max_consecutive_failures)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"documentation",
        {R"DOCS_MD(
## Description {#description}

Collects the embedded documentation of the uniform components of the system into a single table. Every row corresponds to one entity (a function, a table engine, a data type, and so on) and contains the reference documentation of that entity rendered as Markdown — the same content that is published on the website and exposed by the per-kind `system.*` tables.

The `description` is assembled from the structured parts of the embedded documentation (`description`, `syntax`, arguments, examples, and so on), so a single column holds the complete documentation of an entity. When the embedded documentation carries the entire reference page in its `description` (as is the case for the components whose website pages are autogenerated from it: table engines, database engines, data types, formats, table functions), the page is published as-is, without appending sections composed from the structured metadata fields — the page body already covers that material. Aliases are rendered as a short reference to the canonical entity, e.g. ``Alias of `trunc`.``

This table, in a certain way, collects the information available in the per-kind documentation tables ([`system.functions`](/reference/system-tables/functions), [`system.table_engines`](/reference/system-tables/table_engines), [`system.data_type_families`](/reference/system-tables/data_type_families), and others). It is meant, in particular, to back an interactive `help` command in the client, but is useful on its own.

The following kinds of entities are collected (the value of the `type` column is shown in parentheses):

- Functions (`Function`)
- Aggregate functions (`Aggregate Function`)
- Table functions (`Table Function`)
- Table engines (`Table Engine`)
- Database engines (`Database Engine`)
- Data types (`Data Type`)
- Dictionary layouts (`Dictionary Layout`)
- Dictionary sources (`Dictionary Source`)
- Aggregate function combinators (`Aggregate Function Combinator`)
- Data skipping index types (`Data Skipping Index`)
- Disk types (`Disk Type`)
- Settings (`Setting`)
- MergeTree settings (`MergeTree Setting`)
- Server settings (`Server Setting`)
- Formats (`Format`)
- Compression codecs (`Compression Codec`)
- Profile events (`Profile Event`)
- Current metrics (`Current Metric`)
- Asynchronous metrics (`Asynchronous Metric`)
- System tables (`System Table`)
- SQL statements (`Statement`)

For settings (of any kind), the documentation is the setting's description, together with its type and default value; obsolete settings are not exposed. It also carries the history of the changes of the setting's default value across ClickHouse versions: the version in which the setting was introduced and every later change of its default, with the previous value, the new value and the reason for the change. This is the same data that backs the `compatibility` setting and [`system.settings_changes`](/reference/system-tables/settings_changes), so it covers the changes recorded since that mechanism was introduced: an older setting whose default never changed has no history, and neither do server settings, which `compatibility` does not cover. A change recorded under an alias of a setting belongs to the history of that setting, the same way `compatibility` applies it, so the history of a setting that was renamed is not cut at the rename; the exception is a record written under an alias for the sole purpose of registering that alias, which is the history of the alias alone. An alias carries the history of its own name: every record written under it, plus the record that registered it as an alias, which the history file sometimes writes under another name of the same setting.

For system tables, the documentation is the complete reference page embedded in the source. Its column list is rendered from the live table schema, and the event and metric catalogs are rendered from their registries, so generated details stay synchronized with the running binary.

For SQL statements, the documentation is the same as the one exposed by `system.statements`: it also names the enclosing statement, if any, e.g. the `WHERE` clause is a part of `SELECT`.

The `source` column holds the path of the source file where the entity's documentation is defined, relative to the repository root. For most entities it is captured automatically at the place where the documentation object is constructed (the registration site of the component); for the kinds that are documented in a single source file each (such as settings, profile events and current metrics), it is that file.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

Read the documentation of a particular entity:

```sql title="Query"
SELECT description
FROM system.documentation
WHERE type = 'Table Engine' AND name = 'MergeTree'
FORMAT TSVRaw;
```

The same name can refer to several kinds of entities (for example, there is both a `file` table function and a `file` dictionary source), so it is convenient to look a name up across all kinds:

```sql title="Query"
SELECT type, name
FROM system.documentation
WHERE name = 'file'
ORDER BY type;
```

Count the documented entities of each kind:

```sql title="Query"
SELECT type, count()
FROM system.documentation
GROUP BY type
ORDER BY count() DESC;
```

Find out in which version a setting was introduced and how its default value changed since:

```sql title="Query"
SELECT description
FROM system.documentation
WHERE type = 'Setting' AND name = 'async_insert_max_data_size'
FORMAT TSVRaw;
```

## See also {#see-also}

- [`system.functions`](/reference/system-tables/functions) — Regular and aggregate functions.
- [`system.table_functions`](/reference/system-tables/table_functions) — Table functions.
- [`system.table_engines`](/reference/system-tables/table_engines) — Table engines.
- [`system.database_engines`](/reference/system-tables/database_engines) — Database engines.
- [`system.data_type_families`](/reference/system-tables/data_type_families) — Data types.
- [`system.disk_types`](/reference/system-tables/disk_types) — Disk types.
- [`system.settings`](/reference/system-tables/settings) — Settings.
- [`system.merge_tree_settings`](/reference/system-tables/merge_tree_settings) — MergeTree settings.
- [`system.server_settings`](/reference/system-tables/server_settings) — Server settings.
- [`system.settings_changes`](/reference/system-tables/settings_changes) — The history of the changes of the default values of settings.
- [`system.formats`](/reference/system-tables/formats) — Formats.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dropped_tables",
        {R"DOCS_MD(
## Description {#description}

Contains information about tables that drop table has been executed on but for which data cleanup has not yet been performed.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
This table lists only tables dropped from `Atomic` databases. For these tables, `table_dropped_time` is normally based on [`database_atomic_delay_before_drop_table_sec`](/reference/settings/server-settings/settings/other#database_atomic_delay_before_drop_table_sec). For a `Shared` database in ClickHouse Cloud, the recovery period is instead controlled by [`database_shared_drop_table_delay_seconds`](/reference/settings/session-settings/database#database_shared_drop_table_delay_seconds), which defaults to 8 hours; dropped tables from `Shared` databases don't appear in `system.dropped_tables`.

## Example {#example}

The following example shows how to get information about `dropped_tables`.

```sql
SELECT *
FROM system.dropped_tables\G
```

```text
Row 1:
──────
index:                 0
database:              default
table:                 test
uuid:                  03141bb2-e97a-4d7c-a172-95cc066bb3bd
engine:                MergeTree
metadata_dropped_path: /data/ClickHouse/build/programs/data/metadata_dropped/default.test.03141bb2-e97a-4d7c-a172-95cc066bb3bd.sql
table_dropped_time:    2023-03-16 23:43:31
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"dropped_tables_parts",
        {R"DOCS_MD(
## Description {#description}

Contains information about parts of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) dropped tables from [system.dropped_tables](/reference/system-tables/dropped_tables)

The schema of this table is the same as [system.parts](/reference/system-tables/parts)

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See Also {#see-also}

- [MergeTree family](/reference/engines/table-engines/mergetree-family/mergetree)
- [system.parts](/reference/system-tables/parts)
- [system.dropped_tables](/reference/system-tables/dropped_tables)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"enabled_roles",
        {R"DOCS_MD(
## Description {#description}

Contains all active roles at the moment, including the current role of the current user and granted roles for the current role.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"error_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains history of error values from table `system.errors`, periodically flushed to disk.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.error_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:            clickhouse.testing.internal
event_date:          2025-11-11
event_time:          2025-11-11 11:35:28
code:                60
error:               UNKNOWN_TABLE
value:               1
remote:              0
last_error_time:     2025-11-11 11:35:28
last_error_message:  Unknown table expression identifier 'system.table_not_exist' in scope SELECT * FROM system.table_not_exist
last_error_query_id: 77ad9ece-3db7-4236-9b5a-f789bce4aa2e
last_error_trace:    [100506790044914,100506534488542,100506409937998,100506409936517,100506425182891,100506618154123,100506617994473,100506617990486,100506617988112,100506618341386,100506630272160,100506630266232,100506630276900,100506629795243,100506633519500,100506633495783,100506692143858,100506692248921,100506790779783,100506790781278,100506790390399,100506790380047,123814948752036,123814949330028]
```

## See Also {#see-also}

- [error_log setting](/reference/settings/server-settings/settings/other#error_log) — Enabling and disabling the setting.
- [system.errors](/reference/system-tables/errors) — Contains error codes with the number of times they have been triggered.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"errors",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains error codes with the number of times they have been triggered.

To show all possible error codes, including ones which were not triggered, set setting [system_events_show_zero_values](/reference/settings/session-settings/system#system_events_show_zero_values) to 1.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
<Note>
Counters for some errors may increase during successful query execution. It's not recommended to use this table for server monitoring purposes unless you are sure that corresponding error can not be a false positive.
</Note>

## Example {#example}

```sql title="Query"
SELECT name, code, value
FROM system.errors
WHERE value > 0
ORDER BY code ASC
LIMIT 1

┌─name─────────────┬─code─┬─value─┐
│ CANNOT_OPEN_FILE │   76 │     1 │
└──────────────────┴──────┴───────┘
```

```sql title="Response"
WITH arrayMap(x -> demangle(addressToSymbol(x)), last_error_trace) AS all
SELECT name, arrayStringConcat(all, '\n') AS res
FROM system.errors
LIMIT 1
SETTINGS allow_introspection_functions=1\G
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"events",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about the number of events that have occurred in the system. For example, in the table, you can find how many `SELECT` queries were processed since the ClickHouse server started.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.events LIMIT 5
```

```text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## See Also {#see-also}

- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.

## Event descriptions {#event-descriptions}

{/*AUTOGENERATED_DESCRIPTIONS_START*/}
{{PROFILE_EVENTS}}
{/*AUTOGENERATED_DESCRIPTIONS_END*/}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"fail_points",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all available failpoints registered in the server, along with their type and whether they are currently enabled.

Failpoints can be enabled and disabled at runtime using the `SYSTEM ENABLE FAILPOINT` and `SYSTEM DISABLE FAILPOINT` statements.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT * FROM system.fail_points WHERE enabled = 1
```

```text title="Response"
┌─name──────────────────────────────────────┬─type────────────┬─enabled─┐
│ replicated_merge_tree_insert_retry_pause  │ pauseable_once  │       1 │
└───────────────────────────────────────────┴─────────────────┴─────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"filesystem_cache",
        {R"DOCS_MD(
## Description {#description}

Contains information about all entries inside filesystem cache for remote objects.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"filesystem_cache_log",
        {R"DOCS_MD(
Contains a history of all events occurred with filesystem cache for objects on a remote filesystem.

It is safe to truncate or drop this table at any time.

Columns:

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"filesystem_cache_settings",
        {R"DOCS_MD(
## Description {#description}

Contains information about all filesystem cache settings

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"filesystem_read_prefetches_log",
        {R"DOCS_MD(
Contains a history of all prefetches done during reading from MergeTree tables backed by a remote filesystem.

It is safe to truncate or drop this table at any time.

Columns:

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"formats",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all the formats along with flags whether a format is suitable for input/output or whether it supports parallelization.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"functions",
        {R"DOCS_MD(
## Description {#description}

Contains information about normal and aggregate functions.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
 SELECT name, is_aggregate, deterministic, case_insensitive, alias_to FROM system.functions LIMIT 5;
```

```text title="Response"
┌─name─────────────────────┬─is_aggregate─┬─deterministic─┬─case_insensitive─┬─alias_to─┐
│ BLAKE3                   │            0 │                1 │                0 │          │
│ sipHash128Reference      │            0 │                1 │                0 │          │
│ mapExtractKeyLike        │            0 │                1 │                0 │          │
│ sipHash128ReferenceKeyed │            0 │                1 │                0 │          │
│ mapPartialSort           │            0 │                1 │                0 │          │
└──────────────────────────┴──────────────┴──────────────────┴──────────────────┴──────────┘

5 rows in set. Elapsed: 0.002 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"grants",
        {R"DOCS_MD(
## Description {#description}

Privileges granted to ClickHouse user accounts.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"graphite_retentions",
        {R"DOCS_MD(
## Description {#description}

Contains information about parameters [graphite_rollup](/reference/settings/server-settings/settings/graphite#graphite_rollup) which are used in tables with [\*GraphiteMergeTree](/reference/engines/table-engines/mergetree-family/graphitemergetree) engines.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"handlers",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all SQL-defined HTTP handlers created via CREATE HANDLER.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"histogram_metric_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}
History of `system.histogram_metrics`. Snapshot taken every `collect_interval_milliseconds`, flushed to disk.

## Columns {#columns}
{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}
```sql
SELECT event_time, metric, labels, histogram
FROM system.histogram_metric_log
WHERE metric = 'keeper_response_time_ms'
ORDER BY event_time DESC
LIMIT 1
FORMAT Vertical;
```

## See Also {#see-also}
- [system.histogram_metrics](/reference/system-tables/histogram_metrics) — Live histogram metrics.
- [system.metric_log](/reference/system-tables/metric_log) — History of `system.metrics` and `system.events`.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"histogram_metrics",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table contains histogram metrics that can be calculated instantly and exported in the Prometheus format. It is always up to date. Replaces the deprecated `system.latency_log`.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

You can use a query like this to export all the histogram metrics in the Prometheus format.
```sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'histogram' AS type
FROM system.histogram_metrics
FORMAT Prometheus
```

## Metric descriptions {#metric_descriptions}

| Metric | Description |
|---|---|
| `keeper_response_time_ms_bucket` | The response time of Keeper, in milliseconds. |
| `keeper_client_queue_duration_milliseconds_bucket` | Time requests spend waiting to be enqueued and waiting in the queue before being processed by the Keeper client, in milliseconds. |
| `keeper_receive_request_time_milliseconds_bucket` | Time to receive and parse a request from the client in the Keeper TCP handler, in milliseconds. |
| `keeper_dispatcher_requests_queue_time_milliseconds_bucket` | Time a request spends in the Keeper dispatcher requests queue, in milliseconds. |
| `keeper_write_pre_commit_time_milliseconds_bucket` | Time to preprocess a write request before Raft commit, in milliseconds. |
| `keeper_write_commit_time_milliseconds_bucket` | Time to process a write request after Raft commit, in milliseconds. |
| `keeper_dispatcher_responses_queue_time_milliseconds_bucket` | Time a response spends in the Keeper dispatcher responses queue, in milliseconds. |
| `keeper_send_response_time_milliseconds_bucket` | Time to send a response to the client in the Keeper TCP handler (includes queueing and writing to socket), in milliseconds. |
| `keeper_read_wait_for_write_time_milliseconds_bucket` | Time a read request waits for the write request it depends on to complete, in milliseconds. |
| `keeper_read_process_time_milliseconds_bucket` | Time to process a read request in Keeper, in milliseconds. |
| `keeper_batch_size_elements_bucket` | Batch size sent to Raft, in elements. |
| `keeper_batch_size_bytes_bucket` | Batch size sent to Raft, in bytes. |
| `filesystem_cache_evicted_segment_hits_bucket` | Distribution of cache-hit counts on file segments at the moment of their eviction, labelled by cache name. |
| `filesystem_cache_evicted_segment_size_bytes_bucket` | Distribution of byte sizes of evicted file segments, labelled by cache name. |
| `filesystem_cache_evicted_segment_hits_by_user_bucket` | Distribution of cache-hit counts on evicted file segments, labelled by cache name and user id. |
| `filesystem_cache_evicted_segment_size_bytes_by_user_bucket` | Distribution of byte sizes of evicted file segments, labelled by cache name and user id. |

## See Also {#see-also}
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"hypothetical_indexes",
        {R"DOCS_MD(
Lists every hypothetical (what-if) skip index defined in the current session. See [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index#create-hypothetical-index) and [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif).

The contents are session-scoped: each connection sees only its own hypothetical indexes, and the table is empty when no indexes have been created in the current session.

The current `(database, table)` are resolved by UUID at query time, so they reflect `RENAME TABLE` and entries for dropped tables are hidden automatically.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` is the base type name and `type_full` includes the arguments, so users can distinguish between parametrized variants like `bloom_filter(0.01)` and `bloom_filter(0.001)`.

## See also {#see-also}

- [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index#create-hypothetical-index)
- [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"iceberg_files",
        {R"DOCS_MD(
## Description {#description}

This system table contains per-file metadata for Iceberg tables existing in ClickHouse, with one row per data or delete file referenced by the current snapshot of each table. It will be empty if you don't have any Iceberg tables in ClickHouse.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"iceberg_history",
        {R"DOCS_MD(
## Description {#description}

This system table contains the snapshot history of Iceberg tables existing in ClickHouse. It will be empty if you don't have any Iceberg tables in ClickHouse.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"iceberg_metadata_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

The `system.iceberg_metadata_log` table records metadata access and parsing events for Iceberg tables read by ClickHouse. It provides detailed information about each metadata file or entry processed, which is useful for debugging, auditing, and understanding Iceberg table structure evolution.

This table logs every metadata file and entry read from Iceberg tables, including root metadata files, manifest lists, and manifest entries. It helps users trace how ClickHouse interprets Iceberg table metadata and diagnose issues related to schema evolution, file resolution, or query planning.

<Note>
This table is primarily intended for debugging purposes.
</Note>

### Controlling log verbosity {#controlling-log-verbosity}

You can control which metadata events are logged using the [`iceberg_metadata_log_level`](/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_log_level) setting.

To log all metadata used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'manifest_file_entry';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

To log only the root metadata JSON file used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'metadata';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

See more information in the description of the [`iceberg_metadata_log_level`](/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_log_level) setting.

### Good To Know {#good-to-know}

- Use `iceberg_metadata_log_level` at the query level only when you need to investigate your Iceberg table in detail. Otherwise, you may populate the log table with excessive metadata and experience performance degradation.
- The table contains duplicate entries, as it is intended primarily for debugging and does not guarantee uniqueness per entity. Separate rows store content and pruning status because they are collected at different moments in a program. Content is collected when the metadata is read, pruning status is collected when the metadata is checked for pruning. **Never rely on the table itself for deduplication.**
- If you use a `content_type` more verbose than `ManifestListMetadata`, the Iceberg metadata cache is disabled for manifest lists.
- Similarly, if you use a `content_type` more verbose than `ManifestFileMetadata`, the Iceberg metadata cache is disabled for manifest files.
- If the SELECT query was cancelled or failed, the log table may still contain entries for metadata processed before the failure but will not contain information about metadata entities that were not processed.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
### `content_type` values {#content-type-values}

- `None`: No content.
- `Metadata`: Root metadata file.
- `ManifestListMetadata`: Manifest list metadata.
- `ManifestListEntry`: Entry in a manifest list.
- `ManifestFileMetadata`: Manifest file metadata.
- `ManifestFileEntry`: Entry in a manifest file.

## See also {#see-also}
- [Iceberg Table Engine](/reference/engines/table-engines/integrations/iceberg)
- [Iceberg Table Function](/reference/functions/table-functions/iceberg)
- [system.iceberg_history](/reference/system-tables/iceberg_history)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"instrumentation",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains the instrumentation points using LLVM's XRay feature.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.instrumentation FORMAT Vertical;
```

```text
Row 1:
──────
id:            0
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       log
entry_type:    Entry
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     ['test']

Row 2:
──────
id:            1
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       profile
entry_type:    EntryAndExit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     []

Row 3:
──────
id:            2
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       sleep
entry_type:    Exit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     [0.3]

3 rows in set. Elapsed: 0.302 sec.
```

## See Also {#see-also}

- [SYSTEM INSTRUMENT](/reference/statements/system#instrument) — Add or remove instrumentation points.
- [system.trace_log](/reference/system-tables/trace_log) — Inspect profiling log.
- [system.symbols](/reference/system-tables/symbols) — Inspect symbols to add instrumentation points.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"jemalloc_bins",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about memory allocations done via jemalloc allocator in different size classes (bins) aggregated from all arenas.
These statistics might not be absolutely accurate because of thread local caching in jemalloc.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

Find the sizes of allocations that contributed the most to the current overall memory usage.

```sql
SELECT
    *,
    allocations - deallocations AS active_allocations,
    size * active_allocations AS allocated_bytes
FROM system.jemalloc_bins
WHERE allocated_bytes > 0
ORDER BY allocated_bytes DESC
LIMIT 10
```

```text
┌─index─┬─large─┬─────size─┬─allocactions─┬─deallocations─┬─active_allocations─┬─allocated_bytes─┐
│    82 │     1 │ 50331648 │            1 │             0 │                  1 │        50331648 │
│    10 │     0 │      192 │       512336 │        370710 │             141626 │        27192192 │
│    69 │     1 │  5242880 │            6 │             2 │                  4 │        20971520 │
│     3 │     0 │       48 │     16938224 │      16559484 │             378740 │        18179520 │
│    28 │     0 │     4096 │       122924 │        119142 │               3782 │        15491072 │
│    61 │     1 │  1310720 │        44569 │         44558 │                 11 │        14417920 │
│    39 │     1 │    28672 │         1285 │           913 │                372 │        10665984 │
│     4 │     0 │       64 │      2837225 │       2680568 │             156657 │        10026048 │
│     6 │     0 │       96 │      2617803 │       2531435 │              86368 │         8291328 │
│    36 │     1 │    16384 │        22431 │         21970 │                461 │         7553024 │
└───────┴───────┴──────────┴──────────────┴───────────────┴────────────────────┴─────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"jemalloc_profile_text",
        {R"DOCS_MD(
## Description {#description}

Displays the symbolized jemalloc heap profile. Run 'SYSTEM JEMALLOC FLUSH PROFILE' to generate a profile first.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"jemalloc_stats",
        {R"DOCS_MD(
## Description {#description}

Returns jemalloc statistics in a single row with a single column. Equivalent to SYSTEM JEMALLOC STATS command.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"kafka_consumers",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about Kafka consumers.
Applicable for [Kafka table engine](/reference/engines/table-engines/integrations/kafka) (native ClickHouse integration)

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT *
FROM system.kafka_consumers
FORMAT Vertical
```

```text
Row 1:
──────
database:                      test
table:                         kafka
consumer_id:                   ClickHouse-instance-test-kafka-1caddc7f-f917-4bb1-ac55-e28bd103a4a0
assignments.topic:             ['system_kafka_cons']
assignments.partition_id:      [0]
assignments.current_offset:    [18446744073709550615]
exceptions.time:               []
exceptions.text:               []
last_poll_time:                2006-11-09 18:47:47
num_messages_read:             4
last_commit_time:              2006-11-10 04:39:40
num_commits:                   1
last_rebalance_time:           1970-01-01 00:00:00
num_rebalance_revocations:     0
num_rebalance_assignments:     1
is_currently_used:             1
rdkafka_stat:                  {...}
dependencies:                  [['test.mv2','test.target2'],['test.mv1','test.target1']]
missing_dependencies:          []
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"keeper_changelogs",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table does not exist if this node is not configured to run an in-process ClickHouse Keeper. It contains one row per Raft changelog file (`changelog_<from>_<to>.bin[.zstd]`) tracked by the in-process Keeper log store, including the active file currently being appended to.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT from_log_index, to_log_index, entries, path, active FROM system.keeper_changelogs ORDER BY from_log_index;
```

```text
┌─from_log_index─┬─to_log_index─┬─entries─┬─path───────────────────────────┬─active─┐
│              1 │         1000 │    1000 │ changelog_1_1000.bin.zstd      │ false  │
│           1001 │         2000 │     537 │ changelog_1001_2000.bin.zstd   │ true   │
└────────────────┴──────────────┴─────────┴────────────────────────────────┴────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"keeper_cluster",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table does not exist if this node is not configured to run an in-process ClickHouse Keeper. It contains one row per Raft cluster member, fusing static cluster topology (from the Raft configuration) with the local node's own log position.

Every node fills exactly one `last_log_index` value — the row matching its own `server_id`. Peer log positions are not surfaced here because they are tracked only on the leader and that view is not symmetric across the cluster.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT * FROM system.keeper_cluster ORDER BY server_id;
```

```text
┌─server_id─┬─host──┬─endpoint───┬─is_observer─┬─priority─┬─is_leader─┬─is_self─┬─last_log_index─┐
│         1 │ node1 │ node1:9234 │ false       │        3 │ true      │ true    │             42 │
│         2 │ node2 │ node2:9234 │ false       │        2 │ false     │ false   │           ᴺᵁᴸᴸ │
│         3 │ node3 │ node3:9234 │ true        │        1 │ false     │ false   │           ᴺᵁᴸᴸ │
└───────────┴───────┴────────────┴─────────────┴──────────┴───────────┴─────────┴────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"keeper_snapshots",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table does not exist if this node is not configured to run an in-process ClickHouse Keeper. It contains one row per Raft snapshot file tracked by the in-process Keeper state machine, including snapshots currently being received from the leader.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT * FROM system.keeper_snapshots ORDER BY last_log_index;
```

```text
┌─last_log_index─┬─path──────────────────────────┬─disk_name─┬─size_bytes─┬────last_modified_at─┬─is_received─┬─exists_on_disk─┐
│           1000 │ snapshot_1000.bin.zstd        │ default   │      32468 │ 2026-05-22 14:00:00 │ false       │ true           │
│           2000 │ snapshot_2000.bin.zstd        │ default   │      48217 │ 2026-05-22 14:15:00 │ false       │ true           │
└────────────────┴───────────────────────────────┴───────────┴────────────┴─────────────────────┴─────────────┴────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"keeper_storage",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

The table only exists for ClickHouse Keeper deployments that use the `clickhouse server` (and not `clickhouse keeper`) process. It contains one row per node of the data tree stored on the local Keeper node, including the `/keeper` system nodes.

Unlike `system.zookeeper`, this table does not send requests to a Keeper cluster. It reads the committed state of the local Keeper directly from a consistent lock-free view, without affecting request processing. Reading the table does not require a path condition and returns the whole tree, so it is suitable for queries that scan all nodes, such as finding the nodes with the most children or the largest data.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}

## Example {#example}

```sql
SELECT path, num_children, data_length
FROM system.keeper_storage
ORDER BY num_children DESC
LIMIT 3;
```

```text
┌─path──────────────┬─num_children─┬─data_length─┐
│ /                 │            3 │           0 │
│ /clickhouse/tasks │            2 │           0 │
│ /keeper           │            1 │           0 │
└───────────────────┴──────────────┴─────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"keywords",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all keywords used in ClickHouse parser.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"licenses",
        {R"DOCS_MD(
## Description {#description}

Contains licenses of third-party libraries that are located in the [contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib) directory of ClickHouse sources.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT library_name, license_type, license_path FROM system.licenses LIMIT 15
```

```text
┌─library_name───────┬─license_type─┬─license_path────────────────────────┐
│ aws-c-common       │ Apache       │ /contrib/aws-c-common/LICENSE       │
│ boost              │ Boost        │ /contrib/boost/LICENSE_1_0.txt      │
│ brotli             │ MIT          │ /contrib/brotli/LICENSE             │
│ [...]              │ [...]        │ [...]                               │
└────────────────────┴──────────────┴─────────────────────────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"macros",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all macros defined in server configuration.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"masking_policies",
        {R"DOCS_MD(
## Description {#description}

Contains information about all masking policies defined in the system.

Masking policies can only be created and applied in ClickHouse Cloud. In open-source builds the `system.masking_policies` table is always empty, but it is still present so that introspection queries such as `SHOW MASKING POLICIES` work and return an empty result instead of throwing.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"merge_tree_settings",
        {R"DOCS_MD(
## Description {#description}

Contains information about settings for `MergeTree` tables.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}
```sql
SELECT * FROM system.merge_tree_settings LIMIT 3 FORMAT Vertical;
```

```response
SELECT *
FROM system.merge_tree_settings
LIMIT 3
FORMAT Vertical

Query id: 2580779c-776e-465f-a90c-4b7630d0bb70

Row 1:
──────
name:        min_compress_block_size
value:       0
default:     0
changed:     0
description: When granule is written, compress the data in buffer if the size of pending uncompressed data is larger or equal than the specified threshold. If this setting is not set, the corresponding global setting is used.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
is_obsolete: 0
tier:        Production

Row 2:
──────
name:        max_compress_block_size
value:       0
default:     0
changed:     0
description: Compress the pending uncompressed data in buffer if its size is larger or equal than the specified threshold. Block of data will be compressed even if the current granule is not finished. If this setting is not set, the corresponding global setting is used.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
is_obsolete: 0
tier:        Production

Row 3:
──────
name:        index_granularity
value:       8192
default:     8192
changed:     0
description: How many rows correspond to one primary key value.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
is_obsolete: 0
tier:        Production

3 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"merges",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about merges and part mutations currently in process for tables in the MergeTree family.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"metric_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                                                        clickhouse.eu-central1.internal
event_date:                                                      2020-09-05
event_time:                                                      2020-09-05 16:22:33
event_time_microseconds:                                         2020-09-05 16:22:33.196807
ProfileEvent_Query:                                              0
ProfileEvent_SelectQuery:                                        0
ProfileEvent_InsertQuery:                                        0
ProfileEvent_FailedQuery:                                        0
ProfileEvent_FailedSelectQuery:                                  0
...
...
CurrentMetric_Revision:                                          54439
CurrentMetric_VersionInteger:                                    20009001
CurrentMetric_RWLockWaitingReaders:                              0
CurrentMetric_RWLockWaitingWriters:                              0
CurrentMetric_RWLockActiveReaders:                               0
CurrentMetric_RWLockActiveWriters:                               0
CurrentMetric_GlobalThread:                                      74
CurrentMetric_GlobalThreadActive:                                26
CurrentMetric_LocalThread:                                       0
CurrentMetric_LocalThreadActive:                                 0
CurrentMetric_DistributedFilesToInsert:                          0
```

**Schema**
This table can be configured with different schema types using the XML tag `<schema_type>`. The default schema type is `wide`, where each metric or profile event is stored as a separate column. This schema is the most performant and efficient for single-column reads.

The `bucketed` schema stores all profile events and current metrics in a single `metrics` column of type [Map](/reference/data-types/map)([Enum16](/reference/data-types/enum), [Int64](/reference/data-types/int-uint)), so the table consists of a few columns instead of thousands. The `Map` uses the bucketed serialization (`map_serialization_version = 'with_buckets'`) with a constant number of 128 buckets, so reading a single metric reads only one of the 128 buckets. Zero values are not stored: reading a missing key returns `0`. Every metric is also exposed through an `ALIAS` column named as the metric itself (for example, `ProfileEvent_Query UInt64 ALIAS metrics['ProfileEvent_Query']`), so all queries written for the `wide` schema continue to work. Profile events are stored as increments during the collection interval, and current metrics are stored as values at the moment of collection.

```xml
<clickhouse>
    <metric_log>
        <schema_type>bucketed</schema_type>
    </metric_log>
</clickhouse>
```

The `transposed` schema stores data in a format similar to `system.asynchronous_metric_log`, where metrics and events are stored as rows. This schema is useful for low-resource setups because it reduces resource consumption during merges.

**Histograms**

Each row also carries a snapshot of every registered histogram metric in a `histograms` Nested column with fields `metric`, `labels`, `histogram`, `count`, and `sum`. Bucket counts are cumulative since server startup. By default, histograms whose total `count` is zero are not emitted, and zero-counter buckets within an emitted histogram are omitted from the `histogram` map; set `system_metric_log_show_zero_values_in_histograms = 1` (in the default user profile) to keep all histograms and all buckets.

Example query:

```sql
SELECT h.metric, h.labels, h.histogram, h.count, h.sum
FROM system.metric_log
ARRAY JOIN histograms AS h
WHERE h.metric = 'keeper_response_time_ms' AND h.labels['operation_type'] = 'readonly'
ORDER BY event_time DESC
LIMIT 1;
```

## See Also {#see-also}

- [metric_log setting](/reference/settings/server-settings/settings/other#metric_log) — Enabling and disabling the setting.
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"metrics",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains metrics which can be calculated instantly, or have a current value. For example, the number of simultaneously processed queries or the current replica delay. This table is always up to date.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
You can find all supported metrics in source file [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp).

## Example {#example}

```sql
SELECT * FROM system.metrics LIMIT 10
```

```text
┌─metric───────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────┐
│ Query                                │     1 │ Number of executing queries                                            │
│ Merge                                │     0 │ Number of executing background merges                                  │
│ PartMutation                         │     0 │ Number of mutations (ALTER DELETE/UPDATE)                              │
│ ReplicatedFetch                      │     0 │ Number of data parts being fetched from replicas                       │
│ ReplicatedSend                       │     0 │ Number of data parts being sent to replicas                            │
│ ReplicatedChecks                     │     0 │ Number of data parts checking for consistency                          │
│ BackgroundMergesAndMutationsPoolTask │     0 │ Number of active merges and mutations in an associated background pool │
│ BackgroundFetchesPoolTask            │     0 │ Number of active fetches in an associated background pool              │
│ BackgroundCommonPoolTask             │     0 │ Number of active tasks in an associated background pool                │
│ BackgroundMovePoolTask               │     0 │ Number of active tasks in BackgroundProcessingPool for moves           │
└──────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────┘
```

{/*AUTOGENERATED_DESCRIPTIONS_START*/}
{{CURRENT_METRICS}}
{/*AUTOGENERATED_DESCRIPTIONS_END*/}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"models",
        {R"DOCS_MD(
## Description {#description}

Contains a list of CatBoost models loaded into a LibraryBridge's memory along with time when it was loaded.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"moves",
        {R"DOCS_MD(
## Description {#description}

The table contains information about in-progress [data part moves](/reference/statements/alter/partition#move-partitionpart) of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables. Each data part movement is represented by a single row.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.moves
```

```response
┌─database─┬─table─┬─────elapsed─┬─target_disk_name─┬─target_disk_path─┬─part_name─┬─part_size─┬─thread_id─┐
│ default  │ test2 │ 1.668056039 │ s3               │ ./disks/s3/      │ all_3_3_0 │       136 │    296146 │
└──────────┴───────┴─────────────┴──────────────────┴──────────────────┴───────────┴───────────┴───────────┘
```

## See Also {#see-also}

- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) table engine
- [Using Multiple Block Devices for Data Storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)
- [ALTER TABLE ... MOVE PART](/reference/statements/alter/partition#move-partitionpart) command
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"mutations",
        {R"DOCS_MD(
## Description {#description}

The table contains information about [mutations](/reference/statements/alter/index#mutations) of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables and their progress. Each mutation command is represented by a single row.

## Columns: {#columns}

{{SYSTEM_TABLE_COLUMNS}}
<Note>
- If a part name is not in `parts_postpone_reasons` and has not yet been mutated, it means the part is yet not scheduled for mutation.
- The part name `all_parts` represents all parts that have not yet been mutated.
</Note>

- `is_killed` ([UInt8](/reference/data-types/int-uint)) — Indicates whether a mutation has been killed. **Only available in ClickHouse Cloud.**

<Note>
`is_killed=1` does not necessarily mean the mutation is completely finalized. It is possible for a mutation to remain in a state where `is_killed=1` and `is_done=0` for an extended period. This can happen if another long-running mutation is blocking the killed mutation. This is a normal situation.
</Note>

- `is_done` ([UInt8](/reference/data-types/int-uint)) — The flag whether the mutation is done or not. Possible values:
  - `1` if the mutation is completed,
  - `0` if the mutation is still in process.

<Note>
Even if `parts_to_do = 0` it is possible that a mutation of a replicated table is not completed yet because of a long-running `INSERT` query, that will create a new data part needed to be mutated.
</Note>

If there were problems with mutating some data parts, the following columns contain additional information:

- `latest_failed_part` ([String](/reference/data-types/string)) — The name of the most recent part that could not be mutated.
- `latest_fail_time` ([DateTime](/reference/data-types/datetime)) — The date and time of the most recent part mutation failure.
- `latest_fail_reason` ([String](/reference/data-types/string)) — The exception message that caused the most recent part mutation failure.

## Monitoring Mutations {#monitoring-mutations}

To track the progress on the `system.mutations` table, use the following query:

```sql
SELECT * FROM clusterAllReplicas('cluster_name', 'system', 'mutations')
WHERE is_done = 0 AND table = 'tmp';

-- or

SELECT * FROM clusterAllReplicas('cluster_name', 'system.mutations')
WHERE is_done = 0 AND table = 'tmp';
```

Note: this requires read permissions on the `system.*` tables.

<Tip>
**Cloud usage**

In ClickHouse Cloud the `system.mutations` table on each node has all the mutations in the cluster, and there is no need for `clusterAllReplicas`.
</Tip>

## See Also {#see-also}

- [Mutations](/reference/statements/alter/index#mutations)
- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) table engine
- [ReplicatedMergeTree](/reference/engines/table-engines/mergetree-family/replication) family
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"named_collections",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all named collections which were created via SQL query or parsed from configuration file.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"numbers",
        {R"DOCS_MD(
## Description {#description}

This table contains a single UInt64 column named `number` that contains almost all the natural numbers starting from zero.

You can use this table for tests, or if you need to do a brute force search.

Reads from this table are not parallelized.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.numbers LIMIT 10;
```

```response
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```

You can also limit the output by predicates.

```sql
SELECT * FROM system.numbers WHERE number < 10;
```

```response
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"numbers_mt",
        {R"DOCS_MD(
## Description {#description}

The same as [`system.numbers`](/reference/system-tables/numbers) but reads are parallelized. The numbers can be returned in any order.

Used for tests.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.numbers_mt LIMIT 10;
```

```response
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"one",
        {R"DOCS_MD(
## Description {#description}

This table contains a single row with a single `dummy` UInt8 column containing the value 0.

This table is used if a `SELECT` query does not specify the `FROM` clause.

This is similar to the `DUAL` table found in other DBMSs.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.one LIMIT 10;
```

```response
┌─dummy─┐
│     0 │
└───────┘

1 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"opentelemetry_span_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about [trace spans](https://opentracing.io/docs/overview/spans/) for executed queries.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.opentelemetry_span_log LIMIT 1 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
trace_id:         cdab0847-0d62-61d5-4d38-dd65b19a1914
span_id:          701487461015578150
parent_span_id:   2991972114672045096
operation_name:   DB::Block DB::InterpreterSelectQuery::getSampleBlockImpl()
kind:             INTERNAL
start_time_us:    1612374594529090
finish_time_us:   1612374594529108
finish_date:      2021-02-03
attribute.names:  []
attribute.values: []
```

## See Also {#see-also}

- [OpenTelemetry](/guides/oss/deployment-and-scaling/monitoring/opentelemetry)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"part_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

The `system.part_log` table is created only if the [part_log](/reference/settings/server-settings/settings/other#part_log) server setting is specified.

This table contains information about events that occurred with [data parts](/reference/engines/table-engines/mergetree-family/custom-partitioning-key) in the [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) family tables, such as adding or merging data.

The `system.part_log` table contains the following columns:

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
The `system.part_log` table is created after the first inserting data to the `MergeTree` table.

## Example {#example}

```sql
SELECT * FROM system.part_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
query_id:
event_type:              MergeParts
merge_reason:            RegularMerge
merge_algorithm:         Vertical
event_date:              2025-07-19
event_time:              2025-07-19 23:54:19
event_time_microseconds: 2025-07-19 23:54:19.710761
duration_ms:             2158
database:                default
table:                   github_events
table_uuid:              1ad33424-f5f5-402b-ac03-ec82282634ab
part_name:               all_1_7_1
partition_id:            all
partition:               tuple()
part_type:               Wide
part_storage_type:       Full
disk_name:               default
path_on_disk:            ./data/store/1ad/1ad33424-f5f5-402b-ac03-ec82282634ab/all_1_7_1/
rows:                    3285726 -- 3.29 million
size_in_bytes:           438968542 -- 438.97 million
merged_from:             ['all_1_1_0','all_2_2_0','all_3_3_0','all_4_4_0','all_5_5_0','all_6_6_0','all_7_7_0']
bytes_uncompressed:      1373137767 -- 1.37 billion
read_rows:               3285726 -- 3.29 million
read_bytes:              1429206946 -- 1.43 billion
peak_memory_usage:       303611887 -- 303.61 million
error:                   0
exception:
mutation_ids:
ProfileEvents:           {'FileOpen':703,'ReadBufferFromFileDescriptorRead':3824,'ReadBufferFromFileDescriptorReadBytes':439601681,'WriteBufferFromFileDescriptorWrite':592,'WriteBufferFromFileDescriptorWriteBytes':438988500,'ReadCompressedBytes':439601681,'CompressedReadBufferBlocks':6314,'CompressedReadBufferBytes':1539835748,'OpenedFileCacheHits':50,'OpenedFileCacheMisses':484,'OpenedFileCacheMicroseconds':222,'IOBufferAllocs':1914,'IOBufferAllocBytes':319810140,'ArenaAllocChunks':8,'ArenaAllocBytes':131072,'MarkCacheMisses':7,'CreatedReadBufferOrdinary':534,'DiskReadElapsedMicroseconds':139058,'DiskWriteElapsedMicroseconds':51639,'AnalyzePatchRangesMicroseconds':28,'ExternalProcessingFilesTotal':1,'RowsReadByMainReader':170857759,'WaitMarksLoadMicroseconds':988,'LoadedMarksFiles':7,'LoadedMarksCount':14,'LoadedMarksMemoryBytes':728,'Merge':2,'MergeSourceParts':14,'MergedRows':3285733,'MergedColumns':4,'GatheredColumns':51,'MergedUncompressedBytes':1429207058,'MergeTotalMilliseconds':2158,'MergeExecuteMilliseconds':2155,'MergeHorizontalStageTotalMilliseconds':145,'MergeHorizontalStageExecuteMilliseconds':145,'MergeVerticalStageTotalMilliseconds':2008,'MergeVerticalStageExecuteMilliseconds':2006,'MergeProjectionStageTotalMilliseconds':5,'MergeProjectionStageExecuteMilliseconds':4,'MergingSortedMilliseconds':7,'GatheringColumnMilliseconds':56,'ContextLock':2091,'PartsLockHoldMicroseconds':77,'PartsLockWaitMicroseconds':1,'RealTimeMicroseconds':2157475,'CannotWriteToWriteBufferDiscard':36,'LogTrace':6,'LogDebug':59,'LoggerElapsedNanoseconds':514040,'ConcurrencyControlSlotsGranted':53,'ConcurrencyControlSlotsAcquired':53}
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"part_moves_between_shards",
        {R"DOCS_MD(
## Description {#description}

Contains information about parts which are currently in a process of moving between shards and their progress.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"parts",
        {R"DOCS_MD(
## Description {#description}

Contains information about parts of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables.

Each row describes one data part.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.parts LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_4_1_6
part_type:                             Wide
part_storage_type:                     Full
active:                                1
marks:                                 2
rows:                                  6
bytes_on_disk:                         310
data_compressed_bytes:                 157
data_uncompressed_bytes:               91
secondary_indices_compressed_bytes:    58
secondary_indices_uncompressed_bytes:  6
secondary_indices_marks_bytes:         48
marks_bytes:                           144
modification_time:                     2020-06-18 13:01:49
remove_time:                           1970-01-01 00:00:00
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
min_time:                              1970-01-01 00:00:00
max_time:                              1970-01-01 00:00:00
partition_id:                          all
min_block_number:                      1
max_block_number:                      4
level:                                 1
data_version:                          6
primary_key_bytes_in_memory:           8
primary_key_bytes_in_memory_allocated: 64
is_frozen:                             0
database:                              default
table:                                 months
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/months/all_1_4_1_6/
hash_of_all_files:                     2d0657a16d9430824d35e327fcbd87bf
hash_of_uncompressed_files:            84950cc30ba867c77a408ae21332ba29
uncompressed_hash_of_compressed_files: 1ad78f1c6843bbfb99a2c931abe7df7d
delete_ttl_info_min:                   1970-01-01 00:00:00
delete_ttl_info_max:                   1970-01-01 00:00:00
move_ttl_info.expression:              []
move_ttl_info.min:                     []
move_ttl_info.max:                     []
```

## See Also {#see-also}

- [MergeTree family](/reference/engines/table-engines/mergetree-family/mergetree)
- [TTL for Columns and Tables](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"parts_columns",
        {R"DOCS_MD(
## Description {#description}

Contains information about parts and columns of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables.
Each row describes one data part.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.parts_columns LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_2_1
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  2
bytes_on_disk:                         155
data_compressed_bytes:                 56
data_uncompressed_bytes:               4
marks_bytes:                           96
modification_time:                     2020-09-23 10:13:36
remove_time:                           2106-02-07 06:28:15
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
partition_id:                          all
min_block_number:                      1
max_block_number:                      2
level:                                 1
data_version:                          1
primary_key_bytes_in_memory:           2
primary_key_bytes_in_memory_allocated: 64
database:                              default
table:                                 53r93yleapyears
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/53r93yleapyears/all_1_2_1/
column:                                id
type:                                  Int8
column_position:                       1
default_kind:
default_expression:
column_bytes_on_disk:                  76
column_data_compressed_bytes:          28
column_data_uncompressed_bytes:        2
column_marks_bytes:                    48
```

## See Also {#see-also}

- [MergeTree family](/reference/engines/table-engines/mergetree-family/mergetree)
- [Calculating the number and size of compact and wide parts](/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"predicate_statistics_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains sampled selectivity statistics collected while reading from `MergeTree` tables. The table is populated only when [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate) is greater than `0`.

Use this table to inspect how selective user predicates are in real workloads and how many granules remain after primary-key or skip-index filtering. The data is intended as input for workload-driven index and projection recommendations.

## Row shapes {#row-shapes}

A single query can produce two kinds of rows in `system.predicate_statistics_log`:

- **Filter rows**, emitted per prewhere/filter step in `MergeTreeSelectProcessor`. They populate `predicate_expression`, `input_rows`, `passed_rows`, `filter_selectivity`, and the whole-predicate columns `total_input_rows`, `total_passed_rows`, `total_selectivity`. Index-related columns are empty.
- **Index rows**, emitted per read step in `ReadFromMergeTree`. They populate the `index_names`, `index_types`, `total_granules`, `granules_after`, and `index_selectivities` arrays, one entry per index stage (primary key, partition, skip indexes). Predicate-related columns are empty.

Filter rows and index rows for the same query share the same `query_id` and `table`, so they can be joined when both are needed.

## Sampling and overhead {#sampling-and-overhead}

Sampling is controlled by [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate):

- `0` disables collection.
- `1` samples every query.
- `N > 1` samples approximately `1 / N` of queries, hashed by `query_id`.

Lower values produce more data but add CPU work on the read path and more writes to the system log. After enabling the setting, use [`SYSTEM FLUSH LOGS`](/reference/statements/system#flush-logs) if you need rows to appear immediately.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SET predicate_statistics_sample_rate = 1;

SELECT *
FROM hits
WHERE URL LIKE '%/product/%' AND EventDate >= today() - 7
FORMAT Null;

SYSTEM FLUSH LOGS predicate_statistics_log;

SELECT
    query_id,
    predicate_expression,
    round(filter_selectivity, 3) AS step_selectivity,
    round(total_selectivity, 3) AS query_selectivity,
    index_names,
    index_selectivities
FROM system.predicate_statistics_log
WHERE table = 'hits'
ORDER BY event_time DESC
LIMIT 10;
```

## See also {#see-also}

- [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate)
- [system.query_log](/reference/system-tables/query_log)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"primes",
        {R"DOCS_MD(
## Description {#description}

This table contains a single UInt64 column named `prime` that contains prime numbers in ascending order, starting from 2.

You can use this table for tests, or if you need to do a brute force search over prime numbers.

Reads from this table are not parallelized.

This is similar to the [`primes`](/reference/functions/table-functions/primes) table function.

You can also limit the output by predicates.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

The first 10 primes.
```sql
SELECT * FROM system.primes LIMIT 10;
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

The first prime greater than 1e15.
```sql
SELECT prime FROM system.primes WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"privileges",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all available privileges that could be granted to a user or role.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"processes",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This system table is used for implementing the `SHOW PROCESSLIST` query.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
```sql
SELECT * FROM system.processes LIMIT 10 FORMAT Vertical;
```

```response
Row 1:
──────
is_initial_query:     1
user:                 default
query_id:             35a360fa-3743-441d-8e1f-228c938268da
address:              ::ffff:172.23.0.1
port:                 47588
initial_user:         default
initial_query_id:     35a360fa-3743-441d-8e1f-228c938268da
initial_address:      ::ffff:172.23.0.1
initial_port:         47588
interface:            1
os_user:              bharatnc
client_hostname:      tower
client_name:          ClickHouse
client_revision:      54437
client_version_major: 20
client_version_minor: 7
client_version_patch: 2
http_method:          0
http_user_agent:
quota_key:
elapsed:              0.000582537
is_cancelled:         0
is_all_data_sent:     0
read_rows:            0
read_bytes:           0
total_rows_approx:    0
written_rows:         0
written_bytes:        0
memory_usage:         0
peak_memory_usage:    0
query:                SELECT * from system.processes LIMIT 10 FORMAT Vertical;
thread_ids:           [67]
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
Settings:             {'background_pool_size':'32','load_balancing':'random','allow_suspicious_low_cardinality_types':'1','distributed_aggregation_memory_efficient':'1','skip_unavailable_shards':'1','log_queries':'1','max_bytes_before_external_group_by':'20000000000','max_bytes_before_external_sort':'20000000000','allow_introspection_functions':'1'}

1 rows in set. Elapsed: 0.002 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"processors_profile_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table contains profiling on processors level (that you can find in [`EXPLAIN PIPELINE`](/reference/statements/explain#explain-pipeline)).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
EXPLAIN PIPELINE
SELECT sleep(1)
┌─explain─────────────────────────┐
│ (Expression)                    │
│ ExpressionTransform             │
│   (SettingQuotaAndLimits)       │
│     (ReadFromStorage)           │
│     SourceFromSingleChunk 0 → 1 │
└─────────────────────────────────┘

SELECT sleep(1)
SETTINGS log_processors_profiles = 1
Query id: feb5ed16-1c24-4227-aa54-78c02b3b27d4
┌─sleep(1)─┐
│        0 │
└──────────┘
1 rows in set. Elapsed: 1.018 sec.

SELECT
    name,
    elapsed_us,
    input_wait_elapsed_us,
    output_wait_elapsed_us
FROM system.processors_profile_log
WHERE query_id = 'feb5ed16-1c24-4227-aa54-78c02b3b27d4'
ORDER BY name ASC
```

```text title="Response"
┌─name────────────────────┬─elapsed_us─┬─input_wait_elapsed_us─┬─output_wait_elapsed_us─┐
│ ExpressionTransform     │    1000497 │                  2823 │                    197 │
│ LazyOutputFormat        │         36 │               1002188 │                      0 │
│ LimitsCheckingTransform │         10 │               1002994 │                    106 │
│ NullSource              │          5 │               1002074 │                      0 │
│ NullSource              │          1 │               1002084 │                      0 │
│ SourceFromSingleChunk   │         45 │                  4736 │                1000819 │
└─────────────────────────┴────────────┴───────────────────────┴────────────────────────┘
```

Here you can see:

- `ExpressionTransform` was executing `sleep(1)` function, so it `work` will takes 1e6, and so `elapsed_us` > 1e6.
- `SourceFromSingleChunk` need to wait, because `ExpressionTransform` does not accept any data during execution of `sleep(1)`, so it will be in `PortFull` state for 1e6 us, and so `output_wait_elapsed_us` > 1e6.
- `LimitsCheckingTransform`/`NullSource`/`LazyOutputFormat` need to wait until `ExpressionTransform` will execute `sleep(1)` to process the result, so `input_wait_elapsed_us` > 1e6.

## See Also {#see-also}

- [`EXPLAIN PIPELINE`](/reference/statements/explain#explain-pipeline)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"projection_parts",
        {R"DOCS_MD(
## Description {#description}

This table contains information about projection parts for tables of the MergeTree family.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"projection_parts_columns",
        {R"DOCS_MD(
## Description {#description}

This table contains information about columns in projection parts for tables of the MergeTree family.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"projections",
        {R"DOCS_MD(
## Description {#description}

Contains information about existing projections in all tables.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.projections LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       landing
name:        improved_sorting_key
type:        Normal
sorting_key: ['user_id','date']
query:       SELECT * ORDER BY user_id, date
settings:     {}

Row 2:
──────
database:    default
table:       landing
name:        agg_no_key
type:        Aggregate
sorting_key: []
query:       SELECT count()
settings:     {}
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"query_cache",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Shows the content of the [query cache](/concepts/features/performance/caches/query-cache).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.query_cache FORMAT Vertical;
```

```text
Row 1:
──────
query:       SELECT 1 SETTINGS use_query_cache = 1
query_id:    7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
result_size: 128
tag:
stale:       0
shared:      0
compressed:  1
expires_at:  2023-10-13 13:35:45
key_hash:    12188185624808016954

1 row in set. Elapsed: 0.004 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"query_condition_cache",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Shows the content of the [query condition cache](/concepts/features/performance/caches/query-condition-cache).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.query_condition_cache FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
table_uuid:     28270a24-ea27-49f6-99cd-97b9bee976ac
part_name:      all_1_1_0
condition:      or(equals(b, 10000_UInt16), equals(c, 10000_UInt16))
condition_hash: 5456494897146899690 -- 5.46 quintillion
entry_size:     40
matching_marks: 111111110000000000000000000000000000000000000000000000000111111110000000000000000

1 row in set. Elapsed: 0.004 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"query_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Stores metadata and statistics about executed queries, such as start time, duration, error messages, resource usage, and other execution details. It does not store the results of queries.

You can change settings of queries logging in the [query_log](/reference/settings/server-settings/settings/query#query_log) section of the server configuration.

You can disable queries logging by setting [log_queries = 0](/reference/settings/session-settings/log-queries#log_queries). We do not recommend to turn off logging because information in this table is important for solving issues.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_log](/reference/settings/server-settings/settings/query#query_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

The `system.query_log` table registers two kinds of queries:

1.  Initial (top-level) queries.
2.  Child queries that were initiated by other queries, including queries for distributed execution and internal subqueries such as view evaluation. For these queries, information about the original initial query is shown in the `initial_*` columns.

<Tip>
**Filter initial queries by default**

Generally, add `is_initial_query = 1` whenever you query `system.query_log`. This excludes child queries so individual processing steps are not counted separately from the initial query. This filter does not imply that a query was submitted by a client, because server-internal work can also be an initial query.

Use `initial_query_id` instead when you need to trace an initial query together with child queries that preserve its ID. The initial query has the same value for `initial_query_id` and `query_id`, while child queries in the same chain keep the initial query's `initial_query_id` and have their own `query_id`. Not all work spawned by an initial query is correlated this way: server-dispatched work can start a new initial-query chain with a new `initial_query_id`, as with remote [`QueryRunner`](/reference/engines/table-engines/special/query-runner#cluster-mode) dispatches.

```sql
SELECT
    hostname,
    type,
    query_id,
    initial_query_id,
    is_initial_query,
    query
FROM system.query_log
WHERE initial_query_id = '<query_id_of_initial_query>'
ORDER BY event_time_microseconds;
```

If correlated child queries can run on other nodes, query `system.query_log` on every node, for example with [`clusterAllReplicas`](/reference/functions/table-functions/cluster).
</Tip>

Each query creates one or two rows in the `query_log` table, depending on the status (see the `type` column) of the query:

1.  If the query execution was successful, two rows with the `QueryStart` and `QueryFinish` types are created.
2.  If an error occurred during query processing, two events with the `QueryStart` and `ExceptionWhileProcessing` types are created.
3.  If an error occurred before launching the query, a single event with the `ExceptionBeforeStart` type is created.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability) setting to reduce the number of queries, registered in the `query_log` table.

You can use the [log_formatted_queries](/reference/settings/session-settings/log#log_formatted_queries) setting to log formatted queries to the `formatted_query` column.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Examples {#examples}

**Basic example**

```sql
SELECT *
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
ORDER BY query_start_time DESC
LIMIT 1
FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                              clickhouse.eu-central1.internal
type:                                  QueryFinish
event_date:                            2021-11-03
event_time:                            2021-11-03 16:13:54
event_time_microseconds:               2021-11-03 16:13:54.953024
query_start_time:                      2021-11-03 16:13:54
query_start_time_microseconds:         2021-11-03 16:13:54.952325
query_duration_ms:                     0
read_rows:                             69
read_bytes:                            6187
written_rows:                          0
written_bytes:                         0
result_rows:                           69
result_bytes:                          48256
memory_usage:                          0
current_database:                      default
query:                                 DESCRIBE TABLE system.query_log
formatted_query:
normalized_query_hash:                 8274064835331539124
query_kind:
databases:                             []
tables:                                []
columns:                               []
projections:                           []
views:                                 []
exception_code:                        0
exception:
stack_trace:
is_initial_query:                      1
user:                                  default
query_id:                              7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
address:                               ::ffff:127.0.0.1
port:                                  40452
initial_user:                          default
initial_query_id:                      7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
initial_address:                       ::ffff:127.0.0.1
initial_port:                          40452
initial_query_start_time:              2021-11-03 16:13:54
initial_query_start_time_microseconds: 2021-11-03 16:13:54.952325
interface:                             1
os_user:                               sevirov
client_hostname:                       clickhouse.eu-central1.internal
client_name:                           ClickHouse
client_revision:                       54449
client_version_major:                  21
client_version_minor:                  10
client_version_patch:                  1
http_method:                           0
http_user_agent:
http_referer:
forwarded_for:
quota_key:
revision:                              54456
log_comment:
thread_ids:                            [30776,31174]
ProfileEvents:                         {'Query':1,'NetworkSendElapsedMicroseconds':59,'NetworkSendBytes':2643,'SelectedRows':69,'SelectedBytes':6187,'ContextLock':9,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':817,'UserTimeMicroseconds':427,'SystemTimeMicroseconds':212,'OSCPUVirtualTimeMicroseconds':639,'OSReadChars':894,'OSWriteChars':319}
Settings:                              {'load_balancing':'random','max_memory_usage':'10000000000'}
used_aggregate_functions:              []
used_aggregate_function_combinators:   []
used_database_engines:                 []
used_data_type_families:               []
used_dictionaries:                     []
used_formats:                          []
used_functions:                        []
used_storages:                         []
used_table_functions:                  []
used_executable_user_defined_functions:[]
used_sql_user_defined_functions:       []
used_privileges:                       []
missing_privileges:                    []
query_cache_usage:                     None
```

**Cloud example**

In ClickHouse Cloud, `system.query_log` is local to each node; to see all entries you must query via [`clusterAllReplicas`](/reference/functions/table-functions/cluster).

For example, to aggregate query_log rows from every replica in the “default” cluster you can write:

```sql
SELECT *
FROM clusterAllReplicas('default', system.query_log)
WHERE event_time >= now() - toIntervalHour(1)
LIMIT 10
SETTINGS skip_unavailable_shards = 1;
```

## See Also {#see-also}

- [system.query_thread_log](/reference/system-tables/query_thread_log) — This table contains information about each query execution thread.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"query_metric_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains a history of memory and metric values from table `system.events` for individual queries, periodically flushed to disk.

Once a query starts, data is collected at periodic intervals of `query_metric_log_interval` milliseconds (which is set to 1000
by default). The data is also collected when the query finishes if the query takes longer than `query_metric_log_interval`.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.query_metric_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
query_id:                                                        97c8ba04-b6d4-4bd7-b13e-6201c5c6e49d
hostname:                                                        clickhouse.eu-central1.internal
event_date:                                                      2020-09-05
event_time:                                                      2020-09-05 16:22:33
event_time_microseconds:                                         2020-09-05 16:22:33.196807
memory_usage:                                                    313434219
peak_memory_usage:                                               598951986
ProfileEvent_Query:                                              0
ProfileEvent_SelectQuery:                                        0
ProfileEvent_InsertQuery:                                        0
ProfileEvent_FailedQuery:                                        0
ProfileEvent_FailedSelectQuery:                                  0
...
```

## See Also {#see-also}

- [query_metric_log setting](/reference/settings/server-settings/settings/query#query_metric_log) — Enabling and disabling the setting.
- [query_metric_log_interval](/reference/settings/session-settings/other#query_metric_log_interval)
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"query_thread_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about threads that execute queries, for example, thread name, thread start time, duration of query processing.

To start logging:

1.  Configure parameters in the [query_thread_log](/reference/settings/server-settings/settings/query#query_thread_log) section.
2.  Set [log_query_threads](/reference/settings/session-settings/log-query#log_query_threads) to 1.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_thread_log](/reference/settings/server-settings/settings/query#query_thread_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability)) setting to reduce the number of queries, registered in the `query_thread_log` table.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
 SELECT * FROM system.query_thread_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                      clickhouse.eu-central1.internal
event_date:                    2020-09-11
event_time:                    2020-09-11 10:08:17
event_time_microseconds:       2020-09-11 10:08:17.134042
query_start_time:              2020-09-11 10:08:17
query_start_time_microseconds: 2020-09-11 10:08:17.063150
query_duration_ms:             70
read_rows:                     0
read_bytes:                    0
written_rows:                  1
written_bytes:                 12
memory_usage:                  4300844
peak_memory_usage:             4300844
thread_name:                   TCPHandler
thread_id:                     638133
master_thread_id:              638133
query:                         INSERT INTO test1 VALUES
is_initial_query:              1
user:                          default
query_id:                      50a320fd-85a8-49b8-8761-98a86bcbacef
address:                       ::ffff:127.0.0.1
port:                          33452
initial_user:                  default
initial_query_id:              50a320fd-85a8-49b8-8761-98a86bcbacef
initial_address:               ::ffff:127.0.0.1
initial_port:                  33452
interface:                     1
os_user:                       bharatnc
client_hostname:               tower
client_name:                   ClickHouse
client_revision:               54437
client_version_major:          20
client_version_minor:          7
client_version_patch:          2
http_method:                   0
http_user_agent:
quota_key:
revision:                      54440
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
```

## See Also {#see-also}

- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.query_views_log](/reference/system-tables/query_views_log) — This table contains information about each view executed during a query.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"query_views_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about the dependent views executed when running a query, for example, the view type or the execution time.

To start logging:

1. Configure parameters in the [query_views_log](/reference/settings/server-settings/settings/query#query_views_log) section.
2. Set [log_query_views](/reference/settings/session-settings/log-query#log_query_views) to 1.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_views_log](/reference/settings/server-settings/settings/query#query_views_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability)) setting to reduce the number of queries, registered in the `query_views_log` table.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.query_views_log LIMIT 1 \G;
```

```text title="Response"
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2021-06-22
event_time:              2021-06-22 13:23:07
event_time_microseconds: 2021-06-22 13:23:07.738221
view_duration_ms:        0
initial_query_id:        c3a1ac02-9cad-479b-af54-9e9c0a7afd70
view_name:               default.matview_inner
view_uuid:               00000000-0000-0000-0000-000000000000
view_type:               Materialized
view_query:              SELECT * FROM default.table_b
view_target:             default.`.inner.matview_inner`
read_rows:               4
read_bytes:              64
written_rows:            2
written_bytes:           32
peak_memory_usage:       4196188
ProfileEvents:           {'FileOpen':2,'WriteBufferFromFileDescriptorWrite':2,'WriteBufferFromFileDescriptorWriteBytes':187,'IOBufferAllocs':3,'IOBufferAllocBytes':3145773,'FunctionExecute':3,'DiskWriteElapsedMicroseconds':13,'InsertedRows':2,'InsertedBytes':16,'SelectedRows':4,'SelectedBytes':48,'ContextLock':16,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':698,'SoftPageFaults':4,'OSReadChars':463}
status:                  QueryFinish
exception_code:          0
exception:
stack_trace:
```

## See Also {#see-also}

{/*AUTOGENERATED_START*/}
{/*AUTOGENERATED_END*/}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"quota_limits",
        {R"DOCS_MD(
## Description {#description}

Contains information about maximums for all intervals of all quotas. Any number of rows or zero can correspond to one quota.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"quota_usage",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Quota usage by the current user: how much is used and how much is left.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See Also {#see-also}

- [SHOW QUOTA](/reference/statements/show#show-quota))
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"quotas",
        {R"DOCS_MD(
## Description {#description}

Contains information about [quotas](/reference/system-tables/quotas).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See Also {#see-also}

- [SHOW QUOTAS](/reference/statements/show#show-quotas)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"quotas_usage",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Quota usage by all users.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See Also {#see-also}

- [SHOW QUOTA](/reference/statements/show#show-quota))
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"remote_data_paths",
        {R"DOCS_MD(
## Description {#description}

Contains information about data files stored on remote disks (e.g. S3, Azure Blob Storage), including the mapping between local metadata paths and remote blob paths.

Each row represents one remote blob object associated with a data file.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.remote_data_paths LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
disk_name:              s3
path:                   /var/lib/clickhouse/disks/s3/
cache_base_path:        /var/lib/clickhouse/disks/s3_cache/
local_path:             store/123/1234abcd-1234-1234-1234-1234abcd1234/all_0_0_0/data.bin
remote_path:            abc123/all_0_0_0/data.bin
size:                   1048576
common_prefix_for_blobs:
cache_paths:            ['/var/lib/clickhouse/disks/s3_cache/a1/b2/c3d4e5f6']
```

## See Also {#see-also}

- [Using external storage for data storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-s3)
- [Configuring external storage](/concepts/features/configuration/server-config/storing-data#configuring-external-storage)
- [system.disks](/reference/system-tables/disks)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"replicas",
        {R"DOCS_MD(
## Description {#description}

Contains information and status for replicated tables residing on the local server.
This table can be used for monitoring. The table contains a row for every Replicated\* table.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT *
FROM system.replicas
WHERE table = 'test_table'
FORMAT Vertical
```

```text
Query id: dc6dcbcb-dc28-4df9-ae27-4354f5b3b13e

Row 1:
───────
database:                    db
table:                       test_table
engine:                      ReplicatedMergeTree
is_leader:                   1
can_become_leader:           1
is_readonly:                 0
is_session_expired:          0
future_parts:                0
parts_to_check:              0
zookeeper_path:              /test/test_table
replica_name:                r1
replica_path:                /test/test_table/replicas/r1
columns_version:             -1
queue_size:                  27
inserts_in_queue:            27
merges_in_queue:             0
part_mutations_in_queue:     0
queue_oldest_time:           2021-10-12 14:48:48
inserts_oldest_time:         2021-10-12 14:48:48
merges_oldest_time:          1970-01-01 03:00:00
part_mutations_oldest_time:  1970-01-01 03:00:00
oldest_part_to_get:          1_17_17_0
oldest_part_to_merge_to:
oldest_part_to_mutate_to:
log_max_index:               206
log_pointer:                 207
last_queue_update:           2021-10-12 14:50:08
absolute_delay:              99
total_replicas:              5
active_replicas:             5
lost_part_count:             0
last_queue_update_exception:
zookeeper_exception:
replica_is_active:           {'r1':1,'r2':1}
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"replicated_fetches",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about currently running background fetches.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.replicated_fetches LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:                    default
table:                       t
elapsed:                     7.243039876
progress:                    0.41832135995612835
result_part_name:            all_0_0_0
result_part_path:            /var/lib/clickhouse/store/700/70080a04-b2de-4adf-9fa5-9ea210e81766/all_0_0_0/
partition_id:                all
total_size_bytes_compressed: 1052783726
bytes_read_compressed:       440401920
source_replica_path:         /clickhouse/test/t/replicas/1
source_replica_hostname:     node1
source_replica_port:         9009
interserver_scheme:          http
URI:                         http://node1:9009/?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftest%2Ft%2Freplicas%2F1&part=all_0_0_0&client_protocol_version=4&compress=false
to_detached:                 0
thread_id:                   54
```

## See Also {#see-also}

- [Managing ReplicatedMergeTree Tables](/reference/statements/system#managing-replicatedmergetree-tables)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"replicated_merge_tree_settings",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all ReplicatedMergeTree engine specific settings, their current and default values along with descriptions. You may change any of them in SETTINGS section in CREATE query.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"replication_queue",
        {R"DOCS_MD(
## Description {#description}

Contains information about tasks from replication queues stored in ClickHouse Keeper, or ZooKeeper, for tables in the `ReplicatedMergeTree` family.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.replication_queue LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:               merge
table:                  visits_v2
replica_name:           mtgiga001-1t
position:               15
node_name:              queue-0009325559
type:                   MERGE_PARTS
create_time:            2020-12-07 14:04:21
required_quorum:        0
source_replica:         mtgiga001-1t
new_part_name:          20201130_121373_121384_2
parts_to_merge:         ['20201130_121373_121378_1','20201130_121379_121379_0','20201130_121380_121380_0','20201130_121381_121381_0','20201130_121382_121382_0','20201130_121383_121383_0','20201130_121384_121384_0']
is_detach:              0
is_currently_executing: 0
num_tries:              36
last_exception:         Code: 226, e.displayText() = DB::Exception: Marks file '/opt/clickhouse/data/merge/visits_v2/tmp_fetch_20201130_121373_121384_2/CounterID.mrk' does not exist (version 20.8.7.15 (official build))
last_attempt_time:      2020-12-08 17:35:54
num_postponed:          0
postpone_reason:
last_postpone_time:     1970-01-01 03:00:00
```

## See Also {#see-also}

- [Managing ReplicatedMergeTree Tables](/reference/statements/system#managing-replicatedmergetree-tables)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"resources",
        {R"DOCS_MD(
## Description {#description}

Contains information about [resources](/concepts/features/configuration/server-config/workload-scheduling#workload_entity_storage) residing on the local server. The table contains a row for every resource.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT *
FROM system.resources
FORMAT Vertical
```

```text
Row 1:
──────
name:         io_read
read_disks:   ['s3']
write_disks:  []
create_query: CREATE RESOURCE io_read (READ DISK s3)

Row 2:
──────
name:         io_write
read_disks:   []
write_disks:  ['s3']
create_query: CREATE RESOURCE io_write (WRITE DISK s3)
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"rocksdb",
        {R"DOCS_MD(
## Description {#description}

Contains a list of metrics exposed from embedded RocksDB.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"role_grants",
        {R"DOCS_MD(
## Description {#description}

Contains the role grants for users and roles. To add entries to this table, use `GRANT role TO user`.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"roles",
        {R"DOCS_MD(
## Description {#description}

Contains information about configured [roles](/concepts/features/security/access-rights#role-management).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See Also {#see-also}

- [SHOW ROLES](/reference/statements/show#show-roles)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"row_policies",
        {R"DOCS_MD(
## Description {#description}

Contains filters for one particular table, as well as a list of roles and/or users which should use this row policy.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See Also {#see-also}

- [SHOW POLICIES](/reference/statements/show#show-policies)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"s3_queue_metadata",
        {R"DOCS_MD(
## Description {#description}

Contains the current number of processed, processing and failed nodes in keeper for each S3Queue metadata object and, on demand, their contents. Unlike system.s3queue_metadata_cache, which shows the in-memory cache, this table reads the state directly from keeper.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"s3_queue_settings",
        {R"DOCS_MD(
## Description {#description}

Contains information about the settings of [S3Queue](/reference/engines/table-engines/integrations/s3queue) tables. Available from server version `24.10`.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"s3queue_log",
        {R"DOCS_MD(
Contains log entries with information about files processed by the S3Queue engine.

It is safe to truncate or drop this table at any time.

Columns:

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"s3queue_metadata_cache",
        {R"DOCS_MD(
## Description {#description}

Contains in-memory state of S3Queue metadata and currently processed rows per file.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"scheduler",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about and status of [scheduling nodes](/concepts/features/configuration/server-config/workload-scheduling#hierarchy) residing on the local server.
This table can be used for monitoring. The table contains a row for every scheduling node.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT *
FROM system.scheduler
WHERE resource = 'network_read' AND path = '/prio/fair/prod'
FORMAT Vertical
```

```text
Row 1:
──────
resource:          network_read
path:              /prio/fair/prod
type:              fifo
weight:            5
priority:          0
is_active:         0
active_children:   0
dequeued_requests: 67
canceled_requests: 0
dequeued_cost:     4692272
canceled_cost:     0
busy_periods:      63
vruntime:          938454.1999999989
system_vruntime:   ᴺᵁᴸᴸ
queue_length:      0
queue_cost:        0
budget:            -60524
is_satisfied:      ᴺᵁᴸᴸ
inflight_requests: ᴺᵁᴸᴸ
inflight_cost:     ᴺᵁᴸᴸ
max_requests:      ᴺᵁᴸᴸ
max_cost:          ᴺᵁᴸᴸ
max_speed:         ᴺᵁᴸᴸ
max_burst:         ᴺᵁᴸᴸ
throttling_us:     ᴺᵁᴸᴸ
tokens:            ᴺᵁᴸᴸ
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"schema_inference_cache",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about all cached file schemas.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

Let's say we have a file `data.jsonl` with this content:
```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

<Tip>
Place `data.jsonl` in the `user_files_path` directory.  You can find this by looking
in your ClickHouse configuration files. The default is:
```sql
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```
</Tip>

Open `clickhouse-client` and run the `DESCRIBE` query:

```sql
DESCRIBE file('data.jsonl') SETTINGS input_format_try_infer_integers=0;
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Float64)       │              │                    │         │                  │                │
│ age     │ Nullable(Float64)       │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Let's see the content of the `system.schema_inference_cache` table:

```sql
SELECT *
FROM system.schema_inference_cache
FORMAT Vertical
```
```response
Row 1:
──────
storage:                File
source:                 /home/droscigno/user_files/data.jsonl
format:                 JSONEachRow
additional_format_info: schema_inference_hints=, max_rows_to_read_for_schema_inference=25000, schema_inference_make_columns_nullable=true, try_infer_integers=false, try_infer_dates=true, try_infer_datetimes=true, try_infer_numbers_from_strings=true, read_bools_as_numbers=true, try_infer_objects=false
registration_time:      2022-12-29 17:49:52
schema:                 id Nullable(Float64), age Nullable(Float64), name Nullable(String), hobbies Array(Nullable(String))
```

## See Also {#see-also}
- [Automatic schema inference from input data](/concepts/features/interfaces/schema-inference)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"server_settings",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about global settings for the server, which are specified in `config.xml`.
The table also includes supported nested settings with a fixed structure; dynamic sections such as lists are not included.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

The following example shows how to get information about server settings which name contains `thread_pool`.

```sql
SELECT *
FROM system.server_settings
WHERE name LIKE '%thread_pool%'
```

```text
┌─name──────────────────────────────────────────┬─value─┬─default─┬─changed─┬─description─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─type───┬─changeable_without_restart─┬─is_obsolete─┐
│ max_thread_pool_size                          │ 10000 │ 10000   │       0 │ The maximum number of threads that could be allocated from the OS and used for query execution and background operations.                           │ UInt64 │                         No │           0 │
│ max_thread_pool_free_size                     │ 1000  │ 1000    │       0 │ The maximum number of threads that will always stay in a global thread pool once allocated and remain idle in case of insufficient number of tasks. │ UInt64 │                         No │           0 │
│ thread_pool_queue_size                        │ 10000 │ 10000   │       0 │ The maximum number of tasks that will be placed in a queue and wait for execution.                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_size                       │ 100   │ 100     │       0 │ The maximum number of threads that would be used for IO operations                                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_free_size                  │ 0     │ 0       │       0 │ Max free size for IO thread pool.                                                                                                                   │ UInt64 │                         No │           0 │
│ io_thread_pool_queue_size                     │ 10000 │ 10000   │       0 │ Queue size for IO thread pool.                                                                                                                      │ UInt64 │                         No │           0 │
│ max_active_parts_loading_thread_pool_size     │ 64    │ 64      │       0 │ The number of threads to load active set of data parts (Active ones) at startup.                                                                    │ UInt64 │                         No │           0 │
│ max_outdated_parts_loading_thread_pool_size   │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Outdated ones) at startup.                                                                │ UInt64 │                         No │           0 │
│ max_unexpected_parts_loading_thread_pool_size │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Unexpected ones) at startup.                                                              │ UInt64 │                         No │           0 │
│ max_parts_cleaning_thread_pool_size           │ 128   │ 128     │       0 │ The number of threads for concurrent removal of inactive data parts.                                                                                │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_size               │ 1000  │ 1000    │       0 │ The maximum number of threads that would be used for IO operations for BACKUP queries                                                               │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_free_size          │ 0     │ 0       │       0 │ Max free size for backups IO thread pool.                                                                                                           │ UInt64 │                         No │           0 │
│ backups_io_thread_pool_queue_size             │ 0     │ 0       │       0 │ Queue size for backups IO thread pool.                                                                                                              │ UInt64 │                         No │           0 │
└───────────────────────────────────────────────┴───────┴─────────┴─────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴────────────────────────────┴─────────────┘

```

Using of `WHERE changed` can be useful, for example, when you want to check
whether settings in configuration files are loaded correctly and are in use.

{/* */}

```sql
SELECT * FROM system.server_settings WHERE changed AND name='max_thread_pool_size'
```

## See Also {#see-also}

- [Settings](/reference/system-tables/settings)
- [Configuration Files](/concepts/features/configuration/server-config/configuration-files)
- [Server Settings](/reference/settings/server-settings/settings)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"session_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains information about all successful and failed login and logout events.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.session_log LIMIT 1 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
type:                    LoginSuccess
auth_id:                 45e6bd83-b4aa-4a23-85e6-bd83b4aa1a23
session_id:
event_date:              2021-10-14
event_time:              2021-10-14 20:33:52
event_time_microseconds: 2021-10-14 20:33:52.104247
user:                    default
auth_type:               PLAINTEXT_PASSWORD
profiles:                ['default']
roles:                   []
settings:                [('load_balancing','random'),('max_memory_usage','10000000000')]
client_address:          ::ffff:127.0.0.1
client_port:             38490
interface:               TCP
client_hostname:
client_name:             ClickHouse client
client_revision:         54449
client_version_major:    21
client_version_minor:    10
client_version_patch:    0
failure_reason:
certificate_subjects:    []
certificate_serial:
certificate_issuer:
certificate_not_before:  ᴺᵁᴸᴸ
certificate_not_after:   ᴺᵁᴸᴸ
```

## Reading rotated tables after an upgrade {#reading-rotated-tables-after-an-upgrade}

When a new value is added to the `interface` enumeration, the table already stored on disk keeps the older `Enum8` definition. On the first start of the new version, `system.session_log` is renamed to `system.session_log_<N>` and a fresh table with the current schema is created in its place; the schema of the rotated table is not changed.

If a login was recorded over an interface that was missing from the enumeration of the version that wrote it — for example an `ArrowFlight` login on a version where `ArrowFlight` was absent from the `interface` enumeration — the rotated table contains a raw value that its own enumeration does not define, and a `SELECT` from it throws `Unexpected value 10 in enum`. Extend the enumeration of that table to read it; this is a metadata-only operation which does not rewrite any data:

```sql
ALTER TABLE system.session_log_1
    MODIFY COLUMN interface Enum8('TCP' = 1, 'HTTP' = 2, 'gRPC' = 3, 'MySQL' = 4, 'PostgreSQL' = 5, 'Local' = 6, 'TCP_Interserver' = 7, 'Prometheus' = 8, 'Background' = 9, 'ArrowFlight' = 10);
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"settings",
        {R"DOCS_MD(
## Description {#description}

Contains information about session settings for current user.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

The following example shows how to get information about settings which name contains `min_i`.

```sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_insert_block_size_%'
FORMAT Vertical
```

```text
Row 1:
──────
name:        min_insert_block_size_rows
value:       1048449
changed:     0
description: Sets the minimum number of rows in the block that can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     1048449
alias_for:
is_obsolete: 0
tier:        Production

Row 2:
──────
name:        min_insert_block_size_bytes
value:       268402944
changed:     0
description: Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     268402944
alias_for:
is_obsolete: 0
tier:        Production

Row 3:
──────
name:        min_insert_block_size_rows_for_materialized_views
value:       0
changed:     0
description: Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](/reference/statements/create/view). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

## See Also {#see-also}

- [min_insert_block_size_rows](/reference/settings/session-settings/min-insert#min_insert_block_size_rows)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:
is_obsolete: 0
tier:        Production

Row 4:
──────
name:        min_insert_block_size_bytes_for_materialized_views
value:       0
changed:     0
description: Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](/reference/statements/create/view). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

## See Also {#see-also}

- [min_insert_block_size_bytes](/reference/settings/session-settings/min-insert#min_insert_block_size_bytes)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:
is_obsolete: 0
tier:        Production
 ```

Using of `WHERE changed` can be useful, for example, when you want to check:

- Whether settings in configuration files are loaded correctly and are in use.
- Settings that changed in the current session.

```sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

## See Also {#see-also}

- [Settings](/reference/system-tables/overview#system-tables-introduction)
- [Permissions for Queries](/concepts/features/configuration/settings/permissions-for-queries)
- [Constraints on Settings](/concepts/features/configuration/settings/constraints-on-settings)
- [SHOW SETTINGS](/reference/statements/show#show-settings) statement
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"settings_changes",
        {R"DOCS_MD(
## Description {#description}

Contains information about setting changes in previous ClickHouse versions.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT *
FROM system.settings_changes
WHERE version = '23.5'
FORMAT Vertical
```

```text
Row 1:
──────
type:    Core
version: 23.5
changes: [('input_format_parquet_preserve_order','1','0','Allow Parquet reader to reorder rows for better parallelism.'),('parallelize_output_from_storages','0','1','Allow parallelism when executing queries that read from file/url/s3/etc. This may reorder rows.'),('use_with_fill_by_sorting_prefix','0','1','Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently'),('output_format_parquet_compliant_nested_types','0','1','Change an internal field name in output Parquet file schema.')]
```

## See Also {#see-also}

- [Settings](/reference/system-tables/overview#system-tables-introduction)
- [system.settings](/reference/system-tables/settings)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"settings_profile_elements",
        {R"DOCS_MD(
## Description {#description}

Describes the content of the settings profile:

- Сonstraints.
- Roles and users that the setting applies to.
- Parent settings profiles.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"settings_profiles",
        {R"DOCS_MD(
## Description {#description}

Contains properties of configured setting profiles.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See Also {#see-also}

- [SHOW PROFILES](/reference/statements/show#show-profiles)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"stack_trace",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains stack traces of all server threads. Allows developers to introspect the server state.

To analyze stack frames, use the `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` and `demangle` [introspection functions](/reference/functions/regular-functions/introspection).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

Enabling introspection functions:

```sql
SET allow_introspection_functions = 1;
```

Getting symbols from ClickHouse object files:

```sql
WITH arrayMap(x -> demangle(addressToSymbol(x)), trace) AS all SELECT thread_name, thread_id, query_id, arrayStringConcat(all, '\n') AS res FROM system.stack_trace LIMIT 1;
```

```text
Row 1:
──────
thread_name: QueryPipelineEx
thread_id:   743490
query_id:    dc55a564-febb-4e37-95bb-090ef182c6f1
res:         memcpy
large_ralloc
arena_ralloc
do_rallocx
Allocator<true, true>::realloc(void*, unsigned long, unsigned long, unsigned long)
HashTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>::resize(unsigned long, unsigned long)
void DB::Aggregator::executeImplBatch<false, false, true, DB::AggregationMethodOneNumber<unsigned long, HashMapTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>, true, false>>(DB::AggregationMethodOneNumber<unsigned long, HashMapTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>, true, false>&, DB::AggregationMethodOneNumber<unsigned long, HashMapTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>, true, false>::State&, DB::Arena*, unsigned long, unsigned long, DB::Aggregator::AggregateFunctionInstruction*, bool, char*) const
DB::Aggregator::executeImpl(DB::AggregatedDataVariants&, unsigned long, unsigned long, std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>&, DB::Aggregator::AggregateFunctionInstruction*, bool, bool, char*) const
DB::Aggregator::executeOnBlock(std::__1::vector<COW<DB::IColumn>::immutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::immutable_ptr<DB::IColumn>>>, unsigned long, unsigned long, DB::AggregatedDataVariants&, std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>&, std::__1::vector<std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>, std::__1::allocator<std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>>>&, bool&) const
DB::AggregatingTransform::work()
DB::ExecutionThreadContext::executeTask()
DB::PipelineExecutor::executeStepImpl(unsigned long, std::__1::atomic<bool>*)
void std::__1::__function::__policy_invoker<void ()>::__call_impl<std::__1::__function::__default_alloc_func<DB::PipelineExecutor::spawnThreads()::$_0, void ()>>(std::__1::__function::__policy_storage const*)
ThreadPoolImpl<ThreadFromGlobalPoolImpl<false>>::worker(std::__1::__list_iterator<ThreadFromGlobalPoolImpl<false>, void*>)
void std::__1::__function::__policy_invoker<void ()>::__call_impl<std::__1::__function::__default_alloc_func<ThreadFromGlobalPoolImpl<false>::ThreadFromGlobalPoolImpl<void ThreadPoolImpl<ThreadFromGlobalPoolImpl<false>>::scheduleImpl<void>(std::__1::function<void ()>, Priority, std::__1::optional<unsigned long>, bool)::'lambda0'()>(void&&)::'lambda'(), void ()>>(std::__1::__function::__policy_storage const*)
void* std::__1::__thread_proxy[abi:v15000]<std::__1::tuple<std::__1::unique_ptr<std::__1::__thread_struct, std::__1::default_delete<std::__1::__thread_struct>>, void ThreadPoolImpl<std::__1::thread>::scheduleImpl<void>(std::__1::function<void ()>, Priority, std::__1::optional<unsigned long>, bool)::'lambda0'()>>(void*)
```

Getting filenames and line numbers in ClickHouse source code:

```sql
WITH arrayMap(x -> addressToLine(x), trace) AS all, arrayFilter(x -> x LIKE '%/dbms/%', all) AS dbms SELECT thread_name, thread_id, query_id, arrayStringConcat(notEmpty(dbms) ? dbms : all, '\n') AS res FROM system.stack_trace LIMIT 1;
```

```text
Row 1:
──────
thread_name: clickhouse-serv

thread_id: 686
query_id:  cad353e7-1c29-4b2e-949f-93e597ab7a54
res:       /lib/x86_64-linux-gnu/libc-2.27.so
/build/obj-x86_64-linux-gnu/../src/Storages/System/StorageSystemStackTrace.cpp:182
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/vector:656
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectQuery.cpp:1338
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectQuery.cpp:751
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/optional:224
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectWithUnionQuery.cpp:192
/build/obj-x86_64-linux-gnu/../src/Interpreters/executeQuery.cpp:384
/build/obj-x86_64-linux-gnu/../src/Interpreters/executeQuery.cpp:643
/build/obj-x86_64-linux-gnu/../src/Server/TCPHandler.cpp:251
/build/obj-x86_64-linux-gnu/../src/Server/TCPHandler.cpp:1197
/build/obj-x86_64-linux-gnu/../contrib/poco/Net/src/TCPServerConnection.cpp:57
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/atomic:856
/build/obj-x86_64-linux-gnu/../contrib/poco/Foundation/include/Poco/Mutex_POSIX.h:59
/build/obj-x86_64-linux-gnu/../contrib/poco/Foundation/include/Poco/AutoPtr.h:223
/lib/x86_64-linux-gnu/libpthread-2.27.so
/lib/x86_64-linux-gnu/libc-2.27.so
```

## See Also {#see-also}

- [Introspection Functions](/reference/functions/regular-functions/introspection) — Which introspection functions are available and how to use them.
- [system.trace_log](/reference/system-tables/trace_log) — Contains stack traces collected by the sampling query profiler.
- [arrayMap](/reference/functions/regular-functions/array-functions#arrayMap)) — Description and usage example of the `arrayMap` function.
- [arrayFilter](/reference/functions/regular-functions/array-functions#arrayFilter) — Description and usage example of the `arrayFilter` function.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"stemmers",
        {R"DOCS_MD(
## Description {#description}

Shows all available stemmers.
These can be used in the function [stem](/reference/functions/regular-functions/nlp-functions).

<Info>
**Availability**

`system.stemmers` is present only in ClickHouse builds compiled with the `libstemmer` dependency (`USE_LIBSTEMMER`). On builds without it, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`. You can check whether your build has it enabled with:

```sql
SELECT value FROM system.build_options WHERE name = 'USE_LIBSTEMMER';
```
</Info>

Columns:

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.stemmers;
```

```text
 ┌─name───────┐
 │ arabic     │
 │ armenian   │
 │ basque     │
 │ catalan    │
 │ danish     │
 │ dutch      │
 │ english    │
 │ [...]      │
 └────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"storage_policies",
        {R"DOCS_MD(
## Description {#description}

Contains information about storage policies and volumes which are defined in [server configuration](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes_configure).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Volume selection on `INSERT` {#volume-selection-on-insert}

When `INSERT` creates a new data part, ClickHouse picks a destination disk
by trying the rules below in order. The first rule that matches **and can
reserve space for the part** wins; otherwise (rule does not apply, no free
space, or `max_data_part_size` exceeded) evaluation continues with the next
rule.

1. **TTL move rule** — if a `TTL <expr> TO VOLUME 'X'` (or `TO DISK 'X'`)
   clause is already in the past for the rows being inserted, **and**
   `perform_ttl_move_on_insert = 1` (default) on the **TTL destination
   volume** (for `TO DISK 'X'`, the volume containing disk `X`), the part
   is written directly to that destination. If reservation there fails, the
   insert falls back to steps 2–4; a warning is logged but the `INSERT`
   does not fail for this reason alone.
2. **`max_data_part_size`** — a volume rejects parts larger than its
   `max_data_part_size`. This is checked per volume; it does not gate a
   step-1 `TTL ... TO DISK 'X'` reservation, which targets the disk
   directly.
3. **`volume_priority`** — among the remaining volumes, the one with the
   lowest `volume_priority` value is chosen. Volumes without an explicit
   `<volume_priority>` are ordered by their position in the configuration.
4. **`load_balancing`** — once a volume is chosen, the disk inside that
   volume is selected according to its `load_balancing` policy
   (`round_robin` or `least_used`).

<Info>
**Override**

If `min_free_disk_bytes_to_perform_insert` or
`min_free_disk_ratio_to_perform_insert` is non-zero, the precedence above
is bypassed. `INSERT` tries only the volume with the lowest
`volume_priority` and throws `NOT_ENOUGH_SPACE` if no disk in that volume
meets the threshold. Inserts into the `system` database are exempt.
</Info>

<Note>
`perform_ttl_move_on_insert` is read from the **TTL destination** volume,
not from the source volume. For a `TO DISK 'X'` rule, the flag is read
from the volume that contains disk `X`. Setting it on any other volume of
the policy has no effect on the insert path.
</Note>

To force inserts to honour `volume_priority` even when an "already
expired" TTL move rule applies, set `perform_ttl_move_on_insert = 0` on
the TTL destination volume (for `TO DISK 'X'`, on the volume that contains
disk `X`). The part is then written to the priority-N volume first and
moved to the TTL destination by a background move task (observable via
`system.moves`). See the
[`perform_ttl_move_on_insert` setting on the MergeTree engine](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes_configure).
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"symbols",
        {R"DOCS_MD(
## Description {#description}

Contains information for introspection of `clickhouse` binary. It requires the introspection privilege to access.
This table is only useful for C++ experts and ClickHouse engineers.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.symbols WHERE function_id IS NOT NULL LIMIT 5 SETTINGS allow_introspection_functions = 1
```

```text
Row 1:
──────
symbol:           _Z15isClickHouseAppNSt3__117basic_string_viewIcNS_11char_traitsIcEEEERNS_6vectorIPcNS_9allocatorIS5_EEEE
symbol_demangled: isClickHouseApp(std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::vector<char*, std::__1::allocator<char*>>&)
function_id:      1
address_begin:    219229312 -- 219.23 million
address_end:      219231408 -- 219.23 million

Row 2:
──────
symbol:           main
symbol_demangled: main
function_id:      2
address_begin:    219231872 -- 219.23 million
address_end:      219233485 -- 219.23 million

Row 3:
──────
symbol:           _ZN12_GLOBAL__N_19printHelpEiPPc
symbol_demangled: (anonymous namespace)::printHelp(int, char**)
function_id:      3
address_begin:    219233536 -- 219.23 million
address_end:      219233902 -- 219.23 million

Row 4:
──────
symbol:           _ZNSt3__110filesystem4pathC2B8se210105IPcvEERKT_NS1_6formatE
symbol_demangled: std::__1::filesystem::path::path[abi:se210105]<char*, void>(char* const&, std::__1::filesystem::path::format)
function_id:      4
address_begin:    219234496 -- 219.23 million
address_end:      219234620 -- 219.23 million

Row 5:
──────
symbol:           _ZNSt3__113unordered_setINS_17basic_string_viewIcNS_11char_traitsIcEEEENS_4hashIS4_EENS_8equal_toIS4_EENS_9allocatorIS4_EEEC2ESt16initializer_listIS4_E
symbol_demangled: std::__1::unordered_set<std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::hash<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::equal_to<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::allocator<std::__1::basic_string_view<char, std::__1::char_traits<char>>>>::unordered_set(std::initializer_list<std::__1::basic_string_view<char, std::__1::char_traits<char>>>)
function_id:      5
address_begin:    219235584 -- 219.24 million
address_end:      219235708 -- 219.24 million
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"table_engines",
        {R"DOCS_MD(
## Description {#description}

Contains description of table engines supported by server and their feature support information.

This table contains the following columns (the column type is shown in brackets):

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT
    name,
    supports_settings,
    supports_skipping_indices,
    supports_sort_order,
    supports_ttl,
    supports_replication,
    supports_deduplication,
    supports_parallel_insert
FROM system.table_engines
WHERE name IN ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

```text title="Response"
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┬─supports_parallel_insert─┐
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │                        1 │
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │                        0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │                        1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┴──────────────────────────┘
```

## See also {#see-also}

- MergeTree family [query clauses](/reference/engines/table-engines/mergetree-family/mergetree#mergetree-query-clauses)
- Kafka [settings](/reference/engines/table-engines/integrations/kafka#creating-a-table)
- Join [settings](/reference/engines/table-engines/special/join#join-limitations-and-settings)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"table_functions",
        {R"DOCS_MD(
## Description {#description}

Contains a list of all available table functions with their descriptions.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"tables",
        {R"DOCS_MD(
## Description {#description}

Contains metadata of each table that the server knows about.

[Detached](/reference/statements/detach) tables are not shown in `system.tables`.

[Temporary tables](/reference/statements/create/table/temporary-table) are visible in the `system.tables` only in those session where they have been created. They are shown with the empty `database` field and with the `is_temporary` flag switched on.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
name:                       t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/store/81b/81b1c20a-b7c6-4116-a2ce-7583fb6b6736/']
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
metadata_modification_time: 2021-01-25 19:14:32
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE base.t1 (`n` UInt64) ENGINE = MergeTree ORDER BY n
engine_full:                MergeTree ORDER BY n
as_select:                  SELECT database AS table_catalog
partition_key:
sorting_key:                n
primary_key:                n
sampling_key:
skipping_indices_types:     []
storage_policy:             default
total_rows:                 1
total_bytes:                99
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []

Row 2:
──────
database:                   default
name:                       53r93yleapyears
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/data/default/53r93yleapyears/']
metadata_path:              /var/lib/clickhouse/metadata/default/53r93yleapyears.sql
metadata_modification_time: 2020-09-23 09:05:36
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE default.`53r93yleapyears` (`id` Int8, `febdays` Int8) ENGINE = MergeTree ORDER BY id
engine_full:                MergeTree ORDER BY id
as_select:                  SELECT name AS catalog_name
partition_key:
sorting_key:                id
primary_key:                id
sampling_key:
skipping_indices_types:     []
storage_policy:             default
total_rows:                 2
total_bytes:                155
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"text_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains logging entries. The logging level which goes to this table can be limited to the `text_log.level` server setting.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.text_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2020-09-10
event_time:              2020-09-10 11:23:07
event_time_microseconds: 2020-09-10 11:23:07.871397
thread_name:             clickhouse-serv
thread_id:               564917
level:                   Information
query_id:
logger_name:             DNSCacheUpdater
message:                 Update period 15 seconds
revision:                54440
source_file:             /ClickHouse/src/Interpreters/DNSCacheUpdater.cpp; void DB::DNSCacheUpdater::start()
source_line:             45
message_format_string:   Update period {} seconds
value1:                  15
value2:
value3:
value4:
value5:
value6:
value7:
value8:
value9:
value10:
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"time_zones",
        {R"DOCS_MD(
## Description {#description}

Contains a list of time zones that are supported by the ClickHouse server. This list of timezones might vary depending on the version of ClickHouse.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.time_zones LIMIT 10
```

```text
┌─time_zone──────────┐
│ Africa/Abidjan     │
│ Africa/Accra       │
│ Africa/Addis_Ababa │
│ Africa/Algiers     │
│ Africa/Asmara      │
│ Africa/Asmera      │
│ Africa/Bamako      │
│ Africa/Bangui      │
│ Africa/Banjul      │
│ Africa/Bissau      │
└────────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"tokenizers",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Shows all available tokenizers.
These can be used in functions [tokens](/reference/functions/regular-functions/splitting-merging-functions#tokens), [hasAllTokens](/reference/functions/regular-functions/string-search-functions#hasAllTokens), [hasAnyTokens](/reference/functions/regular-functions/string-search-functions#hasAnyTokens), and the [text index](/reference/engines/table-engines/mergetree-family/textindexes).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.tokenizers;
```

```text
┌─name────────────┐
│ ngrams          │
│ splitByNonAlpha │
│ sparseGrams     │
│ tokenbf_v1      │
│ ngrambf_v1      │
│ array           │
│ splitByString   │
│ sparse_grams    │
│ asciiCJK        │
│ icu             │
│ japanese        │
└─────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"trace_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Contains stack traces collected by the [sampling query profiler](/concepts/features/performance/troubleshoot/sampling-query-profiler).

ClickHouse creates this table when the [trace_log](/reference/settings/server-settings/settings/other#trace_log) server configuration section is set. Also see settings: [query_profiler_real_time_period_ns](/reference/settings/session-settings/query-profiler#query_profiler_real_time_period_ns), [query_profiler_cpu_time_period_ns](/reference/settings/session-settings/query-profiler#query_profiler_cpu_time_period_ns), [memory_profiler_step](/reference/settings/session-settings/memory-profiler#memory_profiler_step),
[memory_profiler_sample_probability](/reference/settings/session-settings/memory-profiler#memory_profiler_sample_probability), [trace_profile_events](/reference/settings/session-settings/trace-profile-events#trace_profile_events).

When symbolization is enabled (the default), the demangled function names and source locations are already available in the `symbols` and `lines` columns, so you can analyze the logs directly without introspection functions. The `symbolize` setting applies to profiler-collected trace types; rows with the `Instrumentation` trace type are symbolized regardless of it. Symbolization is supported on ELF platforms (such as Linux) and macOS; on FreeBSD the `symbols` and `lines` columns are always empty. Function names in `symbols` come from the binary's symbol table and are available by default, while source locations in `lines` are best-effort: they require debug info (a `.dSYM` bundle on macOS) and, on ELF platforms, are resolved only for frames inside the main ClickHouse binary; unresolved frames have empty `lines` entries.
If symbolization is disabled, or you want to resolve the raw addresses in the `trace` column on the fly (for example, to expand inline frames), use the `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` and `demangle` introspection functions. These functions are available on the same platforms as symbolization (ELF platforms such as Linux, and macOS); on FreeBSD they are not compiled in either, so the addresses in `trace` have to be resolved outside the server.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Symbolization can be enabled or disabled with the `symbolize` setting under `trace_log` in the server's configuration file. It is enabled by default. The setting applies to profiler-collected trace types; rows with the `Instrumentation` trace type are symbolized regardless of it.

## Example {#example}

```sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-11
event_time:              2025-11-11 11:53:59
event_time_microseconds: 2025-11-11 11:53:59.128333
timestamp_ns:            1762862039128333000
revision:                54504
trace_type:              Instrumentation
cpu_id:                  19
thread_id:               3166432 -- 3.17 million
query_id:                ef462508-e189-4ea2-b231-4489506728e8
trace:                   [350594916,447733712,447742095,447727324,447726659,221642873,450882315,451852359,451905441,451885554,512404306,512509092,612861767,612863269,612466367,612455825,137631896259267,137631896856768]
size:                    0
ptr:                     0
memory_context:          Unknown
memory_blocked_context:  Unknown
event:
increment:               0
symbols:                 ['StackTrace::StackTrace()','DB::InstrumentationManager::createTraceLogElement(DB::InstrumentationManager::InstrumentedPointInfo const&, XRayEntryType, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>) const','DB::InstrumentationManager::profile(XRayEntryType, DB::InstrumentationManager::InstrumentedPointInfo const&)','DB::InstrumentationManager::dispatchHandlerImpl(int, XRayEntryType)','DB::InstrumentationManager::dispatchHandler(int, XRayEntryType)','__xray_FunctionEntry','DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)','DB::logQueryStart(std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>> const&, std::__1::shared_ptr<DB::Context> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, unsigned long, std::__1::shared_ptr<DB::IAST> const&, DB::QueryPipeline const&, DB::IInterpreter const*, bool, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, bool)','DB::executeQueryImpl(char const*, char const*, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum, std::__1::unique_ptr<DB::ReadBuffer, std::__1::default_delete<DB::ReadBuffer>>&, std::__1::shared_ptr<DB::IAST>&, std::__1::shared_ptr<DB::ImplicitTransactionControlExecutor>, std::__1::function<void ()>)','DB::executeQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum)','DB::TCPHandler::runImpl()','DB::TCPHandler::run()','Poco::Net::TCPServerConnection::start()','Poco::Net::TCPServerDispatcher::run()','Poco::PooledThread::run()','Poco::ThreadImpl::runnableEntry(void*)','start_thread','__clone3']
lines:                   ['./build/../src/Common/StackTrace.cpp:395','./src/Common/StackTrace.h:62','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:738','./build/./src/Interpreters/InstrumentationManager.cpp:257','./build/./src/Interpreters/InstrumentationManager.cpp:225','','./build/./src/Interpreters/QueryMetricLog.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:667','./build/./src/Interpreters/executeQuery.cpp:0','./build/./src/Interpreters/executeQuery.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:744','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:583','./build/../base/poco/Net/src/TCPServerConnection.cpp:54','../contrib/llvm-project/libcxx/include/__memory/unique_ptr.h:80','./build/../base/poco/Foundation/src/ThreadPool.cpp:219','../base/poco/Foundation/include/Poco/AutoPtr.h:77','','']
function_id:             231255
function_name:           DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
handler:                 profile
entry_type:              Exit
duration_nanoseconds:   58435
```

## Converting to Chrome Event Trace Format {#chrome-event-trace-format}

The profiling data can be converted to Chrome's Event Trace Format with the following query. Save the query to a `chrome_trace.sql` file:

```sql
WITH traces AS (
    SELECT * FROM system.trace_log
    WHERE event_date >= today() AND trace_type = 'Instrumentation' AND handler = 'profile'
    ORDER BY event_time, entry_type
)
SELECT
    format(
        '{{"traceEvents": [{}\n]}}',
        arrayStringConcat(
            groupArray(
                format(
                    '\n{{"name": "{}", "cat": "clickhouse", "ph": "{}", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}", "cpu_id": {}, "stack": [{}]}}}},',
                    function_name,
                    if(entry_type = 0, 'B', 'E'),
                    timestamp_ns/1000,
                    toString(thread_id),
                    query_id,
                    cpu_id,
                    arrayStringConcat(arrayMap((x, y) -> concat('"', x, ': ', y, '", '), lines, symbols))
                )
            )
        )
    )
FROM traces;
```

And executing it with ClickHouse Client to export it to a `trace.json` file that we can import either with [Perfetto](https://ui.perfetto.dev/) or [speedscope](https://www.speedscope.app/).

```bash
echo $(clickhouse client --query "$(cat chrome_trace.sql)") > trace.json
```

We can omit the stack part if we want a more compact but less informative trace.

## See Also {#see-also}

- [SYSTEM INSTRUMENT](/reference/statements/system#instrument) — Add or remove instrumentation points.
- [system.instrumentation](/reference/system-tables/instrumentation) — Inspect instrumented points.
- [system.symbols](/reference/system-tables/symbols) — Inspect symbols to add instrumentation points.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"transactions",
        {R"DOCS_MD(
## Description {#description}

Contains a list of transactions and their state.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"transactions_info_log",
        {R"DOCS_MD(
Contains information about all transactions executed on a current server.

It is safe to truncate or drop this table at any time.

Columns:

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"unicode",
        {R"DOCS_MD(
## Description {#description}

The `system.unicode` table is a virtual table that provides information about Unicode characters and their properties(https://unicode-org.github.io/icu/userguide/strings/properties.html). This table is generated on-the-fly.

The property names of Unicode code points from ICU are converted to `snake_case` for use as column names.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}
```sql
SELECT * FROM system.unicode WHERE code_point = 'a' LIMIT 1;
```

```text
Row 1:
──────
code_point:                      a
code_point_value:                97
notation:                        U+0061
alphabetic:                      1
ascii_hex_digit:                 1
bidi_control:                    0
bidi_mirrored:                   0
dash:                            0
default_ignorable_code_point:    0
deprecated:                      0
diacritic:                       0
extender:                        0
full_composition_exclusion:      0
grapheme_base:                   1
grapheme_extend:                 0
grapheme_link:                   0
hex_digit:                       1
hyphen:                          0
id_continue:                     1
id_start:                        1
ideographic:                     0
ids_binary_operator:             0
ids_trinary_operator:            0
join_control:                    0
logical_order_exception:         0
lowercase:                       1
math:                            0
noncharacter_code_point:         0
quotation_mark:                  0
radical:                         0
soft_dotted:                     0
terminal_punctuation:            0
unified_ideograph:               0
uppercase:                       0
white_space:                     0
xid_continue:                    1
xid_start:                       1
case_sensitive:                  1
sentence_terminal:               0
variation_selector:              0
nfd_inert:                       1
nfkd_inert:                      1
nfc_inert:                       0
nfkc_inert:                      0
segment_starter:                 1
pattern_syntax:                  0
pattern_white_space:             0
alnum:                           1
blank:                           0
graph:                           1
print:                           1
xdigit:                          1
cased:                           1
case_ignorable:                  0
changes_when_lowercased:         0
changes_when_uppercased:         1
changes_when_titlecased:         1
changes_when_casefolded:         0
changes_when_casemapped:         1
changes_when_nfkc_casefolded:    0
emoji:                           0
emoji_presentation:              0
emoji_modifier:                  0
emoji_modifier_base:             0
emoji_component:                 0
regional_indicator:              0
prepended_concatenation_mark:    0
extended_pictographic:           0
basic_emoji:                     0
emoji_keycap_sequence:           0
rgi_emoji_modifier_sequence:     0
rgi_emoji_flag_sequence:         0
rgi_emoji_tag_sequence:          0
rgi_emoji_zwj_sequence:          0
rgi_emoji:                       0
ids_unary_operator:              0
id_compat_math_start:            0
id_compat_math_continue:         0
bidi_class:                      0
block:                           1
canonical_combining_class:       0
decomposition_type:              0
east_asian_width:                4
general_category:                2
joining_group:                   0
joining_type:                    0
line_break:                      2
numeric_type:                    0
script:                          25
hangul_syllable_type:            0
nfd_quick_check:                 1
nfkd_quick_check:                1
nfc_quick_check:                 1
nfkc_quick_check:                1
lead_canonical_combining_class:  0
trail_canonical_combining_class: 0
grapheme_cluster_break:          0
sentence_break:                  4
word_break:                      1
bidi_paired_bracket_type:        0
indic_positional_category:       0
indic_syllabic_category:         0
vertical_orientation:            0
identifier_status:               1
general_category_mask:           4
numeric_value:                   0
age:                             1.1
bidi_mirroring_glyph:            a
case_folding:                    a
lowercase_mapping:               a
name:                            LATIN SMALL LETTER A
simple_case_folding:             a
simple_lowercase_mapping:        a
simple_titlecase_mapping:        A
simple_uppercase_mapping:        A
titlecase_mapping:               A
uppercase_mapping:               A
bidi_paired_bracket:             a
script_extensions:               ['Latin']
identifier_type:                 ['Recommended']

```

```sql
SELECT code_point, code_point_value, notation FROM system.unicode WHERE code_point = '😂';
```
```text
   ┌─code_point─┬─code_point_value─┬─notation─┐
1. │ 😂          │           128514 │ U+1F602  │
   └────────────┴──────────────────┴──────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"user_defined_functions",
        {R"DOCS_MD(
## Description {#description}

Contains loading status, error information, and configuration metadata for [User-Defined Functions (UDFs)](/reference/functions/regular-functions/udf).

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

View all UDFs and their loading status:

```sql
SELECT
    name,
    load_status,
    type,
    command,
    return_type,
    argument_types
FROM system.user_defined_functions
FORMAT Vertical;
```

```response
Row 1:
──────
name:           my_sum_udf
load_status:    Success
type:           executable
command:        /var/lib/clickhouse/user_scripts/sum.py
return_type:    UInt64
argument_types: ['UInt64','UInt64']
```

Find failed UDFs:

```sql
SELECT
    name,
    loading_error_message
FROM system.user_defined_functions
WHERE load_status = 'Failed';
```

## See Also {#see-also}

-   [User-Defined Functions](/reference/functions/regular-functions/udf) — How to create and configure UDFs.
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"user_directories",
        {R"DOCS_MD(
## Description {#description}

Contains the information about configured user directories - directories on the file system from which ClickHouse server is allowed to read user provided data.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"user_processes",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This system table can be used to get overview of memory usage and ProfileEvents of users.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT * FROM system.user_processes LIMIT 10 FORMAT Vertical;
```

```response
Row 1:
──────
user:              default
memory_usage:      9832
peak_memory_usage: 9832
ProfileEvents:     {'Query':5,'SelectQuery':5,'QueriesWithSubqueries':38,'SelectQueriesWithSubqueries':38,'QueryTimeMicroseconds':842048,'SelectQueryTimeMicroseconds':842048,'ReadBufferFromFileDescriptorRead':6,'ReadBufferFromFileDescriptorReadBytes':234,'IOBufferAllocs':3,'IOBufferAllocBytes':98493,'ArenaAllocChunks':283,'ArenaAllocBytes':1482752,'FunctionExecute':670,'TableFunctionExecute':16,'DiskReadElapsedMicroseconds':19,'NetworkSendElapsedMicroseconds':684,'NetworkSendBytes':139498,'SelectedRows':6076,'SelectedBytes':685802,'ContextLock':1140,'RWLockAcquiredReadLocks':193,'RWLockReadersWaitMilliseconds':4,'RealTimeMicroseconds':1585163,'UserTimeMicroseconds':889767,'SystemTimeMicroseconds':13630,'SoftPageFaults':1947,'OSCPUWaitMicroseconds':6,'OSCPUVirtualTimeMicroseconds':903251,'OSReadChars':28631,'OSWriteChars':28888,'QueryProfilerRuns':3,'LogTrace':79,'LogDebug':24}

1 row in set. Elapsed: 0.010 sec.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"user_query_log",
        {R"DOCS_MD(
## Description {#description}

Shows the current user their own query log records. It reads the query log table configured by the `query_log.database` and `query_log.table` server settings (`system.query_log` by default) and returns only the rows whose initiating user is equal to `currentUser()` (the initiating user is taken from `initial_user` when it is set, otherwise from `user`).

Unlike the query log table itself, `system.user_query_log` can be read without any grants, so users can inspect their own queries without being given access to the queries of others.

This is only supported when the query log is stored locally. If `query_log.engine` is configured as `Distributed` or any other engine that delegates reads to another server, `system.user_query_log` refuses to read from it and throws an exception, because the required access check cannot be enforced across a ClickHouse-protocol server boundary. In that case, disable the table with `query_log.enable_user_query_log = 0`.

The table can be disabled with the `query_log.enable_user_query_log` server setting. If the query log is not configured, or its table has not been created yet, `system.user_query_log` is empty.

Conditions on the partition and key columns of the query log (`event_date`, `event_time`, `query_start_time`, `query_id`, `type`, and similar scalar columns) compared with constants are pushed down to the backing query log table, so ordinary lookups such as the example below keep partition pruning and do not scan the whole retained log.

<Warning>
If a table named `system.user_query_log` was created before upgrading to a ClickHouse version that provides this table, the server will not start until the existing table is renamed or dropped, or `query_log.enable_user_query_log` is set to `0`.
</Warning>

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT
    query_start_time,
    query_duration_ms,
    query
FROM system.user_query_log
ORDER BY query_start_time DESC
LIMIT 10;
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"users",
        {R"DOCS_MD(
## Description {#description}

Contains a list of [user accounts](/concepts/features/security/access-rights#user-account-management) configured on the server.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## See also {#see-also}

- [SHOW USERS](/reference/statements/show#show-users)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"view_refreshes",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

Information about [Refreshable Materialized Views](/reference/statements/create/view#refreshable-materialized-view). Contains all refreshable materialized views, regardless of whether there's a refresh in progress or not.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT
    database,
    view,
    status,
    last_refresh_time,
    next_refresh_time
FROM system.view_refreshes

┌─database─┬─view───────────────────────┬─status────┬───last_refresh_time─┬───next_refresh_time─┐
│ default  │ hello_documentation_reader │ Scheduled │ 2023-12-01 01:24:00 │ 2023-12-01 01:25:00 │
└──────────┴────────────────────────────┴───────────┴─────────────────────┴─────────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"warnings",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table shows warnings about the ClickHouse server.
Warnings of the same type are combined into a single warning.
For example, if the number N of attached databases exceeds a configurable threshold T, a single entry containing the current value N is shown instead of N separate entries.
If current value drops below the threshold, the entry is removed from the table.

The table can be configured with these settings:

- [max_table_num_to_warn](/reference/settings/server-settings/settings/max-table#max_table_num_to_warn)
- [max_database_num_to_warn](/reference/settings/server-settings/settings/max-database#max_database_num_to_warn)
- [max_dictionary_num_to_warn](/reference/settings/server-settings/settings/max-dictionary#max_dictionary_num_to_warn)
- [max_view_num_to_warn](/reference/settings/server-settings/settings/max-view#max_view_num_to_warn)
- [max_part_num_to_warn](/reference/settings/server-settings/settings/max#max_part_num_to_warn)
- [max_pending_mutations_to_warn](/reference/settings/server-settings/settings/max-pending#max_pending_mutations_to_warn)
- [max_pending_mutations_execution_time_to_warn](/reference/settings/server-settings/settings/max-pending#max_pending_mutations_execution_time_to_warn)
- [max_named_collection_num_to_warn](/reference/settings/server-settings/settings/max-named#max_named_collection_num_to_warn)
- [resource_overload_warnings](/concepts/features/configuration/settings/server-overload#resource-overload-warnings)

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}

## Example {#example}

```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"workloads",
        {R"DOCS_MD(
## Description {#description}

Contains information for [workloads](/concepts/features/configuration/server-config/workload-scheduling#workload_entity_storage) residing on the local server. The table contains a row for every workload.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql
SELECT *
FROM system.workloads
FORMAT Vertical
```

```text
Row 1:
──────
name:         production
parent:       all
create_query: CREATE WORKLOAD production IN `all` SETTINGS weight = 9

Row 2:
──────
name:         development
parent:       all
create_query: CREATE WORKLOAD development IN `all`

Row 3:
──────
name:         all
parent:
create_query: CREATE WORKLOAD `all`
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zeros",
        {R"DOCS_MD(
## Description {#description}

Produces unlimited number of non-materialized zeros.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zeros_mt",
        {R"DOCS_MD(
## Description {#description}

Multithreaded version of system.zeros.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zookeeper",
        {R"DOCS_MD(
## Description {#description}

The table does not exist unless ClickHouse Keeper or ZooKeeper is configured. The `system.zookeeper` table exposes data from the Keeper clusters defined in the config.
The query must either have a `path =`   condition or a `path IN`  condition set with the `WHERE` clause as shown below. This corresponds to the path of the children that you want to get data for.

The query `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` outputs data for all children on the `/clickhouse` node.
To output data for all root nodes, write path = '/'.
If the path specified in 'path' does not exist, an exception will be thrown.

The query `SELECT * FROM system.zookeeper WHERE path IN ('/', '/clickhouse')` outputs data for all children on the `/` and `/clickhouse` node.
If in the specified 'path' collection has does not exist path, an exception will be thrown.
It can be used to do a batch of Keeper path queries.

The query `SELECT * FROM system.zookeeper WHERE path = '/clickhouse' AND zookeeperName = 'auxiliary_cluster'` outputs data in `auxiliary_cluster` ZooKeeper cluster.
If the specified 'auxiliary_cluster' does not exists, an exception will be thrown.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
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
name:           example01-08-1
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
name:           example01-08-2
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
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zookeeper_connection",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table does not exist if ZooKeeper is not configured. The 'system.zookeeper_connection' table shows current connections to ZooKeeper (including auxiliary ZooKeepers). Each row shows information about one connection.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT * FROM system.zookeeper_connection;
```

```text
┌─name────┬─host──────┬─port─┬─index─┬──────connected_time─┬─session_uptime_elapsed_seconds─┬─is_expired─┬─keeper_api_version─┬─client_id─┬─xid─┬─enabled_feature_flags────────────────────────────────────────────────────┬─availability_zone─┐
│ default │ 127.0.0.1 │ 2181 │     0 │ 2025-04-10 14:30:00 │                            943 │          0 │                  0 │       420 │  69 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS'] │ eu-west-1b        │
└─────────┴───────────┴──────┴───────┴─────────────────────┴────────────────────────────────┴────────────┴────────────────────┴───────────┴─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zookeeper_connection_log",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

The 'system.zookeeper_connection_log' table shows the history of ZooKeeper connections (including auxiliary ZooKeepers). Each row shows information about one event regarding connections.

<Note>
The table doesn't contain events for disconnections caused by server shutdown.
</Note>

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
Example:

```sql
SELECT * FROM system.zookeeper_connection_log;
```

```text
    ┌─hostname─┬─type─────────┬─event_date─┬──────────event_time─┬────event_time_microseconds─┬─name───────────────┬─host─┬─port─┬─index─┬─client_id─┬─keeper_api_version─┬─enabled_feature_flags───────────────────────────────────────────────────────────────────────┬─availability_zone─┬─reason──────────────┐
 1. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:35 │ 2025-05-12 19:49:35.713067 │ zk_conn_log_test_4 │ zoo2 │ 2181 │     0 │        10 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 2. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:23 │ 2025-05-12 19:49:23.981570 │ default            │ zoo1 │ 2181 │     0 │         4 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 3. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:28 │ 2025-05-12 19:49:28.104021 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 4. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.459251 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 5. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.574312 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 6. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909890 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 7. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909895 │ default            │ zoo2 │ 2181 │     0 │         8 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 8. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912010 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 9. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912014 │ zk_conn_log_test_2 │ zoo3 │ 2181 │     0 │         9 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
10. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912061 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Removed from config │
    └──────────┴──────────────┴────────────┴─────────────────────┴────────────────────────────┴────────────────────┴──────┴──────┴───────┴───────────┴────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────┴─────────────────────┘
```
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zookeeper_info",
        {R"DOCS_MD(
import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>

## Description {#description}

This table outputs combined introspection about zookeeper and the nodes are taken from config.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zookeeper_log",
        {R"DOCS_MD(
## Description {#description}

This table contains information about the parameters of the request to the ZooKeeper server and the response from it.

For requests, only columns with request parameters are filled in, and the remaining columns are filled with default values (`0` or `NULL`). When the response arrives, the data from the response is added to the other columns.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}
## Example {#example}

```sql title="Query"
SELECT * FROM system.zookeeper_log WHERE (session_id = '106662742089334927') AND (xid = '10858') FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
hostname:         clickhouse.eu-central1.internal
type:             Request
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.291792
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             0
error:            ᴺᵁᴸᴸ
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:
stat_czxid:       0
stat_mzxid:       0
stat_pzxid:       0
stat_version:     0
stat_cversion:    0
stat_dataLength:  0
stat_numChildren: 0
children:         []

Row 2:
──────
type:             Response
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.292086
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             16926267
error:            ZOK
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:
stat_czxid:       16925469
stat_mzxid:       16925469
stat_pzxid:       16926179
stat_version:     0
stat_cversion:    7
stat_dataLength:  0
stat_numChildren: 7
children:         ['query-0000000006','query-0000000005','query-0000000004','query-0000000003','query-0000000002','query-0000000001','query-0000000000']
```

## See also {#see-also}

- [ZooKeeper](/guides/oss/best-practices/tips#zookeeper)
- [ZooKeeper guide](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
    {"zookeeper_watches",
        {R"DOCS_MD(
## Description {#description}

Shows currently active [watches](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) registered by this ClickHouse server on ZooKeeper nodes (including auxiliary ZooKeepers). Each row represents one watch.

## Columns {#columns}

{{SYSTEM_TABLE_COLUMNS}}

Example:

```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

**See Also**

-   [ZooKeeper](/guides/oss/best-practices/tips#zookeeper)
-   [ZooKeeper guide](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
)DOCS_MD",
         "src/Storages/System/SystemTableDocumentation.inc"}},
