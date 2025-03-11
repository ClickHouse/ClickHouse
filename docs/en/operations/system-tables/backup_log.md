---
slug: /en/operations/system-tables/backup_log
---
# backup_log

Contains logging entries with the information about `BACKUP` and `RESTORE` operations.

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date of the entry.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time of the entry.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Time of the entry with microseconds precision.
- `id` ([String](../../sql-reference/data-types/string.md)) — Identifier of the backup or restore operation.
- `name` ([String](../../sql-reference/data-types/string.md)) — Name of the backup storage (the contents of the `FROM` or `TO` clause).
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Operation status. Possible values:
    - `'CREATING_BACKUP'`
    - `'BACKUP_CREATED'`
    - `'BACKUP_FAILED'`
    - `'RESTORING'`
    - `'RESTORED'`
    - `'RESTORE_FAILED'`
- `error` ([String](../../sql-reference/data-types/string.md)) — Error message of the failed operation (empty string for successful operations).
- `start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Start time of the operation.
- `end_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — End time of the operation.
- `num_files` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of files stored in the backup.
- `total_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total size of files stored in the backup.
- `num_entries` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder, or the number of files inside the archive if the backup is stored as an archive. It is not the same as `num_files` if it's an incremental backup or if it contains empty files or duplicates. The following is always true: `num_entries <= num_files`.
- `uncompressed_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Uncompressed size of the backup.
- `compressed_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Compressed size of the backup. If the backup is not stored as an archive it equals to `uncompressed_size`.
- `files_read` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of files read during the restore operation.
- `bytes_read` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total size of files read during the restore operation.

**Example**

```sql
BACKUP TABLE test_db.my_table TO Disk('backups_disk', '1.zip')
```
```response
┌─id───────────────────────────────────┬─status─────────┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```
```sql
SELECT * FROM system.backup_log WHERE id = 'e5b74ecb-f6f1-426a-80be-872f90043885' ORDER BY event_date, event_time_microseconds \G
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
SELECT * FROM system.backup_log WHERE id = 'cdf1f731-52ef-42da-bc65-2e1bfcd4ce90' ORDER BY event_date, event_time_microseconds \G
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
SELECT * FROM system.backups ORDER BY start_time
```
```response
┌─id───────────────────────────────────┬─name──────────────────────────┬─status─────────┬─error─┬──────────start_time─┬────────────end_time─┬─num_files─┬─total_size─┬─num_entries─┬─uncompressed_size─┬─compressed_size─┬─files_read─┬─bytes_read─┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ Disk('backups_disk', '1.zip') │ BACKUP_CREATED │       │ 2023-08-19 11:05:21 │ 2023-08-19 11:08:56 │        57 │ 4290364870 │          46 │        4290362365 │      3525068304 │          0 │          0 │
│ cdf1f731-52ef-42da-bc65-2e1bfcd4ce90 │ Disk('backups_disk', '1.zip') │ RESTORED       │       │ 2023-08-19 11:09:19 │ 2023-08-19 11:09:29 │        57 │ 4290364870 │          46 │        4290362365 │      4290362365 │         57 │ 4290364870 │
└──────────────────────────────────────┴───────────────────────────────┴────────────────┴───────┴─────────────────────┴─────────────────────┴───────────┴────────────┴─────────────┴───────────────────┴─────────────────┴────────────┴────────────┘
```

**See Also**

- [Backup and Restore](../../operations/backup.md)
