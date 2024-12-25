---
slug: /ja/operations/system-tables/backup_log
---
# backup_log

`BACKUP`および`RESTORE`操作に関するログエントリを含んでいます。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行しているサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — エントリの日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — エントリの日付と時刻。
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — マイクロ秒精度のエントリの時刻。
- `id` ([String](../../sql-reference/data-types/string.md)) — バックアップまたはリストア操作の識別子。
- `name` ([String](../../sql-reference/data-types/string.md)) — バックアップストレージの名前 (`FROM`または`TO`句の内容)。
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — 操作ステータス。可能な値:
    - `'CREATING_BACKUP'`
    - `'BACKUP_CREATED'`
    - `'BACKUP_FAILED'`
    - `'RESTORING'`
    - `'RESTORED'`
    - `'RESTORE_FAILED'`
- `error` ([String](../../sql-reference/data-types/string.md)) —操作が失敗した場合のエラーメッセージ（成功した場合は空文字列）。
- `start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 操作の開始時刻。
- `end_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 操作の終了時刻。
- `num_files` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — バックアップに保存されたファイルの数。
- `total_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — バックアップに保存されたファイルの合計サイズ。
- `num_entries` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — バックアップのエントリ数。フォルダーとして保存されている場合はフォルダー内のファイル数、アーカイブとして保存されている場合はアーカイブ内のファイル数です。インクリメンタルバックアップや空ファイルまたは重複が含まれている場合、これは`num_files`とは異なる場合があります。常に次が成り立ちます: `num_entries <= num_files`。
- `uncompressed_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — バックアップの非圧縮サイズ。
- `compressed_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — バックアップの圧縮サイズ。バックアップがアーカイブとして保存されていない場合は`uncompressed_size`と同じです。
- `files_read` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — リストア操作中に読み取られたファイルの数。
- `bytes_read` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — リストア操作中に読み取られたファイルの総サイズ。

**例**

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

これはシステムテーブル`system.backups`に書き込まれる基本的に同じ情報です:

```sql
SELECT * FROM system.backups ORDER BY start_time
```
```response
┌─id───────────────────────────────────┬─name──────────────────────────┬─status─────────┬─error─┬──────────start_time─┬────────────end_time─┬─num_files─┬─total_size─┬─num_entries─┬─uncompressed_size─┬─compressed_size─┬─files_read─┬─bytes_read─┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ Disk('backups_disk', '1.zip') │ BACKUP_CREATED │       │ 2023-08-19 11:05:21 │ 2023-08-19 11:08:56 │        57 │ 4290364870 │          46 │        4290362365 │      3525068304 │          0 │          0 │
│ cdf1f731-52ef-42da-bc65-2e1bfcd4ce90 │ Disk('backups_disk', '1.zip') │ RESTORED       │       │ 2023-08-19 11:09:19 │ 2023-08-19 11:09:29 │        57 │ 4290364870 │          46 │        4290362365 │      4290362365 │         57 │ 4290364870 │
└──────────────────────────────────────┴───────────────────────────────┴────────────────┴───────┴─────────────────────┴─────────────────────┴───────────┴────────────┴─────────────┴───────────────────┴─────────────────┴────────────┴────────────┘
```

**関連項目**

- [バックアップとリストア](../../operations/backup.md)
