---
slug: /ru/operations/system-tables/backup_log
---
# system.backup_log {#system_tables-backup-log}

Содержит информацию о всех операциях `BACKUP` and `RESTORE`.

Колонки:

- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Дата события.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Время события с точностью до микросекунд.
- `id` ([String](../../sql-reference/data-types/string.md)) — Идентификатор операции.
- `name` ([String](../../sql-reference/data-types/string.md)) — Название хранилища (содержимое секции `FROM` или `TO` в SQL запросе).
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Статус операции. Возможные значения:
    - `'CREATING_BACKUP'`
    - `'BACKUP_CREATED'`
    - `'BACKUP_FAILED'`
    - `'RESTORING'`
    - `'RESTORED'`
    - `'RESTORE_FAILED'`
- `error` ([String](../../sql-reference/data-types/string.md)) — Сообщение об ошибке, при наличии (записи для успешных операций содержат пустую строку).
- `start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Время начала операции.
- `end_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Время завершения операции.
- `num_files` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество файлов, хранимых в бэкапе.
- `total_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Общий размер файлов, хранимых в бэкапе.
- `num_entries` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество позиций в бэкапе, т.е. либо количество файлов в папке (если бэкап хранится в папке), либо количество файлов в архиве (если бэкап хранится в архиве). Это значение не равно `num_files` в случае если это инкрементальный бэкап либо он содержит пустые файлы или дубликаты. Следующее утверждение верно всегда: `num_entries <= num_files`.
- `uncompressed_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Размер бэкапа до сжатия.
- `compressed_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Размер бэкапа после сжатия. Если бэкап не хранится в виде архива, это значение равно `uncompressed_size`.
- `files_read` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество файлов, прочитанных во время операции восстановления.
- `bytes_read` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Общий размер файлов, прочитанных во время операции восстановления.

**Пример**

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
event_date:              2023-08-19
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

Это по сути та же информация, что заносится и в системную таблицу `system.backups`:

```sql
SELECT * FROM system.backups ORDER BY start_time
```
```response
┌─id───────────────────────────────────┬─name──────────────────────────┬─status─────────┬─error─┬──────────start_time─┬────────────end_time─┬─num_files─┬─total_size─┬─num_entries─┬─uncompressed_size─┬─compressed_size─┬─files_read─┬─bytes_read─┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ Disk('backups_disk', '1.zip') │ BACKUP_CREATED │       │ 2023-08-19 11:05:21 │ 2023-08-19 11:08:56 │        57 │ 4290364870 │          46 │        4290362365 │      3525068304 │          0 │          0 │
│ cdf1f731-52ef-42da-bc65-2e1bfcd4ce90 │ Disk('backups_disk', '1.zip') │ RESTORED       │       │ 2023-08-19 11:09:19 │ 2023-08-19 11:09:29 │        57 │ 4290364870 │          46 │        4290362365 │      4290362365 │         57 │ 4290364870 │
└──────────────────────────────────────┴───────────────────────────────┴────────────────┴───────┴─────────────────────┴─────────────────────┴───────────┴────────────┴─────────────┴───────────────────┴─────────────────┴────────────┴────────────┘
```

**См. также**

- [Backup and Restore](../../operations/backup.md)
