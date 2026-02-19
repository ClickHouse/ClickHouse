---
slug: /ru/operations/utilities/backupview
title: clickhouse_backupview
---

# clickhouse_backupview {#clickhouse_backupview}

Модуль на Питоне для анализа бэкапов, созданных командой [BACKUP](https://clickhouse.com/docs/ru/operations/backup)
Главная идея этого модуля была в том, чтобы позволить извлечение информации из бэкапа без выполнения команды RESTORE.

Этот модуль содержит функции для
- получения списка файлов внутри бэкапа
- чтения файлов из бэкапа
- получения информации в читаемом виде о базах данных, таблицах, партах, содержащихся в бэкапе
- проверки целостности бэкапа

## Пример:

```python
from clickhouse_backupview import open_backup, S3, FileInfo

# Открыть бэкап. Можно также использовать локальный путь:
# backup = open_backup("/backups/my_backup_1/")
backup = open_backup(S3("uri", "access_key_id", "secret_access_key"))

# Получить список баз данных внутри бэкапа.
print(backup.get_databases()))

# Получить список таблиц внутри бэкапа,
# и для каждой таблицы получить ее определение а также список партов и партиций.
for db in backup.get_databases():
    for tbl in backup.get_tables(database=db):
        print(backup.get_create_query(database=db, table=tbl))
        print(backup.get_partitions(database=db, table=tbl))
        print(backup.get_parts(database=db, table=tbl))

# Извлечь все содержимое бэкапа.
backup.extract_all(table="mydb.mytable", out='/tmp/my_backup_1/all/')

# Извлечь данные конкретной таблицы.
backup.extract_table_data(table="mydb.mytable", out='/tmp/my_backup_1/mytable/')

# Извлечь одну партицию из бэкапа.
backup.extract_table_data(table="mydb.mytable", partition="202201", out='/tmp/my_backup_1/202201/')

# Извлечь один парт из бэкапа.
backup.extract_table_data(table="mydb.mytable", part="202201_100_200_3", out='/tmp/my_backup_1/202201_100_200_3/')
```

Больше примеров смотрите в [тесте](https://github.com/ClickHouse/ClickHouse/blob/master/utils/backupview/test/test.py).
