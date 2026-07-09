---
description: 'clickhouse_backupview 文档 {#clickhouse_backupview}'
slug: /operations/utilities/backupview
title: 'clickhouse_backupview'
doc_type: 'reference'
---

用于帮助分析通过 [BACKUP](/zh/operations/backup/overview) 命令创建的 Backup 的 Python 模块。
其主要目的是在无需实际恢复的情况下，从 Backup 中获取一些信息。

此模块提供以下函数：

* 枚举 Backup 中包含的文件
* 从 Backup 中读取文件
* 以可读形式获取 Backup 中包含的数据库、表和 parts 的有用信息
* 检查 Backup 的完整性

<div id="example">
  ## 示例：
</div>

```python
from clickhouse_backupview import open_backup, S3, FileInfo

# Open a backup. We could also use a local path:
# backup = open_backup("/backups/my_backup_1/")
backup = open_backup(S3("uri", "access_key_id", "secret_access_key"))

# Get a list of databasess inside the backup.
print(backup.get_databases()))

# Get a list of tables inside the backup,
# and for each table its create query and a list of parts and partitions.
for db in backup.get_databases():
    for tbl in backup.get_tables(database=db):
        print(backup.get_create_query(database=db, table=tbl))
        print(backup.get_partitions(database=db, table=tbl))
        print(backup.get_parts(database=db, table=tbl))

# Extract everything from the backup.
backup.extract_all(table="mydb.mytable", out='/tmp/my_backup_1/all/')

# Extract the data of a specific table.
backup.extract_table_data(table="mydb.mytable", out='/tmp/my_backup_1/mytable/')

# Extract a single partition.
backup.extract_table_data(table="mydb.mytable", partition="202201", out='/tmp/my_backup_1/202201/')

# Extract a single part.
backup.extract_table_data(table="mydb.mytable", part="202201_100_200_3", out='/tmp/my_backup_1/202201_100_200_3/')
```

更多示例请参阅 [test](https://github.com/ClickHouse/ClickHouse/blob/master/utils/backupview/test/test.py)。