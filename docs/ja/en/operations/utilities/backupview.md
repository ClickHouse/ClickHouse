---
description: 'BACKUP コマンドで作成されたバックアップを分析するための clickhouse_backupview のドキュメント {#clickhouse_backupview}'
slug: /operations/utilities/backupview
title: 'clickhouse_backupview'
doc_type: 'reference'
---

[BACKUP](/ja/operations/backup/overview) コマンドで作成されたバックアップの分析に役立つ Python モジュールです。
主な目的は、実際に復元しなくてもバックアップからいくつかの情報を取得できるようにすることです。

このモジュールは、次のための関数を提供します。

* バックアップに含まれるファイルを列挙する
* バックアップからファイルを読み取る
* バックアップに含まれるデータベース、テーブル、パーツに関する有用な情報を、読みやすい形式で取得する
* バックアップの整合性を確認する

<div id="example">
  ## 例:
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

他の例については、[test](https://github.com/ClickHouse/ClickHouse/blob/master/utils/backupview/test/test.py)を参照してください。