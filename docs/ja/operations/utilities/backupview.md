---
slug: /ja/operations/utilities/backupview
title: clickhouse_backupview
---

# clickhouse_backupview {#clickhouse_backupview}

[BACKUP](https://clickhouse.com/docs/ja/operations/backup) コマンドで作成されたバックアップを分析するのに役立つ Python モジュールです。このモジュールの主な目的は、実際にバックアップを復元せずに、バックアップからいくつかの情報を取得できるようにすることです。

このモジュールは以下の機能を提供します：
- バックアップに含まれるファイルを列挙する
- バックアップからファイルを読み取る
- バックアップに含まれるデータベース、テーブル、パーツについて人間が読みやすい形で有用な情報を取得する
- バックアップの整合性を確認する

## 例:

```python
from clickhouse_backupview import open_backup, S3, FileInfo

# バックアップを開きます。ローカルパスも使用できます：
# backup = open_backup("/backups/my_backup_1/")
backup = open_backup(S3("uri", "access_key_id", "secret_access_key"))

# バックアップ内のデータベースのリストを取得します。
print(backup.get_databases()))

# バックアップ内のテーブルのリストを取得し、
# 各テーブルについて作成クエリとパーツやパーティションのリストを取得します。
for db in backup.get_databases():
    for tbl in backup.get_tables(database=db):
        print(backup.get_create_query(database=db, table=tbl))
        print(backup.get_partitions(database=db, table=tbl))
        print(backup.get_parts(database=db, table=tbl))

# バックアップからすべてを抽出します。
backup.extract_all(table="mydb.mytable", out='/tmp/my_backup_1/all/')

# 特定のテーブルのデータを抽出します。
backup.extract_table_data(table="mydb.mytable", out='/tmp/my_backup_1/mytable/')

# 単一のパーティションを抽出します。
backup.extract_table_data(table="mydb.mytable", partition="202201", out='/tmp/my_backup_1/202201/')

# 単一のパートを抽出します。
backup.extract_table_data(table="mydb.mytable", part="202201_100_200_3", out='/tmp/my_backup_1/202201_100_200_3/')
```

詳細な例は、[テスト](https://github.com/ClickHouse/ClickHouse/blob/master/utils/backupview/test/test.py)をご覧ください。
