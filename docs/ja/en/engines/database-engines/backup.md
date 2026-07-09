---
description: 'バックアップからテーブル/データベースを読み取り専用モードで即座にアタッチできます。'
sidebar_label: 'Backup'
sidebar_position: 60
slug: /engines/database-engines/backup
title: 'Backup'
doc_type: 'reference'
---

Backupデータベースでは、[バックアップ](/ja/operations/backup/overview)からテーブル/データベースを読み取り専用モードで即座にアタッチできます。

Backupデータベースは、インクリメンタル バックアップとインクリメンタルでないバックアップの両方に対応しています。

<div id="creating-a-database">
  ## データベースの作成
</div>

```sql
CREATE DATABASE backup_database
ENGINE = Backup('database_name_inside_backup', Disk('disk_name', 'backup_name'))
```

バックアップの保存先には、`Disk`、`S3`、`File` など、有効な任意のバックアップ[destination](/ja/operations/backup/disk#configure-backup-destinations-for-disk)を指定できます。これは関数形式で渡します。たとえば `Disk('disk_name', 'backup_name')` のように指定します。

**エンジンパラメータ**

* `database_name_inside_backup` — バックアップ内のデータベース名。
* `backup_destination` — バックアップの保存先。

<div id="usage-example">
  ## 使用例
</div>

`Disk` をバックアップの保存先とする例を見てみましょう。まず、`storage.xml` でバックアップ用のディスクを設定します。

```xml
<storage_configuration>
    <disks>
        <backups>
            <type>local</type>
            <path>/home/ubuntu/ClickHouseWorkDir/backups/</path>
        </backups>
    </disks>
</storage_configuration>
<backups>
    <allowed_disk>backups</allowed_disk>
    <allowed_path>/home/ubuntu/ClickHouseWorkDir/backups/</allowed_path>
</backups>
```

使用例です。テスト用のデータベースとテーブルを作成し、いくつかデータを挿入してから、バックアップを作成しましょう。

```sql
CREATE DATABASE test_database;

CREATE TABLE test_database.test_table_1 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
INSERT INTO test_database.test_table_1 VALUES (0, 'test_database.test_table_1');

CREATE TABLE test_database.test_table_2 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
INSERT INTO test_database.test_table_2 VALUES (0, 'test_database.test_table_2');

CREATE TABLE test_database.test_table_3 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
INSERT INTO test_database.test_table_3 VALUES (0, 'test_database.test_table_3');

BACKUP DATABASE test_database TO Disk('backups', 'test_database_backup');
```

これで `test_database_backup` バックアップの準備ができたので、データベース Backup を作成しましょう:

```sql
CREATE DATABASE test_database_backup ENGINE = Backup('test_database', Disk('backups', 'test_database_backup'));
```

これで、データベース内の任意のテーブルに対してクエリを実行できます。

```sql
SELECT id, value FROM test_database_backup.test_table_1;

┌─id─┬─value──────────────────────┐
│  0 │ test_database.test_table_1 │
└────┴────────────────────────────┘

SELECT id, value FROM test_database_backup.test_table_2;

┌─id─┬─value──────────────────────┐
│  0 │ test_database.test_table_2 │
└────┴────────────────────────────┘

SELECT id, value FROM test_database_backup.test_table_3;

┌─id─┬─value──────────────────────┐
│  0 │ test_database.test_table_3 │
└────┴────────────────────────────┘
```

この Backup データベースは、通常のデータベースと同じように扱うこともできます。たとえば、含まれているテーブルに対してクエリを実行できます:

```sql
SELECT database, name FROM system.tables WHERE database = 'test_database_backup';
```

```text
┌─database─────────────┬─name─────────┐
│ test_database_backup │ test_table_1 │
│ test_database_backup │ test_table_2 │
│ test_database_backup │ test_table_3 │
└──────────────────────┴──────────────┘
```