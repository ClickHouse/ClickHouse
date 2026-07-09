---
description: '允许从[备份]中立即以只读
  模式附加表/数据库。'
sidebar_label: 'Backup'
sidebar_position: 60
slug: /engines/database-engines/backup
title: 'Backup'
doc_type: 'reference'
---

Backup 数据库引擎支持从[备份](/zh/operations/backup/overview)中立即以只读模式附加表/数据库。

Backup 数据库引擎同时适用于增量备份和非增量备份。

<div id="creating-a-database">
  ## 创建数据库
</div>

```sql
CREATE DATABASE backup_database
ENGINE = Backup('database_name_inside_backup', Disk('disk_name', 'backup_name'))
```

备份目标可以是任何有效的备份[目标](/zh/operations/backup/disk#configure-backup-destinations-for-disk)，例如 `Disk`、`S3` 或 `File`。它以函数形式传入，例如 `Disk('disk_name', 'backup_name')`。

**引擎参数**

* `database_name_inside_backup` — 备份中的数据库名称。
* `backup_destination` — 备份目标。

<div id="usage-example">
  ## 使用示例
</div>

下面以 `Disk` 备份目标位置为例。首先，在 `storage.xml` 中配置 `backups` 磁盘：

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

使用示例。先创建测试数据库和表，插入一些数据，然后再创建备份：

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

现在我们已经有了 `test_database_backup` 备份，接下来创建 Backup 数据库：

```sql
CREATE DATABASE test_database_backup ENGINE = Backup('test_database', Disk('backups', 'test_database_backup'));
```

现在，我们可以查询数据库中的任意表：

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

也可以像操作普通数据库一样操作这个 Backup 数据库。例如，查询其中的表：

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