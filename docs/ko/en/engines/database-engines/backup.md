---
description: '읽기 전용 모드에서 백업에서 테이블/데이터베이스를 즉시 ATTACH할 수 있습니다.'
sidebar_label: 'Backup'
sidebar_position: 60
slug: /engines/database-engines/backup
title: 'Backup'
doc_type: '참고'
---

Backup 데이터베이스 엔진을 사용하면 [백업](/ko/operations/backup/overview)에서 테이블/데이터베이스를 읽기 전용 모드로 즉시 ATTACH할 수 있습니다.

Backup 데이터베이스 엔진은 증분 백업과 증분이 아닌 백업 모두에서 작동합니다.

<div id="creating-a-database">
  ## 데이터베이스 생성
</div>

```sql
CREATE DATABASE backup_database
ENGINE = Backup('database_name_inside_backup', Disk('disk_name', 'backup_name'))
```

백업 대상은 `Disk`, `S3`, `File`과 같은 유효한 백업 [대상](/ko/operations/backup/disk#configure-backup-destinations-for-disk)이 될 수 있습니다. 함수 형식으로 전달하며, 예를 들어 `Disk('disk_name', 'backup_name')`와 같이 지정합니다.

**엔진 매개변수**

* `database_name_inside_backup` — 백업 내부 데이터베이스의 이름입니다.
* `backup_destination` — 백업 대상입니다.

<div id="usage-example">
  ## 사용 예시
</div>

`Disk` 백업 대상의 예시를 살펴보겠습니다. 먼저 `storage.xml`에서 백업 디스크를 설정합니다:

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

사용 예시입니다. 테스트용 데이터베이스와 테이블을 만들고, 데이터를 몇 개 삽입한 다음 백업을 생성해 보겠습니다:

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

이제 `test_database_backup` 백업이 있으므로 Backup 데이터베이스를 생성하겠습니다:

```sql
CREATE DATABASE test_database_backup ENGINE = Backup('test_database', Disk('backups', 'test_database_backup'));
```

이제 데이터베이스의 어떤 테이블이든 쿼리할 수 있습니다:

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

이 Backup 데이터베이스도 일반 데이터베이스처럼 사용할 수 있습니다. 예를 들어, 그 안의 테이블에 쿼리할 수 있습니다:

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