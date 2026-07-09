---
description: 'clickhouse_backupview 문서 {#clickhouse_backupview}'
slug: /operations/utilities/backupview
title: 'clickhouse_backupview'
doc_type: '참고'
---

[BACKUP](/ko/operations/backup/overview) 명령으로 생성된 백업을 분석하는 데 도움이 되는 Python 모듈입니다.
주된 목적은 실제로 복원하지 않고도 백업에서 일부 정보를 확인할 수 있도록 하는 것입니다.

이 모듈은 다음 기능을 제공합니다.

* 백업에 포함된 파일 열거
* 백업에서 파일 읽기
* 백업에 포함된 데이터베이스, 테이블, 파트에 대한 유용한 정보를 읽기 쉬운 형태로 가져오기
* 백업의 무결성 확인

<div id="example">
  ## 예시:
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

더 많은 예시는 [test](https://github.com/ClickHouse/ClickHouse/blob/master/utils/backupview/test/test.py)를 확인하십시오.