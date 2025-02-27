---
slug: /en/operations/utilities/backupview
title: clickhouse_backupview
---

# clickhouse_backupview {#clickhouse_backupview}

Python module to help analyzing backups made by the [BACKUP](https://clickhouse.com/docs/en/operations/backup) command.
The main motivation was to allows getting some information from a backup without actually restoring it.

This module provides functions to
- enumerate files contained in a backup
- read files from a backup
- get useful information in readable form about databases, tables, parts contained in a backup
- check integrity of a backup

## Example:

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

For more examples see the [test](https://github.com/ClickHouse/ClickHouse/blob/master/utils/backupview/test/test.py).
