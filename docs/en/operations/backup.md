---
sidebar_position: 49
sidebar_label: Data backup and restore
---

# Data backup and restore

While [replication](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [you can’t just drop tables with a MergeTree-like engine containing more than 50 Gb of data](server-configuration-parameters/settings.md#max-table-size-to-drop). However, these safeguards do not cover all possible cases and can be circumvented.

In order to effectively mitigate possible human errors, you should carefully prepare a strategy for backing up and restoring your data **in advance**.

Each company has different resources available and business requirements, so there’s no universal solution for ClickHouse backups and restores that will fit every situation. What works for one gigabyte of data likely won’t work for tens of petabytes. There are a variety of possible approaches with their own pros and cons, which will be discussed below. It is a good idea to use several approaches instead of just one in order to compensate for their various shortcomings.

:::note
Keep in mind that if you backed something up and never tried to restore it, chances are that restore will not work properly when you actually need it (or at least it will take longer than business can tolerate). So whatever backup approach you choose, make sure to automate the restore process as well, and practice it on a spare ClickHouse cluster regularly.
:::

## Duplicating Source Data Somewhere Else {#duplicating-source-data-somewhere-else}

Often data that is ingested into ClickHouse is delivered through some sort of persistent queue, such as [Apache Kafka](https://kafka.apache.org). In this case it is possible to configure an additional set of subscribers that will read the same data stream while it is being written to ClickHouse and store it in cold storage somewhere. Most companies already have some default recommended cold storage, which could be an object store or a distributed filesystem like [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## Configure a backup destination

In the examples below you will see the backup destination specified like `Disk('backups', '1.zip')`.  To prepare the destination add a file to `/etc/clickhouse-server/config.d/backup_disk.xml`.  For example:

```xml
<clickhouse>                                                                                    
    <storage_configuration>                                                                     
        <disks>                                                                                 
            <backups>                                                                           
                <type>local</type>                                                              
                <path>/backups/</path>                                                          
            </backups>                                                                          
        </disks>                                                                                
    </storage_configuration>                                                                    
    <backups>                                                                                   
        <allowed_disk>backups</allowed_disk>                                                    
        <allowed_path>/backups/</allowed_path>                                                  
    </backups>                                                                                  
</clickhouse>
```

## BACKUP DATABASE

Backup the `default` database and several of the `system` tables asynchronously:
```sql
BACKUP DATABASE default, TABLE system.users, \
  TABLE system.roles, TABLE system.settings_profiles, \
  TABLE system.row_policies, TABLE system.quotas \
  TO Disk('backups', '1.zip') ASYNC
```

Backup the `test` database with a new name (`test2`):
```sql
BACKUP DATABASE test AS test2 TO Disk('backups', '1.zip')
```

Backup the database `test`:
```sql
BACKUP DATABASE test TO Disk('backups', '1.zip')

## BACKUP TABLE

Backup the table `mv_1`:
```sql
BACKUP TABLE mv_1 TO Disk('backups', '1.zip')
```

Backup several of the `system` tables:
```sql
BACKUP TABLE system.users, TABLE system.roles, \
  TABLE system.settings_profiles, TABLE system.row_policies, \
  TABLE system.quotas TO Disk('backups', '1.zip')
```

Backup with a specific username:
```sql
BACKUP TABLE system.users, TABLE system.roles \
  TO Disk('backups', '1.zip'), user="u2"
```

Backup a table with a new name:
```sql
BACKUP TABLE test.table AS test.table2 TO Disk('backups', '1.zip')
```

Backup a table asynchronously:
```sql
BACKUP TABLE test.table TO Disk('backups', '1.zip') ASYNC
```

### Compress and password protect a backup

Use `lzma` compression, and password protect the backup:
```sql
BACKUP TABLE test.table TO Disk('backups', '1.zip') \
  SETTINGS compression_method='lzma', compression_level=3, password='qwerty'
```

Add an `id` to the backup (the id is used during `RESTORE`):
```sql
BACKUP TABLE test.table TO Disk('backups', '1.zip') \
  SETTINGS id='first' ASYNC
```

### Incremental table backup

Incrementally backup a table:
```sql
BACKUP TABLE test.table2 TO {incremental_backup_name} \
  SETTINGS base_backup = Disk('backups', '1.zip')
```

## Restore

Restore specific partitions:
```sql
RESTORE TABLE test.table PARTITIONS '2', '3' FROM Disk('backups', '1.zip')
```

Restore all databases and tables from a specific backup:
```sql
RESTORE ALL FROM Disk('backups', '1.zip')
```

Restore based on a username:
```sql
RESTORE ALL FROM Disk('backups', '1.zip'), user="u1"

Restore specific databases and tables asynchronously:
```sql
RESTORE DATABASE default, TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas FROM Disk('backups', '1.zip') ASYNC
```

Restore and rename a table:
```sql
RESTORE DATABASE test2 AS test3 FROM Disk('backups', '1.zip')
```

Restore specific tables:
```sql
RESTORE TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas FROM Disk('backups', '1.zip')
```

```sql
RESTORE TABLE test.table2 FROM {incremental_backup_name}
```

Restore and rename while specifying a username:
```sql
RESTORE TABLE test.table AS test.table2 FROM Disk('backups', '1.zip'), user="u1"
```

Restore and rename from an incremental backup:
```sql
RESTORE TABLE test.table AS test.table2 FROM {incremental_backup_name}"
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip')
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip') ASYNC
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip') SETTINGS allow_non_empty_tables=true
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip') SETTINGS id='first'
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip') SETTINGS id='second' ASYNC
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip') SETTINGS password='qwerty'
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip') SETTINGS structure_only=true
```

```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip'), user="u1"
```

```sql
RESTORE TEMPORARY TABLE temp_tbl FROM Disk('backups', '1.zip'),
```

## Get backup status

```sql
SELECT name, status, num_files, uncompressed_size, compressed_size, error FROM system.backups WHERE id='{id}'
```

```sql
SELECT status FROM system.backups WHERE id='{id2}'
```

```sql
SELECT status FROM system.backups WHERE id IN {ids_for_query} AND status == 'CREATING_BACKUP'
```

```sql
SELECT status, num_files, uncompressed_size, compressed_size, error FROM system.backups WHERE name='{escaped_backup_name}'
```
