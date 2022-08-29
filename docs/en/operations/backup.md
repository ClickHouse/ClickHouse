---
slug: /en/operations/backup
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

## Use

Backups can be either full or incremental, and can include tables and databases.  Bakups can be synchronous (default) or asynchronous.  They can be compressed.  Bakups can be password protected.

The BACKUP statement takes a list of DATABASE and TABLE names, a destination, and options.  Options are:
- ASYNC
- SETTINGS compression_method='lzma', compression_level=3, password='qwerty' id='first'
base_backup = Disk('backups', '1.zip') 

## Usage

```
BACKUP TABLE test.table TO {backup_name}
```

Corresponding restore:
```
RESTORE TABLE test.table FROM {backup_name}
```

:::note
The above RESTORE would fail if the table `test.table` contains data, you would have to drop the table in order to test the RESTORE, or use the setting `allow_non_empty_tables=true`:
```
RESTORE TABLE test.table FROM {backup_name} 
SETTINGS allow_non_empty_tables=true
```
:::

Tables can be restored, or backed up, with new names:
```
RESTORE TABLE test.table AS test.table2 FROM {backup_name}
```

```
BACKUP TABLE test.table3 AS test.table4 TO {backup_name}
```

## Incremental backups

Incremental backups can be taken by specifying the `base_backup`.

Store the initial backup:
```
BACKUP TABLE tbl TO Disk('backups', 'a.zip');
```

```
BACKUP TABLE tbl TO Disk('backups', 'c.zip')
  SETTINGS base_backup=Disk('backups', 'a.zip');
```

```
BACKUP TABLE test.table TO Disk('backups', 'd.zip');
```

Incrementally store new data. The setting `base_backup` causes data since the backup to `Disk('backups', 'd.zip')` to be stored to `Disk('backups', 'incremental-a.zip')`:
```
BACKUP TABLE test.table TO Disk('backups', 'incremental-a.zip')
  SETTINGS base_backup = Disk('backups', 'd.zip')
```

Restore all data from the incremental backup and the base_backup into a new table `test.table2`:
```
RESTORE TABLE test.table AS test.table2 
  FROM Disk('backups', 'incremental-a.zip');
```

## Assign a password to the backup
Backups written to disk can have a password applied to the file:
```
BACKUP TABLE test.table
  TO Disk('backups', 'password-protected.zip')
  SETTINGS password='qwerty'
```

Restore:
```
RESTORE TABLE test.table
  FROM Disk('backups', 'password-protected.zip')
  SETTINGS password='qwerty'
```

## Compression settings

If you would like to specify the compression method or level:
```
BACKUP TABLE test.table
  TO Disk('backups', 'lzma-level3.zip')
  SETTINGS compression_method='lzma', compression_level=3
```

## Restore specific partitions
If specific partitions associated with a table need to be restored these can be specified.  To restore partitions 1 and 4 from backup:
```
RESTORE TABLE test.table PARTITIONS '2', '3'
  FROM Disk('backups', 'lzma-level3.zip')
```

## Check the status of backups

```
SELECT status, error FROM system.backups FORMAT Vertical;
```

