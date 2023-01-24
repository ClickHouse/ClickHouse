---
slug: /en/operations/backup
sidebar_position: 49
sidebar_label: Data backup and restore
title: Data backup and restore
---

While [replication](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [you can’t just drop tables with a MergeTree-like engine containing more than 50 Gb of data](server-configuration-parameters/settings.md#max-table-size-to-drop). However, these safeguards do not cover all possible cases and can be circumvented.

In order to effectively mitigate possible human errors, you should carefully prepare a strategy for backing up and restoring your data **in advance**.

Each company has different resources available and business requirements, so there’s no universal solution for ClickHouse backups and restores that will fit every situation. What works for one gigabyte of data likely won’t work for tens of petabytes. There are a variety of possible approaches with their own pros and cons, which will be discussed below. It is a good idea to use several approaches instead of just one in order to compensate for their various shortcomings.

:::note
Keep in mind that if you backed something up and never tried to restore it, chances are that restore will not work properly when you actually need it (or at least it will take longer than business can tolerate). So whatever backup approach you choose, make sure to automate the restore process as well, and practice it on a spare ClickHouse cluster regularly.
:::

## Configure a backup destination

In the examples below you will see the backup destination specified like `Disk('backups', '1.zip')`.  To prepare the destination add a file to `/etc/clickhouse-server/config.d/backup_disk.xml` specifying the backup destination.  For example, this file defines disk named `backups` and then adds that disk to the **backups > allowed_disk** list:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
<!--highlight-next-line -->
            <backups>
                <type>local</type>
                <path>/backups/</path>
            </backups>
        </disks>
    </storage_configuration>
<!--highlight-start -->
    <backups>
        <allowed_disk>backups</allowed_disk>
        <allowed_path>/backups/</allowed_path>
    </backups>
<!--highlight-end -->
</clickhouse>
```

## Parameters

Backups can be either full or incremental, and can include tables (including materialized views, projections, and dictionaries), and databases.  Backups can be synchronous (default) or asynchronous.  They can be compressed.  Backups can be password protected.

The BACKUP and RESTORE statements take a list of DATABASE and TABLE names, a destination (or source), options and settings:
- The destination for the backup, or the source for the restore.  This is based on the disk defined earlier.  For example `Disk('backups', 'filename.zip')`
- ASYNC: backup or restore asynchronously
- PARTITIONS: a list of partitions to restore
- SETTINGS:
    - [`compression_method`](en/sql-reference/statements/create/table/#column-compression-codecs) and compression_level
    - `password` for the file on disk
    - `base_backup`: the destination of the previous backup of this source.  For example, `Disk('backups', '1.zip')` 

## Usage examples

Backup and then restore a table:
```
BACKUP TABLE test.table TO Disk('backups', '1.zip')
```

Corresponding restore:
```
RESTORE TABLE test.table FROM Disk('backups', '1.zip')
```

:::note
The above RESTORE would fail if the table `test.table` contains data, you would have to drop the table in order to test the RESTORE, or use the setting `allow_non_empty_tables=true`:
```
RESTORE TABLE test.table FROM Disk('backups', '1.zip') 
SETTINGS allow_non_empty_tables=true
```
:::

Tables can be restored, or backed up, with new names:
```
RESTORE TABLE test.table AS test.table2 FROM Disk('backups', '1.zip')
```

```
BACKUP TABLE test.table3 AS test.table4 TO Disk('backups', '2.zip')
```

## Incremental backups

Incremental backups can be taken by specifying the `base_backup`.
:::note
Incremental backups depend on the base backup.  The base backup must be kept available in order to be able to restore from an incremental backup.
:::

Incrementally store new data. The setting `base_backup` causes data since a previous backup to `Disk('backups', 'd.zip')` to be stored to `Disk('backups', 'incremental-a.zip')`:
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
  TO Disk('backups', 'filename.zip')
  SETTINGS compression_method='lzma', compression_level=3
```

## Restore specific partitions
If specific partitions associated with a table need to be restored these can be specified.  To restore partitions 1 and 4 from backup:
```
RESTORE TABLE test.table PARTITIONS '2', '3'
  FROM Disk('backups', 'filename.zip')
```

## Check the status of backups

The backup command returns an `id` and `status`, and that `id` can be used to get the status of the backup.  This is very useful to check the progress of long ASYNC backups.  The example below shows a failure that happened when trying to overwrite an existing backup file:
```sql
BACKUP TABLE helloworld.my_first_table TO Disk('backups', '1.zip') ASYNC
```
```response
┌─id───────────────────────────────────┬─status──────────┐
│ 7678b0b3-f519-4e6e-811f-5a0781a4eb52 │ CREATING_BACKUP │
└──────────────────────────────────────┴─────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

```
SELECT
    *
FROM system.backups
where id='7678b0b3-f519-4e6e-811f-5a0781a4eb52'
FORMAT Vertical
```
```response
Row 1:
──────
id:                7678b0b3-f519-4e6e-811f-5a0781a4eb52
name:              Disk('backups', '1.zip')
#highlight-next-line
status:            BACKUP_FAILED
num_files:         0
uncompressed_size: 0
compressed_size:   0
#highlight-next-line
error:             Code: 598. DB::Exception: Backup Disk('backups', '1.zip') already exists. (BACKUP_ALREADY_EXISTS) (version 22.8.2.11 (official build))
start_time:        2022-08-30 09:21:46
end_time:          2022-08-30 09:21:46

1 row in set. Elapsed: 0.002 sec.
```

## Backup to S3

It is possible to `BACKUP`/`RESTORE` to S3, but this disk should be configured
in a proper way, since by default you will need to backup metadata from local
disk to make backup full.

First of all, you need to configure S3 disk in a special way:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_plain>
                <type>s3_plain</type>
                <endpoint></endpoint>
                <access_key_id></access_key_id>
                <secret_access_key></secret_access_key>
            </s3_plain>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>

    <backups>
        <allowed_disk>s3_plain</allowed_disk>
    </backups>
</clickhouse>
```

And then `BACKUP`/`RESTORE` as usual:

```sql
BACKUP TABLE data TO Disk('s3_plain', 'cloud_backup');
RESTORE TABLE data AS data_restored FROM Disk('s3_plain', 'cloud_backup');
```

:::note
But keep in mind that:
- This disk should not be used for `MergeTree` itself, only for `BACKUP`/`RESTORE`
- It has excessive API calls
:::

## Alternatives

ClickHouse stores data on disk, and there are many ways to backup disks.  These are some alternatives that have been used in the past, and that may fit in well in your environment.

### Duplicating Source Data Somewhere Else {#duplicating-source-data-somewhere-else}

Often data that is ingested into ClickHouse is delivered through some sort of persistent queue, such as [Apache Kafka](https://kafka.apache.org). In this case it is possible to configure an additional set of subscribers that will read the same data stream while it is being written to ClickHouse and store it in cold storage somewhere. Most companies already have some default recommended cold storage, which could be an object store or a distributed filesystem like [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

### Filesystem Snapshots {#filesystem-snapshots}

Some local filesystems provide snapshot functionality (for example, [ZFS](https://en.wikipedia.org/wiki/ZFS)), but they might not be the best choice for serving live queries. A possible solution is to create additional replicas with this kind of filesystem and exclude them from the [Distributed](../engines/table-engines/special/distributed.md) tables that are used for `SELECT` queries. Snapshots on such replicas will be out of reach of any queries that modify data. As a bonus, these replicas might have special hardware configurations with more disks attached per server, which would be cost-effective.

### clickhouse-copier {#clickhouse-copier}

[clickhouse-copier](../operations/utilities/clickhouse-copier.md) is a versatile tool that was initially created to re-shard petabyte-sized tables. It can also be used for backup and restore purposes because it reliably copies data between ClickHouse tables and clusters.

For smaller volumes of data, a simple `INSERT INTO ... SELECT ...` to remote tables might work as well.

### Manipulations with Parts {#manipulations-with-parts}

ClickHouse allows using the `ALTER TABLE ... FREEZE PARTITION ...` query to create a local copy of table partitions. This is implemented using hardlinks to the `/var/lib/clickhouse/shadow/` folder, so it usually does not consume extra disk space for old data. The created copies of files are not handled by ClickHouse server, so you can just leave them there: you will have a simple backup that does not require any additional external system, but it will still be prone to hardware issues. For this reason, it’s better to remotely copy them to another location and then remove the local copies. Distributed filesystems and object stores are still a good options for this, but normal attached file servers with a large enough capacity might work as well (in this case the transfer will occur via the network filesystem or maybe [rsync](https://en.wikipedia.org/wiki/Rsync)).
Data can be restored from backup using the `ALTER TABLE ... ATTACH PARTITION ...`

For more information about queries related to partition manipulations, see the [ALTER documentation](../sql-reference/statements/alter/partition.md#alter_manipulations-with-partitions).

A third-party tool is available to automate this approach: [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup).

