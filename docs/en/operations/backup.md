---
description: 'Guide to backing up and restoring ClickHouse databases and tables'
sidebar_label: 'Backup and Restore'
sidebar_position: 10
slug: /operations/backup
title: 'Backup and Restore'
---

# Backup and Restore

- [Backup to a local disk](#backup-to-a-local-disk)
- [Configuring backup/restore to use an S3 endpoint](#configuring-backuprestore-to-use-an-s3-endpoint)
- [Backup/restore using an S3 disk](#backuprestore-using-an-s3-disk)
- [Alternatives](#alternatives)

## Command summary {#command-summary}

```bash
 BACKUP|RESTORE
  TABLE [db.]table_name [AS [db.]table_name_in_backup]
    [PARTITION[S] partition_expr [,...]] |
  DICTIONARY [db.]dictionary_name [AS [db.]name_in_backup] |
  DATABASE database_name [AS database_name_in_backup]
    [EXCEPT TABLES ...] |
  TEMPORARY TABLE table_name [AS table_name_in_backup] |
  VIEW view_name [AS view_name_in_backup] |
  ALL [EXCEPT {TABLES|DATABASES}...] } [,...]
  [ON CLUSTER 'cluster_name']
  TO|FROM File('<path>/<filename>') | Disk('<disk_name>', '<path>/') | S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>')
  [SETTINGS base_backup = File('<path>/<filename>') | Disk(...) | S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>')]

```

:::note ALL
Prior to version 23.4 of ClickHouse, `ALL` was only applicable to the `RESTORE` command.
:::

## Background {#background}

While [replication](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [you can't just drop tables with a MergeTree-like engine containing more than 50 Gb of data](/operations/settings/settings#max_table_size_to_drop). However, these safeguards do not cover all possible cases and can be circumvented.

In order to effectively mitigate possible human errors, you should carefully prepare a strategy for backing up and restoring your data **in advance**.

Each company has different resources available and business requirements, so there's no universal solution for ClickHouse backups and restores that will fit every situation. What works for one gigabyte of data likely won't work for tens of petabytes. There are a variety of possible approaches with their own pros and cons, which will be discussed below. It is a good idea to use several approaches instead of just one in order to compensate for their various shortcomings.

:::note
Keep in mind that if you backed something up and never tried to restore it, chances are that restore will not work properly when you actually need it (or at least it will take longer than business can tolerate). So whatever backup approach you choose, make sure to automate the restore process as well, and practice it on a spare ClickHouse cluster regularly.
:::

## Backup to a local disk {#backup-to-a-local-disk}

### Configure a backup destination {#configure-a-backup-destination}

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

### Parameters {#parameters}

Backups can be either full or incremental, and can include tables (including materialized views, projections, and dictionaries), and databases.  Backups can be synchronous (default) or asynchronous.  They can be compressed.  Backups can be password protected.

The BACKUP and RESTORE statements take a list of DATABASE and TABLE names, a destination (or source), options and settings:
- The destination for the backup, or the source for the restore.  This is based on the disk defined earlier.  For example `Disk('backups', 'filename.zip')`
- ASYNC: backup or restore asynchronously
- PARTITIONS: a list of partitions to restore
- SETTINGS:
    - `id`: id of backup or restore operation, randomly generated UUID is used, if not specified manually. If there is already running operation with the same `id` exception is thrown.
    - [`compression_method`](/sql-reference/statements/create/table#column_compression_codec) and compression_level
    - `password` for the file on disk
    - `base_backup`: the destination of the previous backup of this source.  For example, `Disk('backups', '1.zip')`
    - `use_same_s3_credentials_for_base_backup`: whether base backup to S3 should inherit credentials from the query. Only works with `S3`.
    - `use_same_password_for_base_backup`: whether base backup archive should inherit the password from the query.
    - `structure_only`: if enabled, allows to only backup or restore the CREATE statements without the data of tables
    - `storage_policy`: storage policy for the tables being restored. See [Using Multiple Block Devices for Data Storage](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes). This setting is only applicable to the `RESTORE` command. The specified storage policy applies only to tables with an engine from the `MergeTree` family.
    - `s3_storage_class`: the storage class used for S3 backup. For example, `STANDARD`
    - `azure_attempt_to_create_container`: when using Azure Blob Storage, whether the specified container will try to be created if it doesn't exist. Default: true.
    - [core settings](/operations/settings/settings) can be used here too

### Usage examples {#usage-examples}

Backup and then restore a table:
```sql
BACKUP TABLE test.table TO Disk('backups', '1.zip')
```

Corresponding restore:
```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip')
```

:::note
The above RESTORE would fail if the table `test.table` contains data, you would have to drop the table in order to test the RESTORE, or use the setting `allow_non_empty_tables=true`:
```sql
RESTORE TABLE test.table FROM Disk('backups', '1.zip')
SETTINGS allow_non_empty_tables=true
```
:::

Tables can be restored, or backed up, with new names:
```sql
RESTORE TABLE test.table AS test.table2 FROM Disk('backups', '1.zip')
```

```sql
BACKUP TABLE test.table3 AS test.table4 TO Disk('backups', '2.zip')
```

### Incremental backups {#incremental-backups}

Incremental backups can be taken by specifying the `base_backup`.
:::note
Incremental backups depend on the base backup.  The base backup must be kept available in order to be able to restore from an incremental backup.
:::

Incrementally store new data. The setting `base_backup` causes data since a previous backup to `Disk('backups', 'd.zip')` to be stored to `Disk('backups', 'incremental-a.zip')`:
```sql
BACKUP TABLE test.table TO Disk('backups', 'incremental-a.zip')
  SETTINGS base_backup = Disk('backups', 'd.zip')
```

Restore all data from the incremental backup and the base_backup into a new table `test.table2`:
```sql
RESTORE TABLE test.table AS test.table2
  FROM Disk('backups', 'incremental-a.zip');
```

### Assign a password to the backup {#assign-a-password-to-the-backup}

Backups written to disk can have a password applied to the file:
```sql
BACKUP TABLE test.table
  TO Disk('backups', 'password-protected.zip')
  SETTINGS password='qwerty'
```

Restore:
```sql
RESTORE TABLE test.table
  FROM Disk('backups', 'password-protected.zip')
  SETTINGS password='qwerty'
```

### Compression settings {#compression-settings}

If you would like to specify the compression method or level:
```sql
BACKUP TABLE test.table
  TO Disk('backups', 'filename.zip')
  SETTINGS compression_method='lzma', compression_level=3
```

### Restore specific partitions {#restore-specific-partitions}
If specific partitions associated with a table need to be restored these can be specified.  To restore partitions 1 and 4 from backup:
```sql
RESTORE TABLE test.table PARTITIONS '2', '3'
  FROM Disk('backups', 'filename.zip')
```

### Backups as tar archives {#backups-as-tar-archives}

Backups can also be stored as tar archives. The functionality is the same as for zip, except that a password is not supported.

Write a backup as a tar:
```sql
BACKUP TABLE test.table TO Disk('backups', '1.tar')
```

Corresponding restore:
```sql
RESTORE TABLE test.table FROM Disk('backups', '1.tar')
```

To change the compression method, the correct file suffix should be appended to the backup name. I.E to compress the tar archive using gzip:
```sql
BACKUP TABLE test.table TO Disk('backups', '1.tar.gz')
```

The supported compression file suffixes are `tar.gz`, `.tgz` `tar.bz2`, `tar.lzma`, `.tar.zst`, `.tzst` and `.tar.xz`.


### Check the status of backups {#check-the-status-of-backups}

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

```sql
SELECT
    *
FROM system.backups
WHERE id='7678b0b3-f519-4e6e-811f-5a0781a4eb52'
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

Along with `system.backups` table, all backup and restore operations are also tracked in the system log table [backup_log](../operations/system-tables/backup_log.md):
```sql
SELECT *
FROM system.backup_log
WHERE id = '7678b0b3-f519-4e6e-811f-5a0781a4eb52'
ORDER BY event_time_microseconds ASC
FORMAT Vertical
```
```response
Row 1:
──────
event_date:              2023-08-18
event_time_microseconds: 2023-08-18 11:13:43.097414
id:                      7678b0b3-f519-4e6e-811f-5a0781a4eb52
name:                    Disk('backups', '1.zip')
status:                  CREATING_BACKUP
error:
start_time:              2023-08-18 11:13:43
end_time:                1970-01-01 03:00:00
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

Row 2:
──────
event_date:              2023-08-18
event_time_microseconds: 2023-08-18 11:13:43.174782
id:                      7678b0b3-f519-4e6e-811f-5a0781a4eb52
name:                    Disk('backups', '1.zip')
status:                  BACKUP_FAILED
#highlight-next-line
error:                   Code: 598. DB::Exception: Backup Disk('backups', '1.zip') already exists. (BACKUP_ALREADY_EXISTS) (version 23.8.1.1)
start_time:              2023-08-18 11:13:43
end_time:                2023-08-18 11:13:43
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

2 rows in set. Elapsed: 0.075 sec.
```

## Configuring BACKUP/RESTORE to use an S3 Endpoint {#configuring-backuprestore-to-use-an-s3-endpoint}

To write backups to an S3 bucket you need three pieces of information:
- S3 endpoint,
  for example `https://mars-doc-test.s3.amazonaws.com/backup-S3/`
- Access key ID,
  for example `ABC123`
- Secret access key,
  for example `Abc+123`

:::note
Creating an S3 bucket is covered in [Use S3 Object Storage as a ClickHouse disk](/integrations/data-ingestion/s3/index.md#configuring-s3-for-clickhouse-use), just come back to this doc after saving the policy, there is no need to configure ClickHouse to use the S3 bucket.
:::

The destination for a backup will be specified like this:

```sql
S3('<S3 endpoint>/<directory>', '<Access key ID>', '<Secret access key>')
```

```sql
CREATE TABLE data
(
    `key` Int,
    `value` String,
    `array` Array(String)
)
ENGINE = MergeTree
ORDER BY tuple()
```

```sql
INSERT INTO data SELECT *
FROM generateRandom('key Int, value String, array Array(String)')
LIMIT 1000
```

### Create a base (initial) backup {#create-a-base-initial-backup}

Incremental backups require a _base_ backup to start from, this example will be used
later as the base backup.  The first parameter of the S3 destination is the S3 endpoint followed by the directory within the bucket to use for this backup.  In this example the directory is named `my_backup`.

```sql
BACKUP TABLE data TO S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_backup', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ de442b75-a66c-4a3c-a193-f76f278c70f3 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```

### Add more data {#add-more-data}

Incremental backups are populated with the difference between the base backup and the current content of the table being backed up.  Add more data before taking the incremental backup:

```sql
INSERT INTO data SELECT *
FROM generateRandom('key Int, value String, array Array(String)')
LIMIT 100
```
### Take an incremental backup {#take-an-incremental-backup}

This backup command is similar to the base backup, but adds `SETTINGS base_backup` and the location of the base backup.  Note that the destination for the incremental backup is not the same directory as the base, it is the same endpoint with a different target directory within the bucket.  The base backup is in `my_backup`, and the incremental will be written to `my_incremental`:
```sql
BACKUP TABLE data TO S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_incremental', 'ABC123', 'Abc+123') SETTINGS base_backup = S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_backup', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ f6cd3900-850f-41c9-94f1-0c4df33ea528 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```
### Restore from the incremental backup {#restore-from-the-incremental-backup}

This command restores the incremental backup into a new table, `data3`.  Note that when an incremental backup is restored, the base backup is also included.  Specify only the incremental backup when restoring:
```sql
RESTORE TABLE data AS data3 FROM S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_incremental', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status───┐
│ ff0c8c39-7dff-4324-a241-000796de11ca │ RESTORED │
└──────────────────────────────────────┴──────────┘
```

### Verify the count {#verify-the-count}

There were two inserts into the original table `data`, one with 1,000 rows and one with 100 rows, for a total of 1,100. Verify that the restored table has 1,100 rows:
```sql
SELECT count()
FROM data3
```
```response
┌─count()─┐
│    1100 │
└─────────┘
```

### Verify the content {#verify-the-content}
This compares the content of the original table, `data` with the restored table `data3`:
```sql
SELECT throwIf((
        SELECT groupArray(tuple(*))
        FROM data
    ) != (
        SELECT groupArray(tuple(*))
        FROM data3
    ), 'Data does not match after BACKUP/RESTORE')
```
## BACKUP/RESTORE Using an S3 Disk {#backuprestore-using-an-s3-disk}

It is also possible to `BACKUP`/`RESTORE` to S3 by configuring an S3 disk in the ClickHouse storage configuration.  Configure the disk like this by adding a file to `/etc/clickhouse-server/config.d`:

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
                        <disk>s3_plain</disk>
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
- If your tables are backed by S3 storage and types of the disks are different, it doesn't use `CopyObject` calls to copy parts to the destination bucket, instead, it downloads and uploads them, which is very inefficient. Prefer to use `BACKUP ... TO S3(<endpoint>)` syntax for this use-case.
:::

## Using Named Collections {#using-named-collections}

Named collections can be used for `BACKUP/RESTORE` parameters. See [here](./named-collections.md#named-collections-for-backups) for an example.

## Alternatives {#alternatives}

ClickHouse stores data on disk, and there are many ways to backup disks.  These are some alternatives that have been used in the past, and that may fit in well in your environment.

### Duplicating Source Data Somewhere Else {#duplicating-source-data-somewhere-else}

Often data that is ingested into ClickHouse is delivered through some sort of persistent queue, such as [Apache Kafka](https://kafka.apache.org). In this case it is possible to configure an additional set of subscribers that will read the same data stream while it is being written to ClickHouse and store it in cold storage somewhere. Most companies already have some default recommended cold storage, which could be an object store or a distributed filesystem like [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

### Filesystem Snapshots {#filesystem-snapshots}

Some local filesystems provide snapshot functionality (for example, [ZFS](https://en.wikipedia.org/wiki/ZFS)), but they might not be the best choice for serving live queries. A possible solution is to create additional replicas with this kind of filesystem and exclude them from the [Distributed](../engines/table-engines/special/distributed.md) tables that are used for `SELECT` queries. Snapshots on such replicas will be out of reach of any queries that modify data. As a bonus, these replicas might have special hardware configurations with more disks attached per server, which would be cost-effective.

For smaller volumes of data, a simple `INSERT INTO ... SELECT ...` to remote tables might work as well.

### Manipulations with Parts {#manipulations-with-parts}

ClickHouse allows using the `ALTER TABLE ... FREEZE PARTITION ...` query to create a local copy of table partitions. This is implemented using hardlinks to the `/var/lib/clickhouse/shadow/` folder, so it usually does not consume extra disk space for old data. The created copies of files are not handled by ClickHouse server, so you can just leave them there: you will have a simple backup that does not require any additional external system, but it will still be prone to hardware issues. For this reason, it's better to remotely copy them to another location and then remove the local copies. Distributed filesystems and object stores are still a good options for this, but normal attached file servers with a large enough capacity might work as well (in this case the transfer will occur via the network filesystem or maybe [rsync](https://en.wikipedia.org/wiki/Rsync)).
Data can be restored from backup using the `ALTER TABLE ... ATTACH PARTITION ...`

For more information about queries related to partition manipulations, see the [ALTER documentation](/sql-reference/statements/alter/partition).

A third-party tool is available to automate this approach: [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup).

## Settings to disallow concurrent backup/restore {#settings-to-disallow-concurrent-backuprestore}

To disallow concurrent backup/restore, you can use these settings respectively.

```xml
<clickhouse>
    <backups>
        <allow_concurrent_backups>false</allow_concurrent_backups>
        <allow_concurrent_restores>false</allow_concurrent_restores>
    </backups>
</clickhouse>
```

The default value for both is true, so by default concurrent backup/restores are allowed.
When these settings are false on a cluster, only 1 backup/restore is allowed to run on a cluster at a time.

## Configuring BACKUP/RESTORE to use an AzureBlobStorage Endpoint {#configuring-backuprestore-to-use-an-azureblobstorage-endpoint}

To write backups to an AzureBlobStorage container you need the following pieces of information:
- AzureBlobStorage endpoint connection string / url,
- Container,
- Path,
- Account name (if url is specified)
- Account Key (if url is specified)

The destination for a backup will be specified like this:

```sql
AzureBlobStorage('<connection string>/<url>', '<container>', '<path>', '<account name>', '<account key>')
```

```sql
BACKUP TABLE data TO AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'testcontainer', 'data_backup');
RESTORE TABLE data AS data_restored FROM AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'testcontainer', 'data_backup');
```

## Backup up system tables {#backup-up-system-tables}

System tables can also be included in your backup and restore workflows, but their inclusion depends on your specific use case.

### Backing Up Log Tables {#backing-up-log-tables}

System tables that store historic data, such as those with a _log suffix (e.g., `query_log`, `part_log`), can be backed up and restored like any other table. If your use case relies on analyzing historic data—for example, using query_log to track query performance or debug issues—it's recommended to include these tables in your backup strategy. However, if historic data from these tables is not required, they can be excluded to save backup storage space.

### Backing Up Access Management Tables {#backing-up-access-management-tables}

System tables related to access management, such as users, roles, row_policies, settings_profiles, and quotas, receive special treatment during backup and restore operations. When these tables are included in a backup, their content is exported to a special `accessXX.txt` file, which encapsulates the equivalent SQL statements for creating and configuring the access entities. Upon restoration, the restore process interprets these files and re-applies the SQL commands to recreate the users, roles, and other configurations.

This feature ensures that the access control configuration of a ClickHouse cluster can be backed up and restored as part of the cluster's overall setup.

Note: This functionality only works for configurations managed through SQL commands (referred to as ["SQL-driven Access Control and Account Management"](/operations/access-rights#enabling-access-control)). Access configurations defined in ClickHouse server configuration files (e.g. `users.xml`) are not included in backups and cannot be restored through this method.
