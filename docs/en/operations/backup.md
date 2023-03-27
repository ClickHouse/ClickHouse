---
slug: /en/operations/backup
description: In order to effectively mitigate possible human errors, you should carefully prepare a strategy for backing up and restoring your data.
---

# Backup and Restore

- [Backup to a local disk](#backup-to-a-local-disk)
- [Configuring backup/restore to use an S3 endpoint](#configuring-backuprestore-to-use-an-s3-endpoint)
- [Backup/restore using an S3 disk](#backuprestore-using-an-s3-disk)
- [Use Backup and Attach to create a read-only table](#use-backup-and-attach-to-create-a-read-only-table)
- [Alternatives](#alternatives)

## Command summary

```bash
 BACKUP|RESTORE
  TABLE [db.]table_name [AS [db.]table_name_in_backup]
    [PARTITION[S] partition_expr [,...]] |
  DICTIONARY [db.]dictionary_name [AS [db.]name_in_backup] |
  DATABASE database_name [AS database_name_in_backup]
    [EXCEPT TABLES ...] |
  TEMPORARY TABLE table_name [AS table_name_in_backup] |
  VIEW view_name [AS view_name_in_backup]
  ALL TEMPORARY TABLES [EXCEPT ...] |
  ALL DATABASES [EXCEPT ...] } [,...]
  [ON CLUSTER 'cluster_name']
  TO|FROM File('<path>/<filename>') | Disk('<disk_name>', '<path>/') | S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>')
  [SETTINGS base_backup = File('<path>/<filename>') | Disk(...) | S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>')]

```

:::note ALL
`ALL` is only applicable to the `RESTORE` command.
:::

## Background

While [replication](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [you can’t just drop tables with a MergeTree-like engine containing more than 50 Gb of data](server-configuration-parameters/settings.md#max-table-size-to-drop). However, these safeguards do not cover all possible cases and can be circumvented.

In order to effectively mitigate possible human errors, you should carefully prepare a strategy for backing up and restoring your data **in advance**.

Each company has different resources available and business requirements, so there’s no universal solution for ClickHouse backups and restores that will fit every situation. What works for one gigabyte of data likely won’t work for tens of petabytes. There are a variety of possible approaches with their own pros and cons, which will be discussed below. It is a good idea to use several approaches instead of just one in order to compensate for their various shortcomings.

:::note
Keep in mind that if you backed something up and never tried to restore it, chances are that restore will not work properly when you actually need it (or at least it will take longer than business can tolerate). So whatever backup approach you choose, make sure to automate the restore process as well, and practice it on a spare ClickHouse cluster regularly.
:::

## Backup to a local disk

### Configure a backup destination

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

### Parameters

Backups can be either full or incremental, and can include tables (including materialized views, projections, and dictionaries), and databases.  Backups can be synchronous (default) or asynchronous.  They can be compressed.  Backups can be password protected.

The BACKUP and RESTORE statements take a list of DATABASE and TABLE names, a destination (or source), options and settings:
- The destination for the backup, or the source for the restore.  This is based on the disk defined earlier.  For example `Disk('backups', 'filename.zip')`
- ASYNC: backup or restore asynchronously
- PARTITIONS: a list of partitions to restore
- SETTINGS:
    - [`compression_method`](/docs/en/sql-reference/statements/create/table.md/#column-compression-codecs) and compression_level
    - `password` for the file on disk
    - `base_backup`: the destination of the previous backup of this source.  For example, `Disk('backups', '1.zip')`

### Usage examples

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

### Incremental backups

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

### Assign a password to the backup

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

### Compression settings

If you would like to specify the compression method or level:
```
BACKUP TABLE test.table
  TO Disk('backups', 'filename.zip')
  SETTINGS compression_method='lzma', compression_level=3
```

### Restore specific partitions
If specific partitions associated with a table need to be restored these can be specified.  To restore partitions 1 and 4 from backup:
```
RESTORE TABLE test.table PARTITIONS '2', '3'
  FROM Disk('backups', 'filename.zip')
```

### Check the status of backups

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

## Configuring BACKUP/RESTORE to use an S3 Endpoint

To write backups to an S3 bucket you need three pieces of information:
- S3 endpoint,
  for example `https://mars-doc-test.s3.amazonaws.com/backup-S3/`
- Access key ID,
  for example `ABC123`
- Secret access key,
  for example `Abc+123`

:::note
Creating an S3 bucket is covered in [Use S3 Object Storage as a ClickHouse disk](/docs/en/integrations/data-ingestion/s3/index.md#configuring-s3-for-clickhouse-use), just come back to this doc after saving the policy, there is no need to configure ClickHouse to use the S3 bucket.
:::

The destination for a backup will be specified like this:
```
S3('<S3 endpoint>/<directory>', '<Access key ID>', '<Secret access key>)
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

### Create a base (initial) backup

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

### Add more data

Incremental backups are populated with the difference between the base backup and the current content of the table being backed up.  Add more data before taking the incremental backup:

```sql
INSERT INTO data SELECT *
FROM generateRandom('key Int, value String, array Array(String)')
LIMIT 100
```
### Take an incremental backup

This backup command is similar to the base backup, but adds `SETTINGS base_backup` and the location of the base backup.  Note that the destination for the incremental backup is not the same directory as the base, it is the same endpoint with a different target directory within the bucket.  The base backup is in `my_backup`, and the incremental will be written to `my_incremental`:
```sql
BACKUP TABLE data TO S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_incremental', 'ABC123', 'Abc+123') SETTINGS base_backup = S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_backup', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ f6cd3900-850f-41c9-94f1-0c4df33ea528 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```
### Restore from the incremental backup

This command restores the incremental backup into a new table, `data3`.  Note that when an incremental backup is restored, the base backup is also included.  Specify only the incremental backup when restoring:
```sql
RESTORE TABLE data AS data3 FROM S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_incremental', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status───┐
│ ff0c8c39-7dff-4324-a241-000796de11ca │ RESTORED │
└──────────────────────────────────────┴──────────┘
```

### Verify the count

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

### Verify the content
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
## BACKUP/RESTORE Using an S3 Disk

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
- It has excessive API calls
:::

## Use Backup and Attach to create a read-only table

Backups can be written to S3 and then attached as read-only tables with these caveats:
- The database must be of type `ordinary`
- The table must have `ENGINE` of type `MergeTree`
- There can be no Materialized Views associated with the table

### Add a storage configuration

Configure your ClickHouse server by adding a disk of type `s3_plain`.  Here is an example configuration.  Substitute your `endpoint`, `access_key_id`, and `secret_access_key` on the lines highlighted:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <backup_disk_s3_plain>
                <type>s3_plain</type>
                <!-- highlight-start -->
                <endpoint>https://s3-backup-for-read-only-table.s3.amazonaws.com/tables/</endpoint>
                <access_key_id>Your AWS Access Key</access_key_id>
                <secret_access_key>Your AWS Secret Key</secret_access_key>
                <!-- highlight-end -->
                <s3_max_single_part_upload_size>33554432</s3_max_single_part_upload_size>
            </backup_disk_s3_plain>
            <s3_backup_compact>
                <type>s3_plain</type>
                <!-- highlight-start -->
                <!-- NOTE: /backup_compact/ is a name of BACKUP -->
                <endpoint>https://s3-backup-for-read-only-table.s3.amazonaws.com/tables/backup_compact/</endpoint>
                <access_key_id>Your AWS Access Key</access_key_id>
                <secret_access_key>Your AWS Secret Key</secret_access_key>
                <!-- highlight-end -->
                <s3_max_single_part_upload_size>33554432</s3_max_single_part_upload_size>
            </s3_backup_compact>
            <s3_backup_wide>
                <type>s3_plain</type>
                <!-- highlight-start -->
                <!-- NOTE: /backup_wide/ is a name of BACKUP -->
                <endpoint>https://s3-backup-for-read-only-table.s3.amazonaws.com/tables/backup_wide/</endpoint>
                <access_key_id>Your AWS Access Key</access_key_id>
                <secret_access_key>Your AWS Secret Key</secret_access_key>
                <!-- highlight-end -->
                <s3_max_single_part_upload_size>33554432</s3_max_single_part_upload_size>
            </s3_backup_wide>
        </disks>
        <policies>
            <s3_backup_compact>
                <volumes>
                    <main>
                        <disk>s3_backup_compact</disk>
                    </main>
                </volumes>
            </s3_backup_compact>
            <s3_backup_wide>
                <volumes>
                    <main>
                        <disk>s3_backup_wide</disk>
                    </main>
                </volumes>
            </s3_backup_wide>
        </policies>
    </storage_configuration>
    <backups>
        <allowed_disk>backup_disk_s3_plain</allowed_disk>
    </backups>
</clickhouse>
```

### Create a database of type ordinary

Enable the use of ordinary databases:
```sql
set allow_deprecated_database_ordinary=1;
```

Create the database:
```sql
create database ordinary_db engine=Ordinary;
```

### Create a table

This will create a table in the `ordinary_db` database that has five partitions:
- there are 100 rows, with two columns `part` and `key`
- `key` is set to the value provided by the function `numbers(100)`
- `part` is set to the value `number % 5`
- the table is partitioned on the column `part`

```sql
CREATE TABLE ordinary_db.compact
ENGINE = MergeTree
PARTITION BY part
ORDER BY key AS
SELECT
    number % 5 AS part,
    number AS key
FROM numbers(100)
```

### Backup the table to S3
```sql
BACKUP TABLE ordinary_db.compact
TO Disk('backup_disk_s3_plain', 'backup_compact')
SETTINGS deduplicate_files = 0
```
```response
┌─id───────────────────────────────────┬─status─────────┐
│ 6b323487-87be-40f6-99ac-3d049221b98a │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘

1 row in set. Elapsed: 5.698 sec.
```

### Examine the way data is stored in S3

Navigate to the bucket and locate the folder for the backup.

![folder contents in S3](@site/docs/en/operations/images/backup-for-RO-attach-one-partition.png)

### Drop the table
```sql
DROP TABLE ordinary_db.compact
```

### Attach the backup
```sql
ATTACH TABLE ordinary_db.compact
(
    `part` UInt8,
    `key` UInt64
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000,
max_suspicious_broken_parts = 0,
#highlight-next-line
storage_policy = 's3_backup_compact'
```

### Verify the content

```sql
SELECT *
FROM ordinary_db.compact
```
```response
┌─part─┬─key─┐
│    1 │   1 │
│    1 │   6 │
│    1 │  11 │
│    1 │  16 │
│    1 │  21 │
│    1 │  26 │
│    1 │  31 │
│    1 │  36 │
│    1 │  41 │
│    1 │  46 │
│    1 │  51 │
│    1 │  56 │
│    1 │  61 │
│    1 │  66 │
│    1 │  71 │
│    1 │  76 │
│    1 │  81 │
│    1 │  86 │
│    1 │  91 │
│    1 │  96 │
└──────┴─────┘
┌─part─┬─key─┐
│    2 │   2 │
│    2 │   7 │
│    2 │  12 │
│    2 │  17 │
│    2 │  22 │
│    2 │  27 │
│    2 │  32 │
│    2 │  37 │
│    2 │  42 │
│    2 │  47 │
│    2 │  52 │
│    2 │  57 │
│    2 │  62 │
│    2 │  67 │
│    2 │  72 │
│    2 │  77 │
│    2 │  82 │
│    2 │  87 │
│    2 │  92 │
│    2 │  97 │
└──────┴─────┘
┌─part─┬─key─┐
│    0 │   0 │
│    0 │   5 │
│    0 │  10 │
│    0 │  15 │
│    0 │  20 │
│    0 │  25 │
│    0 │  30 │
│    0 │  35 │
│    0 │  40 │
│    0 │  45 │
│    0 │  50 │
│    0 │  55 │
│    0 │  60 │
│    0 │  65 │
│    0 │  70 │
│    0 │  75 │
│    0 │  80 │
│    0 │  85 │
│    0 │  90 │
│    0 │  95 │
└──────┴─────┘
┌─part─┬─key─┐
│    4 │   4 │
│    4 │   9 │
│    4 │  14 │
│    4 │  19 │
│    4 │  24 │
│    4 │  29 │
│    4 │  34 │
│    4 │  39 │
│    4 │  44 │
│    4 │  49 │
│    4 │  54 │
│    4 │  59 │
│    4 │  64 │
│    4 │  69 │
│    4 │  74 │
│    4 │  79 │
│    4 │  84 │
│    4 │  89 │
│    4 │  94 │
│    4 │  99 │
└──────┴─────┘
┌─part─┬─key─┐
│    3 │   3 │
│    3 │   8 │
│    3 │  13 │
│    3 │  18 │
│    3 │  23 │
│    3 │  28 │
│    3 │  33 │
│    3 │  38 │
│    3 │  43 │
│    3 │  48 │
│    3 │  53 │
│    3 │  58 │
│    3 │  63 │
│    3 │  68 │
│    3 │  73 │
│    3 │  78 │
│    3 │  83 │
│    3 │  88 │
│    3 │  93 │
│    3 │  98 │
└──────┴─────┘

100 rows in set. Elapsed: 1.115 sec. 
```

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
