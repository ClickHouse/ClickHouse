---
description: 'Details backup/restore to or from a local disk'
sidebar_label: 'Local disk / S3 disk'
slug: /operations/backup/disk
title: 'Backup and Restore in ClickHouse'
---

import GenericSettings from '@site/docs/operations/backup_restore/_snippets/_generic_settings.md';
import ExampleSetup from '@site/docs/operations/backup_restore/_snippets/_example_setup.md';

# BACKUP / RESTORE to disk {#backup-to-a-local-disk}

## Syntax {#syntax}

```sql

```
## Configure backup destinations for disk {#configure-backup-destinations-for-disk}

### Configure a backup destination for local disk {#configure-a-backup-destination}

In the examples below you will see the backup destination specified as `Disk('backups', '1.zip')`.  
To use the `Disk` backup engine it is necessary to first add a file specifying 
the backup destination at the path below:

```text
/etc/clickhouse-server/config.d/backup_disk.xml
```

For example, the configuration below defines a disk named `backups` and then adds that disk to
the **allowed_disk** list of **backups**:

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

### Configure a backup destination for S3 disk {#backuprestore-using-an-s3-disk}

It is also possible to `BACKUP`/`RESTORE` to S3 by configuring an S3 disk in the
ClickHouse storage configuration. Configure the disk like this by adding a file to
`/etc/clickhouse-server/config.d` as was done above for the local disk.

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

`BACKUP`/`RESTORE` for S3 disk is done in the same way as for local disk:

```sql
BACKUP TABLE data TO Disk('s3_plain', 'cloud_backup');
RESTORE TABLE data AS data_restored FROM Disk('s3_plain', 'cloud_backup');
```

:::note
- This disk should not be used for `MergeTree` itself, only for `BACKUP`/`RESTORE`
- If your tables are backed by S3 storage and the types of the disks are different, 
it doesn't use `CopyObject` calls to copy parts to the destination bucket, instead,
it downloads and uploads them, which is very inefficient. In this case prefer using
the `BACKUP ... TO S3(<endpoint>)` syntax for this use-case.
:::

## Usage examples of backup/restore to local disk {#usage-examples}

### Backup and restore a table {#backup-and-restore-a-table}

<ExampleSetup/>

To backup the table you can run:

```sql title="Query"
BACKUP TABLE test_db.test_table TO Disk('backups', '1.zip')
```

```response title="Response"
   ┌─id───────────────────────────────────┬─status─────────┐
1. │ 065a8baf-9db7-4393-9c3f-ba04d1e76bcd │ BACKUP_CREATED │
   └──────────────────────────────────────┴────────────────┘
```

The table can be restored from the backup using the following command if the table is empty:

```sql title="Query"
RESTORE TABLE test_db.test_table FROM Disk('backups', '1.zip')
```

```response title="Response"
   ┌─id───────────────────────────────────┬─status───┐
1. │ f29c753f-a7f2-4118-898e-0e4600cd2797 │ RESTORED │
   └──────────────────────────────────────┴──────────┘
```

:::note
The above `RESTORE` would fail if the table `test.table` contains data.
The setting `allow_non_empty_tables=true` allows `RESTORE TABLE` to insert data
into non-empty tables. This will mix earlier data in the table with the data extracted from the backup.
This setting can therefore cause data duplication in the table, and should be used with caution.
:::

To restore the table with data already in it, run:

```sql
RESTORE TABLE test_db.table_table FROM Disk('backups', '1.zip')
SETTINGS allow_non_empty_tables=true
```

Tables can be restored, or backed up, with new names:

```sql
RESTORE TABLE test_db.table_table AS test_db.test_table_renamed FROM Disk('backups', '1.zip')
```

The backup archive for this backup has the following structure:

```text
├── .backup
└── metadata
    └── test_db
        └── test_table.sql
```

<!-- TO DO: 
Explanation here about the backup format. See Issue 24a
https://github.com/ClickHouse/clickhouse-docs/issues/3968
--> 

Formats other than zip can be used. See ["Backups as tar archives"](#backups-as-tar-archives)
below for further details.

### Incremental backups to disk {#incremental-backups}

A base backup in ClickHouse is the initial, full backup from which the following 
incremental backups are created. Incremental backups only store the changes 
made since the base backup, so the base backup must be kept available to
restore from any incremental backup. The base backup destination can be set with setting
`base_backup`.

:::note
Incremental backups depend on the base backup. The base backup must be kept available 
to be able to restore from an incremental backup.
:::

To make an incremental backup of a table, first make a base backup:

```sql
BACKUP TABLE test_db.test_table TO Disk('backups', 'd.zip')
```

```sql
BACKUP TABLE test_db.test_table TO Disk('backups', 'incremental-a.zip')
SETTINGS base_backup = Disk('backups', 'd.zip')
```

All data from the incremental backup and the base backup can be restored into a
new table `test_db.test_table2` with command:

```sql
RESTORE TABLE test_db.test_table AS test_db.test_table2
FROM Disk('backups', 'incremental-a.zip');
```

### Securing a backup {#assign-a-password-to-the-backup}

Backups written to disk can have a password applied to the file.
The password can be specified using the `password` setting:

```sql
BACKUP TABLE test_db.test_table
TO Disk('backups', 'password-protected.zip')
SETTINGS password='qwerty'
```

To restore a password-protected backup, the password must again
be specified using the `password` setting:

```sql
RESTORE TABLE test_db.test_table
FROM Disk('backups', 'password-protected.zip')
SETTINGS password='qwerty'
```

### Backups as tar archives {#backups-as-tar-archives}

Backups can be stored not only as zip archives, but also as tar archives. 
The functionality is the same as for zip, except that password protection is not 
supported for tar archives. Additionally, tar archives support a variety of 
compression methods.

To make a backup of a table as a tar:

```sql
BACKUP TABLE test_db.test_table TO Disk('backups', '1.tar')
```

to restore from a tar archive:

```sql
RESTORE TABLE test_db.test_table FROM Disk('backups', '1.tar')
```

To change the compression method, the correct file suffix should be appended to 
the backup name. For example, to compress the tar archive using gzip run:

```sql
BACKUP TABLE test_db.test_table TO Disk('backups', '1.tar.gz')
```

The supported compression file suffixes are:
- `tar.gz`
- `.tgz`
- `tar.bz2`
- `tar.lzma`
- `.tar.zst`
- `.tzst`
- `.tar.xz`

### Compression settings {#compression-settings}

The compression method and level of compression can be specified using
setting `compression_method` and `compression_level` respectively.

<!-- TO DO:
More information needed on these settings and why you would want to do this 
-->

```sql
BACKUP TABLE test_db.test_table
TO Disk('backups', 'filename.zip')
SETTINGS compression_method='lzma', compression_level=3
```

### Restore specific partitions {#restore-specific-partitions}

If specific partitions associated with a table need to be restored, these can be specified.

Let's create a simple partitioned table into four parts, insert some data into it and then
take a backup of only the first and fourth partitions:

<details>

<summary>Setup</summary>

```sql
CREATE IF NOT EXISTS test_db;
       
-- Create a partitioend table
CREATE TABLE test_db.partitioned (
    id UInt32,
    data String,
    partition_key UInt8
) ENGINE = MergeTree()
PARTITION BY partition_key
ORDER BY id;

INSERT INTO test_db.partitioned VALUES
(1, 'data1', 1),
(2, 'data2', 2),
(3, 'data3', 3),
(4, 'data4', 4);

SELECT count() FROM test_db.partitioned;

SELECT partition_key, count() 
FROM test_db.partitioned
GROUP BY partition_key
ORDER BY partition_key;
```

```response
   ┌─count()─┐
1. │       4 │
   └─────────┘
   ┌─partition_key─┬─count()─┐
1. │             1 │       1 │
2. │             2 │       1 │
3. │             3 │       1 │
4. │             4 │       1 │
   └───────────────┴─────────┘
```

</details>

Run the following command to back up partitions 1 and 4:

```sql
BACKUP TABLE test_db.partitioned PARTITIONS '1', '4'
TO Disk('backups', 'partitioned.zip')
```

Run the following command to restore partitions 1 and 4:

```sql
RESTORE TABLE test_db.partitioned PARTITIONS '1', '4'
FROM Disk('backups', 'partitioned.zip')
SETTINGS allow_non_empty_tables=true
```

## Administration and Troubleshooting {#check-the-status-of-backups}

The backup command returns an `id` and `status`, and that `id` can be used to
get the status of the backup. This is very useful to check the progress of long
`ASYNC` backups. The example below shows a failure that happened when trying to
overwrite an existing backup file:

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

Along with the [`system.backups`](/operations/system-tables/backups) table, all backup and restore operations are also tracked in the system log table
[`system.backup_log`](/operations/system-tables/backup_log):

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
