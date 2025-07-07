---
description: 'Overview of ClickHouse backup and restore'
sidebar_label: 'Overview'
slug: /operations/backup/overview
title: 'Backup and Restore in ClickHouse'
---

import GenericSettings from '@site/docs/operations/backup_restore/_snippets/_generic_settings.md';
import Syntax from '@site/docs/operations/backup_restore/_snippets/_syntax.md';
import AzureSettings from '@site/docs/operations/backup_restore/_snippets/_azure_settings.md';
import S3Settings from '@site/docs/operations/backup_restore/_snippets/_s3_settings.md';

> This section broadly covers backups and restores in ClickHouse. For a more 
detailed description of each backup method, see the pages for specific methods
in the sidebar.

## Introduction {#introduction}

While [replication](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not 
protect against human errors: accidental deletion of data, deletion of the wrong 
table or a table on the wrong cluster, and software bugs that result in incorrect 
data processing or data corruption. 

In many cases mistakes like these will affect all replicas. ClickHouse has built-in
safeguards to prevent some types of mistakes, for example, by [default](/operations/settings/settings#max_table_size_to_drop) 
you can't just drop tables with a `MergeTree` family engine containing more than 
50 Gb of data. However, these safeguards do not cover all possible cases and 
problems can still occur.

To effectively mitigate possible human errors, you should carefully prepare a 
strategy for backing up and restoring your data **in advance**.

Each company has different resources available and business requirements, so 
there's no universal solution for ClickHouse backups and restores that will fit 
every situation. What works for one gigabyte of data likely won't work for tens 
of petabytes of data. There are a variety of possible approaches with their own pros 
and cons, which are presented in this section of the docs. It is a good idea to 
use several approaches instead of just one such as to compensate for their various 
shortcomings.

:::note
Keep in mind that if you backed something up and never tried to restore it, 
chances are that the restore will not work properly when you actually need it (or at 
least it will take longer than the business can tolerate). So whatever backup 
approach you choose, make sure to automate the restore process as well, and practice
it on a spare ClickHouse cluster regularly.
:::

The following pages detail the various backup and 
restore methods available in ClickHouse:

| Page                                                             | Description                                               |
|------------------------------------------------------------------|-----------------------------------------------------------|
| [Backup/restore using local disk or S3 disk](./01_local_disk.md) | Details backup/restore to or from a local disk or S3 disk |
| [Backup/restore using S3 endpoint](./02_s3_endpoint.md)          | Details backup/restore to or from an S3 endpoint          |
| [Backup/restore using AzureBlobStorage](./03_s3_disk.md)       | Details backup/restore to or from Azure blob storage      |
| [Alternative methods](./04_alternative_methods.md)               | Discusses alternative backup methods                      |        

Backups can:
- be [full or incremental](#backup-types)
- [synchronous or asynchronous](#synchronous-vs-asynchronous)
- [concurrent or non-concurrent](#concurrent-vs-non-concurrent)
- [compressed or uncompressed](#compressed-vs-uncompressed)
- use [named collections](#using-named-collections)
- be password protected
- be taken of [system tables, log tables, or access management tables](#system-backups)

## Backup types {#backup-types}

Backups can be either full or incremental. Full backups are a complete copy of the 
data, while incremental backups are a delta of the data from the last full backup.

Full backups have the advantage of being a simple, independent (of other backups)
and reliable recovery method. However, they can take a long time to complete and
can consume a lot of space. Incremental backups, on the other hand, are more 
efficient in terms of both time and space, but restoring the data requires all
the backups to be available.

Depending on your needs, you may want to use:
- **Full backups** for smaller databases or critical data.
- **Incremental backups** for larger databases or when backups need to be done frequently and cost effectively.
- **Both**, for instance, weekly full backups and daily incremental backups.

## Synchronous vs asynchronous backups {#synchronous-vs-asynchronous}

`BACKUP` and `RESTORE` commands can also be marked `ASYNC`. In this case, the 
backup command returns immediately, and the backup process runs in the background.
If the commands are not marked `ASYNC`, the backup process is synchronous and
the command blocks until the backup completes.

## Concurrent vs non-concurrent backups {#concurrent-vs-non-concurrent}

By default, ClickHouse allows concurrent backups and restores. This means you 
can initiate multiple backup or restore operations simultaneously. However, 
there are server-level settings that let you disallow this behavior. If you set
these settings to false, only one backup or restore operation is allowed to run 
on a cluster at a time. This can help avoid resource contention or potential 
conflicts between operations.

To disallow concurrent backup/restore, you can use these settings respectively:

```xml
<clickhouse>
    <backups>
        <allow_concurrent_backups>false</allow_concurrent_backups>
        <allow_concurrent_restores>false</allow_concurrent_restores>
    </backups>
</clickhouse>
```

The default value for both is true, so by default concurrent backup/restores are 
allowed. When these settings are false on a cluster, only a single backup/restore
is allowed to run on a cluster at a time.

## Compressed vs uncompressed backups {#compressed-vs-uncompressed}


## Using Named Collections {#using-named-collections}

Named collections can be used for `BACKUP/RESTORE` parameters.
See the section ["named collections for backups"](../named-collections.md#named-collections-for-backups) for an example.

## Backing up system, log or access management tables {#system-backups}

System tables can also be included in your backup and restore workflows, but their
inclusion depends on your specific use case.

System tables that store historic data, such as those with a `_log` suffix (e.g.,
`query_log`, `part_log`), can be backed up and restored like any other table. 
If your use case relies on analyzing historic data - for example, using `query_log` 
to track query performance or debug issues - it's recommended to include these 
tables in your backup strategy. However, if historic data from these tables is 
not required, they can be excluded to save backup storage space.

System tables related to access management, such as users, roles, row_policies, 
settings_profiles, and quotas, receive special treatment during backup and restore operations.
When these tables are included in a backup, their content is exported to a special 
`accessXX.txt` file, which encapsulates the equivalent SQL statements for creating
and configuring the access entities. Upon restoration, the restore process 
interprets these files and re-applies the SQL commands to recreate the users, 
roles, and other configurations. This feature ensures that the access control 
configuration of a ClickHouse cluster can be backed up and restored as part of 
the cluster's overall setup.

This functionality only works for configurations managed through SQL commands 
(referred to as ["SQL-driven Access Control and Account Management"](/operations/access-rights#enabling-access-control)). 
Access configurations defined in ClickHouse server configuration files (e.g. `users.xml`)
are not included in backups and cannot be restored through this method.

## General syntax {#syntax}

<Syntax/>

### Command summary {#command-summary}

Each of the commands above is detailed below:

| **Command**                                                            | **Description**                                                                                                                                      |
|------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BACKUP`                                                               | Creates a backup of specified objects                                                                                                                |
| `RESTORE`                                                              | Restores objects from a backup                                                                                                                       |
| `[ASYNC]`                                                              | Makes the operation run asynchronously (returns immediately with an ID you can monitor)                                                              |
| `TABLE [db.]table_name [AS [db.]table_name_in_backup]`                 | Backs up/restores a specific table (can be renamed)                                                                                                  |
| `[PARTITION[S] partition_expr [,...]]`                                 | Only backup/restore specific partitions of the table                                                                                                 |
| `DICTIONARY [db.]dictionary_name [AS [db.]name_in_backup]`             | Backs up/restores a dictionary object                                                                                                                |
| `DATABASE database_name [AS database_name_in_backup]`                  | Backs up/restores an entire database (can be renamed)                                                                                                |
| `TEMPORARY TABLE table_name [AS table_name_in_backup]`                 | Backs up/restores a temporary table (can be renamed)                                                                                                 |
| `VIEW view_name [AS view_name_in_backup]`                              | Backs up/restores a view (can be renamed)                                                                                                            |
| `[EXCEPT TABLES ...]`                                                  | Exclude specific tables when backing up a database                                                                                                   |
| `ALL`                                                                  | Backs up/restores everything (all databases, tables, etc.). Prior to version 23.4 of ClickHouse, `ALL` was only applicable to the `RESTORE` command. |
| `[EXCEPT {TABLES\|DATABASES}...]`                                      | Exclude specific tables or databases when using `ALL`                                                                                                |
| `[ON CLUSTER 'cluster_name']`                                          | Execute the backup/restore across a ClickHouse cluster                                                                                               |
| `TO\|FROM`                                                             | Direction: `TO` for backup destination, `FROM` for restore source                                                                                    |
| `File('<path>/<filename>')`                                            | Store to/restore from local file system                                                                                                              |
| `Disk('<disk_name>', '<path>/')`                                       | Store to/restore from a configured disk                                                                                                              |
| `S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>')` | Store to/restore from Amazon S3 or S3-compatible storage                                                                                             |
| `[SETTINGS ...]`                                                       | See below for complete list of settings                                                                                                              |                                                                                                                         |

### Settings {#settings}

**Generic backup/restore settings**

<GenericSettings/>

**S3 specific settings**

<S3Settings/>

**Azure specific settings**

<AzureSettings/>


## Administration and troubleshooting {#check-the-status-of-backups}

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


