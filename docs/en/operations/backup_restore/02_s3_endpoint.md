---
description: 'Overview of ClickHouse backup and restore'
sidebar_label: 'S3 endpoint'
slug: /operations/backup/s3_endpoint
title: 'Backup and restore to/from an S3 endpoint'
---

# BACKUP / RESTORE to or from an S3 endpoint {#backup-to-a-local-disk}

This article covers backing up or restoring backups to/from an S3 bucket
via an S3 endpoint.

## Syntax {#syntax}

```sql

```

## Usage example {#usage-examples}

### Incremental backup to an S3 endpoint {#incremental-backup-to-an-s3-endpoint}

In this example, we will create a backup to an S3 endpoint and then restore from it
again.

:::note
For an explanation of the differences between a full backup and an incremental
backup, see ["Backup types"](/operations/backup/overview/#backup-types)
:::

You will need the following information to use this method:

| Parameter         | Example                                                      |
|-------------------|--------------------------------------------------------------|
| An S3 endpoint    | `https://backup-ch-docs.s3.us-east-1.amazonaws.com/backups/` |
| Access key ID     | `BKIOZLE2VYN3VXXTP9RC`                                       |
| Secret access key | `40bwYnbqN7xU8bVePaUCh3+YEyGXu8UOMV9ANpwL`                   |

:::tip
Creating an S3 bucket is covered in section ["use S3 Object Storage as a ClickHouse disk"](/integrations/data-ingestion/s3/index.md#configuring-s3-for-clickhouse-use)
:::

The destination for a backup is specified as:

```sql
S3('<s3 endpoint>/<directory>', '<access key id>', '<secret access key>', '<extra_credentials>')
```
<br/>
<VerticalStepper headerLevel="h4">

#### Setup {#create-a-table}

Create the following database and table and insert some random data into it:

```sql
CREATE DATABASE IF NOT EXISTS test_db;
CREATE TABLE test_db.test_table
(
    `key` Int,
    `value` String,
    `array` Array(String)
)
ENGINE = MergeTree
ORDER BY tuple()
```

```sql
INSERT INTO test_db.test_table SELECT *
FROM generateRandom('key Int, value String, array Array(String)')
LIMIT 1000
```

#### Create a base backup {#create-a-base-initial-backup}

Incremental backups require a _base_ backup to start from. The first parameter of
the S3 destination is the S3 endpoint followed by the directory within the bucket
to use for this backup. In this example the directory is named `my_backup`.

Run the following command to create the base backup:

```sql
BACKUP TABLE test_db.test_table TO S3(
'https://backup-ch-docs.s3.us-east-1.amazonaws.com/backups/base_backup',
'<access key id>',
'<secret access key>'
)
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ de442b75-a66c-4a3c-a193-f76f278c70f3 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```

#### Add more data {#add-more-data}

Incremental backups are populated with the difference between the base backup and
the current content of the table being backed up. Add more data before taking the
incremental backup:

```sql
INSERT INTO test_db.test_table SELECT *
FROM generateRandom('key Int, value String, array Array(String)')
LIMIT 100
```

#### Take an incremental backup {#take-an-incremental-backup}

This backup command is similar to the base backup, but adds `SETTINGS base_backup` and the location of the base backup.  Note that the destination for the incremental backup is not the same directory as the base, it is the same endpoint with a different target directory within the bucket.  The base backup is in `my_backup`, and the incremental will be written to `my_incremental`:

```sql
BACKUP TABLE test_db.test_table TO S3(
'https://backup-ch-docs.s3.us-east-1.amazonaws.com/backups/incremental_backup',
'<access key id>',
'<secret access key>'
)
SETTINGS base_backup = S3(
'https://backup-ch-docs.s3.us-east-1.amazonaws.com/backups/base_backup',
'<access key id>',
'<secret access key>'
)
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ f6cd3900-850f-41c9-94f1-0c4df33ea528 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```

#### Restore from the incremental backup {#restore-from-the-incremental-backup}

This command restores the incremental backup into a new table, `test_table_restored`.  
Note that when an incremental backup is restored, the base backup is also included. 
Specify only the **incremental backup** when restoring:

```sql
RESTORE TABLE data AS test_db.test_table_restored FROM S3(
'https://backup-ch-docs.s3.us-east-1.amazonaws.com/backups/incremental_backup',
'<access key id>',
'<secret access key>'
)
```

```response
┌─id───────────────────────────────────┬─status───┐
│ ff0c8c39-7dff-4324-a241-000796de11ca │ RESTORED │
└──────────────────────────────────────┴──────────┘
```

#### Verify the count {#verify-the-count}

There were two inserts into the original table `data`, one with 1,000 rows and one with 100 rows, for a total of 1,100. 
Verify that the restored table has 1,100 rows:

```sql
SELECT count()
FROM test_db.test_table_restored
```

```response
┌─count()─┐
│    1100 │
└─────────┘
```

#### Verify the content {#verify-the-content}

This compares the content of the original table, `test_table` with the restored table `test_table_restored`:

```sql
SELECT throwIf((
   SELECT groupArray(tuple(*))
   FROM test_db.test_table
   ) != (
   SELECT groupArray(tuple(*))
   FROM test_db.test_table_restored
), 'Data does not match after BACKUP/RESTORE')
```

</VerticalStepper>
