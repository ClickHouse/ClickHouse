---
description: 'System table containing logging entries with information about `BACKUP`
  and `RESTORE` operations.'
keywords: ['system table', 'backups']
slug: /operations/system-tables/backups
title: 'system.backups'
doc_type: 'reference'
---

# system.backups

Contains a list of all `BACKUP` or `RESTORE` operations with their current states and other properties. Note, that table is not persistent and it shows only operations executed after the last server restart.

Here's the markdown table with the name and comment columns:

| Column              | Description                                                                                                          |
|---------------------|----------------------------------------------------------------------------------------------------------------------|
| `id`                | Operation ID, can be either passed via SETTINGS id=... or be randomly generated UUID.                                |
| `name`              | Operation name, a string like `Disk('backups', 'my_backup')`                                                         |
| `base_backup_name`  | Base Backup Operation name, a string like `Disk('backups', 'my_base_backup')`                                        |
| `query_id`          | Query ID of a query that started backup.                                                                             |
| `status`            | Status of backup or restore operation.                                                                               |
| `error`             | The error message if any.                                                                                            |
| `start_time`        | The time when operation started.                                                                                     |
| `end_time`          | The time when operation finished.                                                                                    |
| `num_files`         | The number of files stored in the backup.                                                                            |
| `total_size`        | The total size of files stored in the backup.                                                                        |
| `num_entries`       | The number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder. |
| `uncompressed_size` | The uncompressed size of the backup.                                                                                 |
| `compressed_size`   | The compressed size of the backup.                                                                                   |
| `files_read`        | Returns the number of files read during RESTORE from this backup.                                                    |
| `bytes_read`        | Returns the total size of files read during RESTORE from this backup.                                                |
| `ProfileEvents`     | All the profile events captured during this operation.                                                               |
