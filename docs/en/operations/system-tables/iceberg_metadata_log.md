---
description: 'System table containing information about metadata files read from Iceberg tables. Each entry
  represents either a root metadata file, metadata extracted from an Avro file, or an entry of some Avro file.'
keywords: ['system table', 'iceberg_metadata_log']
slug: /operations/system-tables/iceberg_metadata_log
title: 'system.iceberg_metadata_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.iceberg_metadata_log

The `system.iceberg_metadata_log` table records metadata access and parsing events for Iceberg tables read by ClickHouse. It provides detailed information about each metadata file or entry processed, which is useful for debugging, auditing, and understanding Iceberg table structure evolution.

## Purpose {#purpose}

This table logs every metadata file and entry read from Iceberg tables, including root metadata files, manifest lists, and manifest entries. It helps users trace how ClickHouse interprets Iceberg table metadata and diagnose issues related to schema evolution, file resolution, or query planning.

:::note
This table is primarily intended for debugging purposes.
:::

## Columns {#columns}

| Name           | Type      | Description                                                                                   |
|----------------|-----------|----------------------------------------------------------------------------------------------|
| `event_date`   | [Date](../../sql-reference/data-types/date.md)      | Date of the log entry.                                                                       |
| `event_time`   | [DateTime](../../sql-reference/data-types/datetime.md)  | Timestamp of the event.                                                                      |
| `query_id`     | [String](../../sql-reference/data-types/string.md)    | Query ID that triggered the metadata read.                                                   |
| `content_type` | [Enum8](../../sql-reference/data-types/enum.md)     | Type of metadata content (see below).                                                        |
| `table_path`   | [String](../../sql-reference/data-types/string.md)    | Path to the Iceberg table.                                                                   |
| `file_path`    | [String](../../sql-reference/data-types/string.md)    | Path to the root metadata JSON file, Avro manifest list, or manifest file.                   |
| `content`      | [String](../../sql-reference/data-types/string.md)    | Content in JSON format (raw metadata from .json, Avro metadata, or Avro entry).              |
| `row_in_file`  | [Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md)) | Row number in the file, if applicable. Present for `ManifestListEntry` and `ManifestFileEntry` content types. |

## `content_type` values {#content-type-values}

- `None`: No content.
- `Metadata`: Root metadata file.
- `ManifestListMetadata`: Manifest list metadata.
- `ManifestListEntry`: Entry in a manifest list.
- `ManifestFileMetadata`: Manifest file metadata.
- `ManifestFileEntry`: Entry in a manifest file.

<SystemTableCloud/>

## Controlling log verbosity {#controlling-log-verbosity}

You can control which metadata events are logged using the [`iceberg_metadata_log_level`](../../operations/settings/settings.md#iceberg_metadata_log_level) setting.

To log all metadata used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'manifest_file_entry';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

To log only the root metadata JSON file used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'metadata';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

See more information in the description of the [`iceberg_metadata_log_level`](../../operations/settings/settings.md#iceberg_metadata_log_level) setting.

### Good To Know {#good-to-know}

- Use `iceberg_metadata_log_level` at the query level only when you need to investigate your Iceberg table in detail. Otherwise, you may populate the log table with excessive metadata and experience performance degradation.
- The table may contain duplicate entries, as it is intended primarily for debugging and does not guarantee uniqueness per entity.
- If you use a `content_type` more verbose than `ManifestListMetadata`, the Iceberg metadata cache is disabled for manifest lists.
- Similarly, if you use a `content_type` more verbose than `ManifestFileMetadata`, the Iceberg metadata cache is disabled for manifest files.

## See also {#see-also}
- [Iceberg Table Engine](../../engines/table-engines/integrations/iceberg.md)
- [Iceberg Table Function](../../sql-reference/table-functions/iceberg.md)
- [system.iceberg_history](./iceberg_history.md)
