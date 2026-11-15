---
description: 'System table containing information about metadata files read from Delta Lake tables. Each entry
  represents a root metadata JSON file.'
keywords: ['system table', 'delta_lake_metadata_log']
slug: /operations/system-tables/delta_lake_metadata_log
title: 'system.delta_lake_metadata_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.delta_lake_metadata_log

The `system.delta_lake_metadata_log` table records metadata access and parsing events for Delta Lake tables read by ClickHouse. It provides detailed information about each metadata file, which is useful for debugging, auditing, and understanding Delta table structure evolution.

## Purpose {#purpose}

This table logs every metadata file read from Delta Lake tables. It helps users trace how ClickHouse interprets Delta table metadata and diagnose issues related to schema evolution, snapshot resolution, or query planning.

:::note
This table is primarily intended for debugging purposes.
:::

## Columns {#columns}
| Name           | Type      | Description                                                                                   |
|----------------|-----------|----------------------------------------------------------------------------------------------|
| `event_date`   | [Date](../../sql-reference/data-types/date.md)      | Date of the log file.                                                                       |
| `event_time`   | [DateTime](../../sql-reference/data-types/datetime.md)  | Timestamp of the event.                                                                      |
| `query_id`     | [String](../../sql-reference/data-types/string.md)    | Query ID that triggered the metadata read.                                                   |
| `table_path`   | [String](../../sql-reference/data-types/string.md)    | Path to the Delta Lake table.                                                                |
| `file_path`    | [String](../../sql-reference/data-types/string.md)    | Path to the root metadata JSON file.             |
| `content`      | [String](../../sql-reference/data-types/string.md)    | Content in JSON format (raw metadata from .json).       |

<SystemTableCloud/>

## Controlling log verbosity {#controlling-log-verbosity}

You can control which metadata events are logged using the [`delta_lake_log_metadata`](../../operations/settings/settings.md#delta_lake_log_metadata) setting.

To log all metadata used in the current query:

```sql
SELECT * FROM my_delta_table SETTINGS delta_lake_log_metadata = 1;

SYSTEM FLUSH LOGS delta_lake_metadata_log;

SELECT *
FROM system.delta_lake_metadata_log
WHERE query_id = '{previous_query_id}';
```
