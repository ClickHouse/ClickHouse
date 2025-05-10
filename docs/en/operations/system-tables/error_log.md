---
description: 'System table containing the history of error values from table `system.errors`,
  periodically flushed to disk.'
keywords: ['system table', 'error_log']
slug: /operations/system-tables/error_log
title: 'system.error_log'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud/>

Contains history of error values from table `system.errors`, periodically flushed to disk.

Columns:
- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `code` ([Int32](../../sql-reference/data-types/int-uint.md)) — Code number of the error.
- `error` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) - Name of the error.
- `last_error_time` ([DateTime](../../sql-reference/data-types/datetime.md))  — The time when the last error happened.
- `last_error_message` ([String](../../sql-reference/data-types/string.md)) — Message for the last error.
- `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of times this error happened.
- `remote` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Remote exception (i.e. received during one of the distributed queries).
- `last_error_query_id` ([String](../../sql-reference/data-types/string.md)) — Id of a query that caused the last error (if available).
- `last_error_trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — A stack trace that represents a list of physical addresses where the called methods are stored.
- `last_error_symbols` ([Array(LowCardinality(String))](../../sql-reference/data-types/array.md)), If the symbolization is enabled, contains demangled symbol names, corresponding to the `last_error_trace`.
- `last_error_lines` ([Array(LowCardinality(String))](../../sql-reference/data-types/array.md)), If the symbolization is enabled, contains strings with file names with line numbers, corresponding to the `last_error_trace`.

The symbolization can be enabled or disabled in the `symbolize` under `trace_log` in the server's configuration file.

**Example**

```sql
SELECT * FROM system.error_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2024-06-18
event_time: 2024-06-18 07:32:39
code:       999
error:      KEEPER_EXCEPTION
value:      2
remote:     0
```

**See also**

- [error_log setting](../../operations/server-configuration-parameters/settings.md#error_log) — Enabling and disabling the setting.
- [system.errors](../../operations/system-tables/errors.md) — Contains error codes with the number of times they have been triggered.
- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
