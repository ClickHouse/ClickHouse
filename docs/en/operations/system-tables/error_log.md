---
description: 'System table containing the history of error values from table `system.errors`,
  periodically flushed to disk.'
keywords: ['system table', 'error_log']
slug: /operations/system-tables/system-error-log
title: 'system.error_log'
doc_type: 'reference'
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
- `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of times this error happened.
- `remote` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Remote exception (i.e. received during one of the distributed queries).
- `last_error_time` ([DateTime](../../sql-reference/data-types/datetime.md))  — The time when the last error happened.
- `last_error_message` ([String](../../sql-reference/data-types/string.md)) — Message for the last error.
- `last_error_query_id` ([String](../../sql-reference/data-types/string.md)) — Id of a query that caused the last error (if available).
- `last_error_trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — A stack trace that represents a list of physical addresses where the called methods are stored.

**Example**

```sql
SELECT * FROM system.error_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:            clickhouse.testing.internal
event_date:          2025-11-11
event_time:          2025-11-11 11:35:28
code:                60
error:               UNKNOWN_TABLE
value:               1
remote:              0
last_error_time:     2025-11-11 11:35:28
last_error_message:  Unknown table expression identifier 'system.table_not_exist' in scope SELECT * FROM system.table_not_exist
last_error_query_id: 77ad9ece-3db7-4236-9b5a-f789bce4aa2e
last_error_trace:    [100506790044914,100506534488542,100506409937998,100506409936517,100506425182891,100506618154123,100506617994473,100506617990486,100506617988112,100506618341386,100506630272160,100506630266232,100506630276900,100506629795243,100506633519500,100506633495783,100506692143858,100506692248921,100506790779783,100506790781278,100506790390399,100506790380047,123814948752036,123814949330028]
```

**See also**

- [error_log setting](../../operations/server-configuration-parameters/settings.md#error_log) — Enabling and disabling the setting.
- [system.errors](../../operations/system-tables/errors.md) — Contains error codes with the number of times they have been triggered.
- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
