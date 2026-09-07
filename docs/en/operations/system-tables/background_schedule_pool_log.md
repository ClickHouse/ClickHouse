---
description: 'System table containing history of background schedule pool task executions.'
keywords: ['system table', 'background_schedule_pool_log']
slug: /operations/system-tables/background_schedule_pool_log
title: 'system.background_schedule_pool_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.background_schedule_pool_log

<SystemTableCloud/>

The `system.background_schedule_pool_log` table is created only if the [background_schedule_pool_log](/operations/server-configuration-parameters/settings#background_schedule_pool_log) server setting is specified.

This table contains the history of background schedule pool task executions. Background schedule pools are used for executing periodic tasks such as distributed sends, buffer flushes, and message broker operations.

The `system.background_schedule_pool_log` table contains the following columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds precision.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — Identifier of the query associated with the background task (Note, it is not a real query, but just a randomly generated ID for matching logs in `system.text_log`).
- `database` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Name of the database.
- `table` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Name of the table.
- `table_uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — UUID of the table the background task belongs to.
- `log_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Name of the background task.
- `duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Duration of the task execution in milliseconds.
- `error` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The error code of the occurred exception.
- `exception` ([String](../../sql-reference/data-types/string.md)) — Text message of the occurred error.

The `system.background_schedule_pool_log` table is created after the first background task execution.

**Example**

```sql
SELECT * FROM system.background_schedule_pool_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-12-18
event_time:              2025-12-18 10:30:15
event_time_microseconds: 2025-12-18 10:30:15.123456
query_id:
database:                default
table:                   data
table_uuid:              00000000-0000-0000-0000-000000000000
log_name:                default.data
duration_ms:             42
error:                   0
exception:
```

**See Also**

- [system.background_schedule_pool](background_schedule_pool.md) — Contains information about currently scheduled tasks in background schedule pools.
