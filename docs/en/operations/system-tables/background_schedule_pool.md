---
description: 'System table containing information about tasks in background schedule pools.'
keywords: ['system table', 'background_schedule_pool']
slug: /operations/system-tables/background_schedule_pool
title: 'system.background_schedule_pool'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.background_schedule_pool

<SystemTableCloud/>

Contains information about tasks in background schedule pools. Background schedule pools are used for executing periodic tasks such as distributed sends, buffer flushes, and message broker operations.

Columns:

- `pool` ([String](../../sql-reference/data-types/string.md)) — Pool name. Possible values:
  - `schedule` — General purpose schedule pool
  - `buffer_flush` — Pool for flushing Buffer table data
  - `distributed` — Pool for distributed table operations
  - `message_broker` — Pool for message broker operations
- `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `table_uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Table UUID.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — Query ID (if executing now) (Note, it is not a real query, but just a randomly generated ID for matching logs in `system.text_log`).
- `elapsed_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Task execution time (if executing now).
- `log_name` ([String](../../sql-reference/data-types/string.md)) — Log name for the task.
- `deactivated` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether the task is deactivated (always false, since deactivated tasks are removed from the pool).
- `scheduled` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether the task is scheduled for execution.
- `delayed` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether the task is scheduled with delay.
- `executing` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether the task is currently executing.

**Example**

```sql
SELECT * FROM system.background_schedule_pool LIMIT 5 FORMAT Vertical;
```

```text
Row 1:
──────
pool:        distributed
database:    default
table:       data
table_uuid:  00000000-0000-0000-0000-000000000000
query_id:
elapsed_ms:  0
log_name:    BackgroundJobsAssignee:DataProcessing
deactivated: 0
scheduled:   1
delayed:     0
executing:   0
```

**See Also**

- [system.background_schedule_pool_log](background_schedule_pool_log.md) — Contains history of background schedule pool task executions.
