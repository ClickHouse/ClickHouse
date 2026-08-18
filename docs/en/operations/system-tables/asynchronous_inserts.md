---
description: 'System table containing information about pending asynchronous inserts
  in queue.'
keywords: ['system table', 'asynchronous_inserts']
slug: /operations/system-tables/asynchronous_inserts
title: 'system.asynchronous_inserts'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud/>

Contains information about pending asynchronous inserts in queue.

Columns:

- `query` ([String](../../sql-reference/data-types/string.md)) — Query string.
- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `format` ([String](/sql-reference/data-types/string.md)) — Format name.
- `first_update` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — First insert time with microseconds resolution.
- `total_bytes` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — Total number of bytes waiting in the queue.
- `entries.query_id` ([Array(String)](../../sql-reference/data-types/array.md)) - Array of query ids of the inserts waiting in the queue.
- `entries.bytes` ([Array(UInt64)](../../sql-reference/data-types/array.md)) - Array of bytes of each insert query waiting in the queue.

**Example**

Query:

```sql
SELECT * FROM system.asynchronous_inserts LIMIT 1 \G;
```

Result:

```text
Row 1:
──────
query:            INSERT INTO public.data_guess (user_id, datasource_id, timestamp, path, type, num, str) FORMAT CSV
database:         public
table:            data_guess
format:           CSV
first_update:     2023-06-08 10:08:54.199606
total_bytes:      133223
entries.query_id: ['b46cd4c4-0269-4d0b-99f5-d27668c6102e']
entries.bytes:    [133223]
```

**See Also**

- [system.query_log](/operations/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_insert_log](/operations/system-tables/asynchronous_insert_log) — This table contains information about async inserts performed.
