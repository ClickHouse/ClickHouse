---
description: 'System table which shows the content of the query cache.'
keywords: ['system table', 'query_cache']
slug: /operations/system-tables/query_cache
title: 'system.query_cache'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.query_cache

<SystemTableCloud/>

Shows the content of the [query cache](../query-cache.md).

Columns:

- `query` ([String](../../sql-reference/data-types/string.md)) — Query string.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the query.
- `result_size` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — Size of the query cache entry.
- `tag` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — Tag of the query cache entry.
- `stale` ([UInt8](../../sql-reference/data-types/int-uint.md)) — If the query cache entry is stale.
- `shared` ([UInt8](../../sql-reference/data-types/int-uint.md)) — If the query cache entry is shared between multiple users.
- `compressed` ([UInt8](../../sql-reference/data-types/int-uint.md)) — If the query cache entry is compressed.
- `expires_at` ([DateTime](../../sql-reference/data-types/datetime.md)) — When the query cache entry becomes stale.
- `key_hash` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — A hash of the query string, used as a key to find query cache entries.

**Example**

```sql
SELECT * FROM system.query_cache FORMAT Vertical;
```

```text
Row 1:
──────
query:       SELECT 1 SETTINGS use_query_cache = 1
query_id:    7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
result_size: 128
tag:
stale:       0
shared:      0
compressed:  1
expires_at:  2023-10-13 13:35:45
key_hash:    12188185624808016954

1 row in set. Elapsed: 0.004 sec.
```
