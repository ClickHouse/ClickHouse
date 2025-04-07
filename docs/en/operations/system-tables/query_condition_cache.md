---
description: 'System table which shows the content of the query condition cache.'
keywords: ['system table', 'query_condition_cache']
slug: /operations/system-tables/query_condition_cache
title: 'system.query_condition_cache'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.query_condition_cache

<SystemTableCloud/>

Shows the content of the [query condition cache](../query-condition-cache.md).

Columns:

- `table_uuid` ([String](../../sql-reference/data-types/string.md)) — The table UUID.
- `part_name` ([String](../../sql-reference/data-types/string.md)) — The part name.
- `key_hash` ([String](/sql-reference/data-types/string.md)) — The hash of the filter condition.
- `entry_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of the entry in bytes.
- `matching_marks` ([String](../../sql-reference/data-types/string.md)) — Matching marks.

**Example**

``` sql
SELECT * FROM system.query_condition_cache FORMAT Vertical;
```

``` text
Row 1:
──────
table_uuid:     28270a24-ea27-49f6-99cd-97b9bee976ac
part_name:      all_1_1_0
key_hash:       5456494897146899690 -- 5.46 quintillion
entry_size:     40
matching_marks: 111111110000000000000000000000000000000000000000000000000111111110000000000000000

1 row in set. Elapsed: 0.004 sec.
```
