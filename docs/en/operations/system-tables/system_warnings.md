---
description: 'This table contains warning messages about clickhouse server.'
keywords: [ 'system table', 'warnings' ]
slug: /operations/system-tables/system_warnings
title: 'system.warnings'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.warnings

<SystemTableCloud/>

This table shows warnings about the ClickHouse server.
Warnings of the same type are combined into a single warning.
For example, if the number N of attached databases exceeds a configurable threshold T, a single entry containing the current value N is shown instead of N separate entries.
If current value drops below the threshold, the entry is removed from the table.

The table can be configured with these settings:

- [max_table_num_to_warn](../server-configuration-parameters/settings.md#max_table_num_to_warn)
- [max_database_num_to_warn](../server-configuration-parameters/settings.md#max_database_num_to_warn)
- [max_dictionary_num_to_warn](../server-configuration-parameters/settings.md#max_dictionary_num_to_warn)
- [max_view_num_to_warn](../server-configuration-parameters/settings.md#max_view_num_to_warn)
- [max_part_num_to_warn](../server-configuration-parameters/settings.md#max_part_num_to_warn)
- [max_pending_mutations_to_warn](../server-configuration-parameters/settings.md#max_pending_mutations_to_warn)

Columns:

- `message` ([String](../../sql-reference/data-types/string.md)) — Warning message.
- `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — The format string used to format the message.

**Example**

Query:

```sql
 SELECT * FROM system.warnings LIMIT 2 \G;
```

Result:

```text
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```
