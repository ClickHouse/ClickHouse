---
description: 'This table contains warning messages about clickhouse server.'
keywords: [ 'system table', 'warnings' ]
slug: /en/operations/system-tables/system_warnings
title: 'system.warnings'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.warnings

<SystemTableCloud/>

This table contains warning messages about clickhouse server. The threshold for some of the warnings can be configured
accordingly. When the warning threshold is crossed, clickhouse server will add a warning message to `system.warnings`
table. It will also update a warning message according to the current value and remove it when it drops below the
configured threshold. Currently, the following warnings are configurable.

### Maximum number of tables {#max_num_of_tables}

For warning when the maximum number of attached tables exceeds the specified value.

**Configuration:**

```xml
<max_table_num_to_warn>500</max_table_num_to_warn>
```

### Maximum number of databases {#max_num_of_databases}

For warning when the number of attached databases exceeds the specified value.

**Configuration:**

```xml
<max_database_num_to_warn>500</max_database_num_to_warn>
```

### Maximum number of dictionaries {#max_num_of_dictionaries}

For warning when the number of attached dictionaries exceeds the specified value.

**Configuration:**

```xml
<max_dictionary_num_to_warn>500</max_dictionary_num_to_warn>
```

### Maximum number of views {#max_num_of_views}

For warning when the number of attached views exceeds the specified value.

**Configuration:**

```xml
<max_view_num_to_warn>500</max_view_num_to_warn>
```

### Maximum number of parts {#max_num_of_parts}

For warning when the number of active parts exceeds the specified value.

**Configuration:**

```xml
<max_part_num_to_warn>500</max_part_num_to_warn>
```

### Maximum number of pending mutations {#max_num_of_pending_mutations}

For warning when the number of pending mutations exceeds the specified value.

**Configuration:**

```xml
<max_pending_mutations_to_warn>500</max_pending_mutations_to_warn>
```

Columns:

- `message` ([String](../../sql-reference/data-types/string.md)) — Warning message.
- `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — The format string that
  was used to format the message.

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
