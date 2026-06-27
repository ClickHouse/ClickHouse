---
title: 'system.fail_points'
slug: '/en/operations/system-tables/fail_points'
description: 'Contains a list of all available failpoints with their type and current status.'
keywords: ['system table', 'fail_points', 'failpoint', 'testing', 'debug']
doc_type: 'reference'
---

# system.fail_points {#fail_points}

Contains a list of all available failpoints registered in the server, along with their type and whether they are currently enabled.

Failpoints can be enabled and disabled at runtime using the `SYSTEM ENABLE FAILPOINT` and `SYSTEM DISABLE FAILPOINT` statements.

## Columns {#columns}

- `name` ([String](../../sql-reference/data-types/string.md)) — Name of the failpoint.
- `type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the failpoint. Possible values:
  - `'once'` — Triggers a single time and then auto-disables.
  - `'regular'` — Triggers every time the failpoint is hit.
  - `'pauseable_once'` — Blocks execution once until explicitly resumed.
  - `'pauseable'` — Blocks execution every time the failpoint is hit until explicitly resumed.
- `enabled` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether the failpoint is currently enabled. `1` means enabled, `0` means disabled.

## Example {#example}

```sql
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT * FROM system.fail_points WHERE enabled = 1
```

```text
┌─name──────────────────────────────────────┬─type────────────┬─enabled─┐
│ replicated_merge_tree_insert_retry_pause  │ pauseable_once  │       1 │
└───────────────────────────────────────────┴─────────────────┴─────────┘
```
