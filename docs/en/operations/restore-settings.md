---
description: 'Settings of the RESTORE query that control which categories of objects are restored from a backup.'
sidebar_label: 'Restore settings'
sidebar_position: 71
slug: /operations/restore-settings
title: 'Restore settings'
keywords: ['backup', 'restore', 'structure_only']
doc_type: 'reference'
---

# Restore settings {#restore-settings}

The `RESTORE` query accepts settings in its `SETTINGS` clause that control which
categories of objects are read from a backup. This page documents the settings
that select what is restored: table data, access entities, and user-defined functions.

## `structure_only` {#structure-only}

Type: `Bool`. Default: `false`.

When set to `true`, only `CREATE` queries are read from the backup, without the
contents of the restored objects. It is a convenient shortcut that sets the
effective defaults of all three category settings below to `false`:

- `restore_table_data`
- `restore_access_entities`
- `restore_functions`

## `restore_table_data` {#restore-table-data}

Type: `Bool`. Optional; when not set it defaults to `!structure_only` (i.e. `true`
unless `structure_only` is set).

Whether to restore the data of tables. When set explicitly it overrides
`structure_only`: set it to `true` to restore table data even when
`structure_only=true`, or to `false` to skip table data even when `structure_only`
is not set.

## `restore_access_entities` {#restore-access-entities}

Type: `Bool`. Optional; when not set it defaults to `!structure_only`.

Whether to restore access entities — users, roles, settings profiles, row policies,
and quotas. When set explicitly it overrides `structure_only`.

## `restore_functions` {#restore-functions}

Type: `Bool`. Optional; when not set it defaults to `!structure_only`.

Whether to restore user-defined functions. When set explicitly it overrides
`structure_only`.

## Effective behavior {#effective-behavior}

Each category is restored when its corresponding setting is `true`. When a category
setting is not specified, its value is derived from `structure_only`: all three are
`true` by default, and all three become `false` when `structure_only=true`.

| `structure_only` | `restore_*` not set | `restore_* = true` | `restore_* = false` |
|:----------------:|:-------------------:|:------------------:|:-------------------:|
| `false`          | restored            | restored           | skipped             |
| `true`           | skipped             | restored           | skipped             |

This is fully backward compatible: a `RESTORE` query that does not set any of the
new settings behaves exactly as before.

## Examples {#examples}

Restore table definitions and access entities, but not table data — for example
to set up an environment for testing or replaying queries without copying data:

```sql
RESTORE ALL FROM backup_name
SETTINGS structure_only = true, restore_access_entities = true
```

Restore only table data, without re-creating access entities or user-defined
functions:

```sql
RESTORE ALL FROM backup_name
SETTINGS structure_only = true, restore_table_data = true
```

Restore everything except user-defined functions:

```sql
RESTORE ALL FROM backup_name
SETTINGS restore_functions = false
```
