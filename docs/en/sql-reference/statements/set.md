---
description: 'Documentation for SET Statement'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'SET Statement'
doc_type: 'reference'
---

# SET Statement

```sql
SET param = value
```

Assigns `value` to the `param` [setting](/operations/settings/overview) for the current session. You cannot change [server settings](../../operations/server-configuration-parameters/settings.md) this way.

You can also set all the values from the specified settings profile in a single query.

```sql
SET profile = 'profile-name-from-the-settings-file'
```

For boolean settings set to true, you can use a shorthand syntax by omitting the value assignment. When only the setting name is specified, it is automatically set to `1` (true).

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

For more information, see [Settings](../../operations/settings/settings.md).
