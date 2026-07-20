---
description: 'Documentation for SET Statement'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'SET Statement'
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

For more information, see [Settings](../../operations/settings/settings.md).
