---
toc_priority: 50
toc_title: SET
---

# SET Statement {#query-set}

``` sql
SET param = value
```

Assigns `value` to the `param` [setting](../../operations/settings/index.md) for the current session. You cannot change [server settings](../../operations/server-configuration-parameters/index.md) this way.

You can also set all the values from the specified settings profile in a single query.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

For more information, see [Settings](../../operations/settings/settings.md).
