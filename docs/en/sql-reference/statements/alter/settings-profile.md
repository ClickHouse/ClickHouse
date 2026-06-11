---
description: 'Documentation for Settings Profile'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

Changes settings profiles.

Syntax:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
    [ADD PROFILES 'profile_name' [,...] ]
```
## Incremental vs full replacement {#incremental-vs-full-replacement}

⚠️ A bare `SETTINGS` clause **removes all existing settings and all inherited (parent) profiles** from the profile before applying the new ones.

To change a single setting while keeping the rest, use `ADD SETTINGS` or `MODIFY SETTINGS` (see examples below).

## ADD vs MODIFY {#add-vs-modify}

Both `ADD SETTINGS` and `MODIFY SETTINGS` preserve the other settings in the profile, but they treat an existing entry for the *same* setting differently:

- `ADD SETTINGS variable = value ...` first drops any existing entry for `variable` and then inserts the new one. It therefore **replaces the value together with all constraints** of that setting. Any previously defined `MIN`, `MAX`, or writability (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) for `variable` that you do not repeat is discarded.
- `MODIFY SETTINGS variable = value ...` **merges field by field**: it overrides only the fields you actually specify (the value, or `MIN`, or `MAX`, or the writability) and keeps the other fields of that setting as they were.

:::tip
In short, use `MODIFY SETTINGS` when you only want to tweak one aspect of a setting (e.g. just the value, while keeping an existing `MAX`); use `ADD SETTINGS` when you want to redefine a setting from scratch.
:::

## Examples {#examples}

Create a profile to use in the examples below:

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

### MODIFY SETTINGS {#example-modify-settings}

Add or change a single setting while keeping the others:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

Because `MODIFY` merges field by field, changing only the value of a setting keeps its existing constraints:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

### ADD SETTINGS {#example-add-settings}

Add a setting (also keeping the others), redefining it completely if it already exists:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

Unlike `MODIFY`, re-running `ADD` with only a value drops the previously defined constraints for that setting:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

### DROP SETTINGS {#example-drop-settings}

Remove one or more named settings:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

Remove all settings at once:

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

### Working with inherited profiles {#example-profiles}

Add or remove parent (inherited) profiles without affecting the profile's own settings:

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```
