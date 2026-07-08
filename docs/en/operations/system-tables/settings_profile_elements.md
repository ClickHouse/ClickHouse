---
description: 'System table which describes the content of the settings profile: constraints,
  roles and users that the setting applies to, parent settings profiles.'
keywords: ['system table', 'settings_profile_elements']
slug: /operations/system-tables/settings_profile_elements
title: 'system.settings_profile_elements'
---

# system.settings_profile_elements

Describes the content of the settings profile:

- Сonstraints.
- Roles and users that the setting applies to.
- Parent settings profiles.

Columns:
- `profile_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting profile name.

- `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — User name.

- `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Role name.

- `index` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Sequential number of the settings profile element.

- `setting_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting name.

- `value` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting value.

- `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The minimum value of the setting. `NULL` if not set.

- `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The maximum value of the setting. NULL if not set.

- `writability` ([Nullable](../../sql-reference/data-types/nullable.md)([Enum8](../../sql-reference/data-types/enum.md)('WRITABLE' = 0, 'CONST' = 1, 'CHANGEABLE_IN_READONLY' = 2))) — Sets the settings constraint writability kind.

- `inherit_profile` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — A parent profile for this setting profile. `NULL` if not set. Setting profile will inherit all the settings' values and constraints (`min`, `max`, `readonly`) from its parent profiles.
