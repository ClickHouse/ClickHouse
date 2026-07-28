---
description: 'System table containing information about configured roles.'
keywords: ['system table', 'roles']
slug: /operations/system-tables/roles
title: 'system.roles'
---

# system.roles

Contains information about configured [roles](../../guides/sre/user-management/index.md#role-management).

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Role name.
- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — Role ID.
- `storage` ([String](../../sql-reference/data-types/string.md)) — Path to the storage of roles. Configured in the `access_control_path` parameter.

## See Also {#see-also}

- [SHOW ROLES](/sql-reference/statements/show#show-roles)
