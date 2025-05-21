---
description: "System table containing all active roles at the moment, including the current role of the current user and the granted roles for the current role"
slug: /operations/system-tables/enabled-roles
title: "enabled_roles"
keywords: ["system table", "enabled_roles"]
---

Contains all active roles at the moment, including the current role of the current user and granted roles for the current role.

Columns:

- `role_name` ([String](../../sql-reference/data-types/string.md))) — Role name.
- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `enabled_role` is a role with `ADMIN OPTION` privilege.
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `enabled_role` is a current role of a current user.
- `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `enabled_role` is a default role.
