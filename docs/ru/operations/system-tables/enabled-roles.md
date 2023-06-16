# system.enabled_roles {#system_tables-enabled_roles}

Содержит все активные роли на данный момент, включая текущую роль текущего пользователя и  роли, назначенные для текущей роли.

Столбцы:

- `role_name` ([String](../../sql-reference/data-types/string.md))) — Имя роли.
- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Флаг, который показывает, обладает ли `enabled_role` роль привилегией `ADMIN OPTION`.
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Флаг, который показывает, является ли `enabled_role` текущей ролью текущего пользователя.
- `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Флаг, который показывает, является ли `enabled_role` ролью по умолчанию.

