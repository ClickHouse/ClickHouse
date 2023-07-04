# system.current_roles {#system_tables-current_roles}

Содержит активные роли текущего пользователя. `SET ROLE` изменяет содержимое этой таблицы.

Столбцы:

 - `role_name` ([String](../../sql-reference/data-types/string.md))) — Имя роли.
 - `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Флаг, который показывает, обладает ли `current_role` роль привилегией `ADMIN OPTION`.
 - `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) —  Флаг, который показывает, является ли `current_role` ролью по умолчанию.

