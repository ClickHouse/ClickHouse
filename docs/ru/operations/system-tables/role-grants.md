# system.role_grants {#system_tables-role_grants}

Содержит [гранты](../../sql-reference/statements/grant.md) ролей для пользователей и ролей. Чтобы добавить записи в эту таблицу, используйте команду `GRANT role TO user`.

Столбцы:

- `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя пользователя.
- `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя роли.
- `granted_role_name` ([String](../../sql-reference/data-types/string.md)) — Имя роли, назначенной для роли `role_name`. Чтобы назначить одну роль другой используйте `GRANT role1 TO role2`.
- `granted_role_is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Флаг, который показывает, является ли `granted_role` ролью по умолчанию. Возможные значения:
    -   1 — `granted_role` является ролью по умолчанию.
    -   0 — `granted_role` не является ролью по умолчанию.
- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Флаг, который показывает, обладает ли `granted_role` роль привилегией `ADMIN OPTION`. Возможные значения:
    -   1 — Роль обладает привилегией `ADMIN OPTION`.
    -   0 — Роль не обладает привилегией `ADMIN OPTION`. 

