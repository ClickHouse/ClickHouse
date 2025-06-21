---
slug: /ru/operations/system-tables/grants
---
# system.grants {#system_tables-grants}

Привилегии пользовательских аккаунтов ClickHouse.

Столбцы:
-    `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Название учётной записи.

-    `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Роль, назначенная учетной записи пользователя.

-    `access_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Параметры доступа для учетной записи пользователя ClickHouse.

-    `database` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя базы данных.

-    `table` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя таблицы.

-    `column` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя столбца, к которому предоставляется доступ.

-    `is_partial_revoke` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Логическое значение. Показывает, были ли отменены некоторые привилегии. Возможные значения:
- `0` — Строка описывает грант.
- `1` — Строка описывает частичный отзыв.

-    `grant_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Разрешение предоставлено с опцией `WITH GRANT OPTION`, подробнее см. [GRANT](../../sql-reference/statements/grant.md#grant-privigele-syntax).
