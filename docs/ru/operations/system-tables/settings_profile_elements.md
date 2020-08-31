# system.settings_profile_elements {#system_tables-settings_profile_elements}

Описывает содержимое профиля настроек:

- Ограничения.
- Роли и пользователи, к которым применяется настройка.
- Родительские профили настроек.

Столбцы:
-    `profile_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя профиля настроек.

-    `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя пользователя.

-    `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя роли.

-    `index` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Порядковый номер элемента профиля настроек.

-    `setting_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Имя настройки.

-    `value` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Значение настройки.

-    `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Минимальное значение настройки. `NULL` если не задано.

-    `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Максимальное значение настройки. `NULL` если не задано.

-    `readonly` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges))) — Профиль разрешает только запросы на чтение.

-    `inherit_profile` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Родительский профиль для данного профиля настроек. `NULL` если не задано. Профиль настроек может наследовать все значения и ограничения настроек (`min`, `max`, `readonly`) от своего родительского профиля.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/settings_profile_elements) <!--hide-->
