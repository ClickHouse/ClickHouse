# system.settings_profiles {#system_tables-settings_profiles}

Содержит свойства сконфигурированных профилей настроек.

Столбцы:
-    `name` ([String](../../sql-reference/data-types/string.md)) — Имя профиля настроек.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — ID профиля настроек.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — Путь к хранилищу профилей настроек. Настраивается в параметре `access_control_path`. 

-    `num_elements` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Число элементов для этого профиля в таблице `system.settings_profile_elements`.

-    `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Признак, который показывает, что параметры профиля заданы для всех ролей и/или пользователей.

-    `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Список ролей и/или пользователей, к которым применяется профиль настроек.

-    `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Профиль настроек применяется ко всем ролям и/или пользователям, за исключением перечисленных.

## Смотрите также {#see-also}

-   [SHOW PROFILES](../../sql-reference/statements/show.md#show-profiles-statement)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/settings_profiles) <!--hide-->
