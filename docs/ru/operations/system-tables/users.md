# system.users {#system_tables-users}

Содержит список [аккаунтов пользователей](../../operations/access-rights.md#user-account-management), настроенных на сервере.

Столбцы:
-    `name` ([String](../../sql-reference/data-types/string.md)) — Имя пользователя.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — ID пользователя.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — Путь к хранилищу пользователей. Настраивается в параметре `access_control_path`. 

-    `auth_type` ([Enum8](../../sql-reference/data-types/enum.md)('no_password' = 0,'plaintext_password' = 1, 'sha256_password' = 2, 'double_sha1_password' = 3)) — Показывает тип аутентификации. Существует несколько способов идентификации пользователя: без пароля, с помощью обычного текстового пароля, с помощью шифрования [SHA256] (https://ru.wikipedia.org/wiki/SHA-2) или с помощью шифрования [double SHA-1] (https://ru.wikipedia.org/wiki/SHA-1).

-    `auth_params` ([String](../../sql-reference/data-types/string.md)) — Параметры аутентификации в формате JSON, зависят от `auth_type`.

-    `host_ip` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — IP-адреса хостов, которым разрешено подключаться к серверу ClickHouse.

-    `host_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Имена хостов, которым разрешено подключаться к серверу ClickHouse.

-    `host_names_regexp` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Регулярное выражение для имен хостов, которым разрешено подключаться к серверу ClickHouse.

-    `host_names_like` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Имена хостов, которым разрешено подключаться к серверу ClickHouse, заданные с помощью предиката LIKE.

-    `default_roles_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Показывает, что все предоставленные роли установлены для пользователя по умолчанию.

-    `default_roles_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Список предоставленных ролей по умолчанию.

-    `default_roles_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Все предоставленные роли задаются по умолчанию, за исключением перечисленных.

## Смотрите также {#see-also}

-   [SHOW USERS](../../sql-reference/statements/show.md#show-users-statement)

