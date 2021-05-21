# system.roles {#system_tables-roles}

Содержит сведения о [ролях](../../operations/access-rights.md#role-management).

Столбцы:

- `name` ([String](../../sql-reference/data-types/string.md)) — Имя роли.

- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — ID роли.

- `storage` ([String](../../sql-reference/data-types/string.md)) — Путь к хранилищу ролей. Настраивается в параметре `access_control_path`.

## Смотрите также {#see-also}

-   [SHOW ROLES](../../sql-reference/statements/show.md#show-roles-statement)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/roles) <!--hide-->
