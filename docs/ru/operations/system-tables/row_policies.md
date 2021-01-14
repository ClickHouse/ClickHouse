# system.row_policies {#system_tables-row_policies}

Содержит фильтры безопасности уровня строк (политики строк) для каждой таблицы, а также список ролей и/или пользователей, к которым применяются эти политики.

Столбцы:
-    `name` ([String](../../sql-reference/data-types/string.md)) — Имя политики строк.

-    `short_name` ([String](../../sql-reference/data-types/string.md)) — Короткое имя политики строк. Имена политик строк являются составными, например: `myfilter ON mydb.mytable`. Здесь `myfilter ON mydb.mytable` — это имя политики строк, `myfilter` — короткое имя.

-    `database` ([String](../../sql-reference/data-types/string.md)) — Имя базы данных.

-    `table` ([String](../../sql-reference/data-types/string.md)) — Имя таблицы.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — ID политики строк.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — Имя каталога, в котором хранится политика строк.

-    `select_filter` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Условие, которое используется для фильтрации строк.

-    `is_restrictive` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Показывает, ограничивает ли политика строк доступ к строкам, подробнее см. [CREATE ROW POLICY](../../sql-reference/statements/create/row-policy.md#create-row-policy-as). Значения:
- `0` — Политика строк определяется с помощью условия 'AS PERMISSIVE'.
- `1` — Политика строк определяется с помощью условия 'AS RESTRICTIVE'.

-    `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Показывает, что политики строк заданы для всех ролей и/или пользователей.

-    `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Список ролей и/или пользователей, к которым применяется политика строк.

-    `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Политики строк применяются ко всем ролям и/или пользователям, за исключением перечисленных.

## Смотрите также {#see-also}

-   [SHOW POLICIES](../../sql-reference/statements/show.md#show-policies-statement)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/row_policies) <!--hide-->
