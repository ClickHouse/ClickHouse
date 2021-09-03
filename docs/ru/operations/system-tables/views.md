# system.views {#system-views}

Содержит зависимости всех представлений и их тип. Метаданные представления поступают из [system.tables](tables.md).

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, в которой находится представление.

-   `name` ([String](../../sql-reference/data-types/string.md)) — имя представления.

-   `main_dependency_database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных, от которой зависит представление.

-   `main_dependency_table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы, от которой зависит представление.

-   `view_type` ([Enum8](../../sql-reference/data-types/enum.md)) — тип представления. Возможные значения:
    -   `'Default' = 1` — [представления по умолчанию](../../sql-reference/statements/create/view.md#normal). Не должны быть в этом журнале.
    -   `'Materialized' = 2` — [материализованные представления](../../sql-reference/statements/create/view.md#materialized).
    -   `'Live' = 3` — [LIVE-представления](../../sql-reference/statements/create/view.md#live-view).

**Пример**

Запрос:

```sql
SELECT * FROM system.views LIMIT 2 FORMAT Vertical;
```

Результат:

```text
Row 1:
──────
database:                 default
name:                     live_view
main_dependency_database: default
main_dependency_table:    view_source_tb
view_type:                Live

Row 2:
──────
database:                 default
name:                     materialized_view
main_dependency_database: default
main_dependency_table:    view_source_tb
view_type:                Materialized
```
