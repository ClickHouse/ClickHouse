---
sidebar_position: 55
sidebar_label: sqlite
---

## sqlite {#sqlite}

Позволяет выполнять запросы к данным, хранящимся в базе данных [SQLite](../../engines/database-engines/sqlite.md).

**Синтаксис** 

``` sql
    sqlite('db_path', 'table_name')
```

**Аргументы** 

-   `db_path` — путь к файлу с базой данных SQLite. [String](../../sql-reference/data-types/string.md).
-   `table_name` — имя таблицы в базе данных SQLite. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Объект таблицы с теми же столбцами, что и в исходной таблице `SQLite`. 

**Пример**

Запрос:

``` sql
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

Результат:

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

**См. также** 

-   [SQLite](../../engines/table-engines/integrations/sqlite.md) движок таблиц