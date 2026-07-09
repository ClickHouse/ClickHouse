---
description: 'Позволяет выполнять запросы к данным, хранящимся в базе данных SQLite.'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

Позволяет выполнять запросы к данным, хранящимся в базе данных [SQLite](../../engines/database-engines/sqlite.md).

<div id="syntax">
  ## Синтаксис
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## Аргументы
</div>

* `db_path` — Путь к файлу с базой данных SQLite. [String](../../sql-reference/data-types/string.md).
* `table_name` — Имя таблицы в базе данных SQLite или запрос, передаваемый в SQLite как есть (см. [Передача запроса вместо имени таблицы](#passing-a-query)). [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Возвращаемое значение
</div>

* Объект таблицы с теми же столбцами, что и у исходной таблицы `SQLite`.

<div id="passing-a-query">
  ## Передача запроса вместо имени таблицы
</div>

Вместо имени таблицы в качестве второго аргумента можно указать запрос `SELECT`, который передаётся в SQLite как есть. Структура результирующей таблицы определяется автоматически по результату запроса. Запрос можно записать либо как подзапрос, либо обернуть в функцию `query`:

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Такая таблица доступна только для чтения: `INSERT` в неё не допускается. Тот же синтаксис поддерживает и движок таблицы [`SQLite`](/ru/engines/table-engines/integrations/sqlite).

:::note
Форма с подзапросом `(SELECT ...)` разбирается ClickHouse и перед отправкой в SQLite сериализуется заново. Поэтому она должна быть корректной с точки зрения ClickHouse SQL. Чтобы передать синтаксис, специфичный для SQLite и не разбираемый ClickHouse, используйте форму `query('...')`: её текст передаётся в SQLite как есть.

Любые внешние `WHERE`, `LIMIT`, агрегация и т. д. из запроса к ClickHouse **не** проталкиваются в переданный запрос — они применяются в ClickHouse после получения полного результата запроса. Чтобы ограничить объём данных, читаемых из SQLite, поместите фильтр внутрь переданного запроса. При [`external_table_strict_query = 1`](/ru/operations/settings/settings#external_table_strict_query) внешний фильтр, который нельзя протолкнуть, вместо локального применения отклоняется с исключением.
:::

<div id="example">
  ## Пример
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## Связанные материалы
</div>

* [SQLite](../../engines/table-engines/integrations/sqlite.md) — движок таблицы
* [Движок базы данных SQLite](../../engines/database-engines/sqlite.md) — раздел о поддержке типов данных