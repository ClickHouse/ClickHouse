---
description: 'Движок позволяет импортировать и экспортировать данные в SQLite, а также выполнять запросы
  к таблицам SQLite напрямую из ClickHouse.'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'Движок таблицы SQLite'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # Движок таблицы SQLite
</div>

<CloudNotSupportedBadge />

Этот движок позволяет импортировать и экспортировать данные в SQLite, а также выполнять запросы к таблицам SQLite напрямую из ClickHouse.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Параметры движка**

* `db_path` — Путь к файлу SQLite с базой данных.
* `table` — Имя таблицы в базе данных SQLite или запрос, который передаётся в SQLite как есть (см. [Передача запроса вместо имени таблицы](#passing-a-query)).

<div id="passing-a-query">
  ## Передача запроса вместо имени таблицы
</div>

Вместо имени таблицы в качестве аргумента `table` можно указать запрос `SELECT`, который передаётся в SQLite как есть. Структура таблицы определяется по результату запроса. Запрос можно записать либо как подзапрос, либо обернуть в функцию `query`:

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Такая таблица доступна только для чтения: `INSERT` в неё не допускается. Тот же синтаксис поддерживается табличной функцией [`sqlite`](/ru/sql-reference/table-functions/sqlite).

:::note
Форма с подзапросом `(SELECT ...)` разбирается ClickHouse и затем повторно сериализуется перед отправкой в SQLite. Поэтому она должна быть корректным ClickHouse SQL. Чтобы передать синтаксис, специфичный для SQLite и не разбираемый ClickHouse, используйте форму `query('...')`, текст которой передаётся в SQLite дословно.

Любой внешний `WHERE`, `LIMIT`, агрегация и т. д. из окружающего запроса к ClickHouse **не** переносится в переданный запрос — всё это применяется в ClickHouse после получения полного результата запроса. Чтобы ограничить объём данных, читаемых из SQLite, поместите фильтр внутрь переданного запроса. При [`external_table_strict_query = 1`](/ru/operations/settings/settings#external_table_strict_query) внешний фильтр, который нельзя перенести, отклоняется с исключением вместо локального применения.
:::

<div id="data-types-support">
  ## Поддержка типов данных
</div>

Если в определении таблицы вы явно указываете типы столбцов ClickHouse, из столбцов SQLite типа TEXT можно разобрать следующие типы ClickHouse:

* [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* Все целочисленные типы ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

Сопоставление типов по умолчанию см. в разделе [SQLite database engine](../../../engines/database-engines/sqlite.md#data_types-support).

<div id="usage-example">
  ## Пример использования
</div>

Показан запрос на создание таблицы SQLite:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

Возвращает данные из таблицы:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**См. также**

* движок [SQLite](../../../engines/database-engines/sqlite.md)
* табличная функция [sqlite](../../../sql-reference/table-functions/sqlite.md)