---
description: 'Движок `Merge` (не путать с `MergeTree`) не хранит данные
  самостоятельно, но позволяет одновременно читать из любого количества других таблиц.'
sidebar_label: 'Merge'
sidebar_position: 30
slug: /engines/table-engines/special/merge
title: 'Движок таблицы Merge'
doc_type: 'reference'
---

Движок `Merge` (не путать с `MergeTree`) не хранит данные самостоятельно, но позволяет одновременно читать из любого количества других таблиц.

Чтение автоматически распараллеливается. Запись в таблицу не поддерживается. При чтении используются индексы таблиц, из которых фактически выполняется чтение, если такие индексы существуют.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

<div id="engine-parameters">
  ## Параметры движка
</div>

<div id="db_name">
  ### `db_name`
</div>

`db_name` — Возможные значения:

* имя базы данных,
  * константное выражение, возвращающее строку с именем базы данных, например `currentDatabase()`,
  * `REGEXP(expression)`, где `expression` — регулярное выражение для сопоставления с именами БД.

<div id="tables_regexp">
  ### `tables_regexp`
</div>

`tables_regexp` — регулярное выражение для сопоставления с именами таблиц в указанной БД или указанных БД.

Регулярные выражения — [re2](https://github.com/google/re2) (поддерживает подмножество PCRE), с учетом регистра.
См. примечания об экранировании символов в регулярных выражениях в разделе &quot;match&quot;.

<div id="usage">
  ## Использование
</div>

При выборе таблиц для чтения сама таблица `Merge` не выбирается, даже если она соответствует регулярному выражению. Это сделано, чтобы избежать циклов.
Можно создать две таблицы `Merge`, которые будут бесконечно пытаться читать данные друг друга, но это плохая идея.

Типичный способ использовать движок `Merge` — работать с большим количеством таблиц `TinyLog` как с одной таблицей.

<div id="examples">
  ## Примеры
</div>

**Пример 1**

Рассмотрим две базы данных: `ABC_corporate_site` и `ABC_store`. Таблица `all_visitors` будет содержать идентификаторы из таблиц `visitors` обеих баз данных.

```sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**Пример 2**

Предположим, у вас есть старая таблица `WatchLog_old`, и вы решили изменить схему партиционирования, не перенося данные в новую таблицу `WatchLog_new`, при этом вам нужно видеть данные из обеих таблиц.

```sql
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
ORDER BY (date, UserId, EventType);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
PARTITION BY date
ORDER BY (UserId, EventType)
SETTINGS index_granularity=8192;

INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog AS WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

```text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_table` — Имя таблицы, из которой были прочитаны данные. Тип: [String](../../../sql-reference/data-types/string.md).

  Если применяется фильтр по `_table` (например, `WHERE _table='xyz'`), будут прочитаны только таблицы, удовлетворяющие условию фильтрации.

* `_database` — Содержит имя базы данных, из которой были прочитаны данные. Тип: [String](../../../sql-reference/data-types/string.md).

**См. также**

* [Виртуальные столбцы](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* табличная функция [merge](../../../sql-reference/table-functions/merge.md)