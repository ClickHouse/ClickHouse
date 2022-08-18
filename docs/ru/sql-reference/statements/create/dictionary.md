---
toc_priority: 38
toc_title: "Словарь"
---

# CREATE DICTIONARY {#create-dictionary-query}

Создаёт [внешний словарь](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) с заданной [структурой](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [источником](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [способом размещения в памяти](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) и [периодом обновления](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

**Синтаксис**

``` sql
CREATE DICTIONARY [OR REPLACE][IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2],
    attr1 type2 [DEFAULT|EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2 [DEFAULT|EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

Структура внешнего словаря состоит из атрибутов. Атрибуты словаря задаются как столбцы таблицы. Единственным обязательным свойством атрибута является его тип, все остальные свойства могут иметь значения по умолчанию.

В зависимости от [способа размещения словаря в памяти](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md), ключами словаря могут быть один и более атрибутов.

Более подробную информацию смотрите в разделе [внешние словари](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Вы можете добавить комментарий к словарю при его создании, используя секцию `COMMENT`.

**Пример**

Входная таблица `source_table`:

``` text
┌─id─┬─value──┐
│  1 │ First  │
│  2 │ Second │
└────┴────────┘
```

Создание словаря:

``` sql
CREATE DICTIONARY dictionary_with_comment
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'The temporary dictionary';
```

Вывод словаря:

``` sql
SHOW CREATE DICTIONARY dictionary_with_comment;
```

```text
┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE DICTIONARY default.dictionary_with_comment
(
    `id` UInt64,
    `value` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(FLAT())
COMMENT 'The temporary dictionary' │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Вывод комментария к словарю:

``` sql
SELECT comment FROM system.dictionaries WHERE name == 'dictionary_with_comment' AND database == currentDatabase();
```

```text
┌─comment──────────────────┐
│ The temporary dictionary │
└──────────────────────────┘
```

**См. также**

-   [system.dictionaries](../../../operations/system-tables/dictionaries.md) — эта таблица содержит информацию о [внешних словарях](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).
