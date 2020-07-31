---
toc_priority: 4
toc_title: Словарь
---

# CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
```

Создаёт [внешний словарь](../../../sql-reference/statements/create.md) с заданной [структурой](../../sql-reference/statements/create.md), [источником](../../../sql-reference/statements/create.md), [способом размещения в памяти](../../../sql-reference/statements/create.md) и [периодом обновления](../../../sql-reference/statements/create.md).

Структура внешнего словаря состоит из атрибутов. Атрибуты словаря задаются как столбцы таблицы. Единственным обязательным свойством атрибута является его тип, все остальные свойства могут иметь значения по умолчанию.

В зависимости от [способа размещения словаря в памяти](../../../sql-reference/statements/create.md), ключами словаря могут быть один и более атрибутов.

Смотрите [Внешние словари](../../../sql-reference/statements/create.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/create/dictionary) 
<!--hide-->