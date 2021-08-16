---
toc_priority: 38
toc_title: "Словарь"
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
LIFETIME({MIN min_val MAX max_val | max_val})
```

Создаёт [внешний словарь](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) с заданной [структурой](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [источником](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [способом размещения в памяти](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) и [периодом обновления](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

Структура внешнего словаря состоит из атрибутов. Атрибуты словаря задаются как столбцы таблицы. Единственным обязательным свойством атрибута является его тип, все остальные свойства могут иметь значения по умолчанию.

В зависимости от [способа размещения словаря в памяти](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md), ключами словаря могут быть один и более атрибутов.

Смотрите [Внешние словари](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

<!--hide-->