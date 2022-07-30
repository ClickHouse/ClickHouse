---
sidebar_position: 45
sidebar_label: "Иерархические словари"
---

# Иерархические словари {#ierarkhicheskie-slovari}

ClickHouse поддерживает иерархические словари с [числовыми ключом](external-dicts-dict-structure.md#ext_dict-numeric-key).

Рассмотрим следующую структуру:

``` text
0 (Common  parent)
│
├── 1 (Russia)
│   │
│   └── 2 (Moscow)
│       │
│       └── 3 (Center)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

Эту иерархию можно выразить в виде следующей таблицы-словаря.

| region_id | parent_region | region_name  |
|------------|----------------|---------------|
| 1          | 0              | Russia        |
| 2          | 1              | Moscow        |
| 3          | 2              | Center        |
| 4          | 0              | Great Britain |
| 5          | 4              | London        |

Таблица содержит столбец `parent_region`, содержащий ключ ближайшего предка для текущего элемента.

ClickHouse поддерживает свойство [hierarchical](external-dicts-dict-structure.md#hierarchical-dict-attr) для атрибутов [внешнего словаря](./). Это свойство позволяет конфигурировать словари, подобные описанному выше.

С помощью функции [dictGetHierarchy](../../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy) можно получить цепочку предков элемента.

Структура словаря для нашего примера может выглядеть следующим образом:

``` xml
<dictionary>
    <structure>
        <id>
            <name>region_id</name>
        </id>

        <attribute>
            <name>parent_region</name>
            <type>UInt64</type>
            <null_value>0</null_value>
            <hierarchical>true</hierarchical>
        </attribute>

        <attribute>
            <name>region_name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

    </structure>
</dictionary>
```

