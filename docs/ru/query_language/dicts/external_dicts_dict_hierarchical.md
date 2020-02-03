# Иерархические словари

ClickHouse поддерживает иерархические словари с [числовыми ключом](external_dicts_dict_structure.md#ext_dict-numeric-key).

Рассмотрим следующую структуру:

```text
0 (Common ancestor)
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

Ключ | Ключ предка | Имя
----|--------------|------
1 | 0 | Russia
2 | 1 | Moscow
3 | 2 | Center
4 | 0 | Great Britain
5 | 4 | London

Таблица содержит атрибут, равный ключу ближайшего предка для текущего элемента.

Иерархический словарь можно задать, установив свойств [hierarchical](external_dicts_dict_structure.md#hierarchical-dict-attr) для атрибута, который должен содержать ключ предка.

С помощью функции [dictGetHierarchy](../functions/ext_dict_functions.md#dictgethierarchy) можно получить цепочку предков элемента.
