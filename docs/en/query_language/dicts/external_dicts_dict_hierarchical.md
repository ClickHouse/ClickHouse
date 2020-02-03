# Hierarchical Dictionaries

ClickHouse supports hierarchical dictionaries with a [numeric key](external_dicts_dict_structure.md#ext_dict-numeric-key).

Look at the following hierarchical structure:

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

This hierarchy can be expressed as the following dictionary table.

Key | Ancestor key | Name
----|--------------|------
1 | 0 | Russia
2 | 1 | Moscow
3 | 2 | Center
4 | 0 | Great Britain
5 | 4 | London

This table contains an attribute that equals the key of the nearest ancestor for the element.

Using the [hierarchical](external_dicts_dict_structure.md#hierarchical-dict-attr) configuration property, you can define the hierarchical dictionary of such kind in ClickHouse.

The [dictGetHierarchy](../functions/ext_dict_functions.md#dictgethierarchy) function allows you to get all the ancestors of an element.
