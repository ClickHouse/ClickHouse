# Hierarchical Dictionaries

ClickHouse supports hierarchical dictionaries with [numeric key](external_dicts_dict_structure.md#ext_dict-numeric-key).

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

This table contains an attribute that equals to the key of a nearest ancestor for the element.

Using the [hierarchical](external_dicts_dict_structure.md#hierarchical-dict-attr) configuration property you can define the hierarchical dictionary of such kind in ClickHouse.

Using the [dictGetHierarchy](../functions/ext_dict_functions.md#dictgethierarchy) function you can get all the ancestors of an element.
