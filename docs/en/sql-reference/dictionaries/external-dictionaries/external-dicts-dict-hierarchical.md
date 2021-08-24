---
toc_priority: 45
toc_title: Hierarchical dictionaries
---

# Hierarchical Dictionaries {#hierarchical-dictionaries}

ClickHouse supports hierarchical dictionaries with a [numeric key](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict-numeric-key).

Look at the following hierarchical structure:

``` text
0 (Common parent)
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

| region_id | parent_region | region_name  |
|------------|----------------|---------------|
| 1          | 0              | Russia        |
| 2          | 1              | Moscow        |
| 3          | 2              | Center        |
| 4          | 0              | Great Britain |
| 5          | 4              | London        |

This table contains a column `parent_region` that contains the key of the nearest parent for the element.

ClickHouse supports the [hierarchical](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#hierarchical-dict-attr) property for [external dictionary](../../../sql-reference/dictionaries/external-dictionaries/index.md) attributes. This property allows you to configure the hierarchical dictionary similar to described above.

The [dictGetHierarchy](../../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy) function allows you to get the parent chain of an element.

For our example, the structure of dictionary can be the following:

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

