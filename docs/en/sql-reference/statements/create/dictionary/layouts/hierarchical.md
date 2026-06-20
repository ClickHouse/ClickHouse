---
slug: /sql-reference/statements/create/dictionary/layouts/hierarchical
title: 'Hierarchical dictionaries'
sidebar_label: 'Hierarchical'
sidebar_position: 10
description: 'Configure hierarchical dictionaries with parent-child key relationships.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Hierarchical dictionaries {#hierarchical-dictionaries}

ClickHouse supports hierarchical dictionaries with a [numeric key](../keys-and-fields.md#numeric-key).

Look at the following hierarchical structure:

```text
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

ClickHouse supports the hierarchical property for external dictionary attributes. This property allows you to configure the hierarchical dictionary similar to described above.

The [dictGetHierarchy](../../../functions/ext-dict-functions.md#dictGetHierarchy) function allows you to get the parent chain of an element.

For our example, the structure of the dictionary can be the following:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
CREATE DICTIONARY regions_dict
(
    region_id UInt64,
    parent_region UInt64 DEFAULT 0 HIERARCHICAL,
    region_name String DEFAULT ''
)
PRIMARY KEY region_id
SOURCE(...)
LAYOUT(HASHED())
LIFETIME(3600);
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
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

</TabItem>
</Tabs>
<br/>
