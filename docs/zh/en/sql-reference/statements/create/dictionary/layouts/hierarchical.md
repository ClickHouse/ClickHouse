---
slug: /sql-reference/statements/create/dictionary/layouts/hierarchical
title: '层级字典'
sidebar_label: '层级'
sidebar_position: 10
description: '配置具有父子键关系的层级字典'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hierarchical-dictionaries">
  ## 层级字典
</div>

ClickHouse 支持带有[数值键](../attributes.md#numeric-key)的层级字典。

如下所示的层级结构：

```text
0 (Common parent)
│
├── 1 (United States of America)
│   │
│   └── 2 (California)
│       │
│       └── 3 (San Francisco)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

这种层级结构可以表示为下方的字典表。

| region&#95;id | parent&#95;region | region&#95;name |
| ------------- | ----------------- | --------------- |
| 1             | 0                 | 美利坚合众国          |
| 2             | 1                 | 加利福尼亚           |
| 3             | 2                 | 旧金山             |
| 4             | 0                 | 大不列颠            |
| 5             | 4                 | 伦敦              |

该表包含一个 `parent_region` 列，用于存储该元素最近父级的键。

ClickHouse 支持外部字典属性的层级属性。借助此属性，你可以像上文所述那样配置层级字典。

[dictGetHierarchy](/zh/sql-reference/functions/ext-dict-functions.md#dictGetHierarchy) 函数可用于获取元素的父级链。

对于本示例，字典的结构可以如下所示：

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

  <TabItem value="xml" label="配置文件">
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

<br />