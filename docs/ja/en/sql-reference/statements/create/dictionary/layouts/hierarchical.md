---
slug: /sql-reference/statements/create/dictionary/layouts/hierarchical
title: '階層型 Dictionary'
sidebar_label: '階層型'
sidebar_position: 10
description: '親子関係のキーを持つ階層型 Dictionary を設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hierarchical-dictionaries">
  ## 階層型 Dictionary
</div>

ClickHouse は、[数値キー](../attributes.md#numeric-key)を持つ階層型 Dictionary をサポートしています。

以下の階層構造を見てください。

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

この階層は、次の Dictionary テーブルとして表すことができます。

| region&#95;id | parent&#95;region | region&#95;name |
| ------------- | ----------------- | --------------- |
| 1             | 0                 | アメリカ合衆国         |
| 2             | 1                 | カリフォルニア         |
| 3             | 2                 | サンフランシスコ        |
| 4             | 0                 | イギリス            |
| 5             | 4                 | ロンドン            |

このテーブルには `parent_region` というカラムがあり、各要素の直接の親のキーが格納されています。

ClickHouse は、外部 Dictionary 属性の hierarchical プロパティをサポートしています。このプロパティを使用すると、上記のような階層型 Dictionary を設定できます。

[dictGetHierarchy](/ja/sql-reference/functions/ext-dict-functions.md#dictGetHierarchy) 関数を使用すると、要素の親チェーンを取得できます。

この例では、Dictionary の structure は次のようになります。

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

  <TabItem value="xml" label="設定ファイル">
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