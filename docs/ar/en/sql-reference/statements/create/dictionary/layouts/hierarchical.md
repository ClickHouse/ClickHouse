---
slug: /sql-reference/statements/create/dictionary/layouts/hierarchical
title: 'القواميس الهرمية'
sidebar_label: 'هرمية'
sidebar_position: 10
description: 'اضبط القواميس الهرمية باستخدام علاقات بين المفاتيح الأصلية والفرعية.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hierarchical-dictionaries">
  ## القواميس الهرمية
</div>

يدعم ClickHouse القواميس الهرمية ذات [مفتاح رقمي](../attributes.md#numeric-key).

انظر إلى البنية الهرمية التالية:

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

يمكن تمثيل هذا التسلسل الهرمي بجدول القاموس التالي.

| region&#95;id | parent&#95;region | region&#95;name            |
| ------------- | ----------------- | -------------------------- |
| 1             | 0                 | الولايات المتحدة الأمريكية |
| 2             | 1                 | كاليفورنيا                 |
| 3             | 2                 | سان فرانسيسكو              |
| 4             | 0                 | بريطانيا العظمى            |
| 5             | 4                 | لندن                       |

يحتوي هذا الجدول على عمود `parent_region` يضم مفتاح أقرب عنصر أب للعنصر الحالي.

يدعم ClickHouse الخاصية الهرمية لسمات القواميس الخارجية. وتتيح لك هذه الخاصية تهيئة القاموس الهرمي على نحو مماثل لما هو موضح أعلاه.

تتيح لك الدالة [dictGetHierarchy](/ar/sql-reference/functions/ext-dict-functions.md#dictGetHierarchy) الحصول على سلسلة العناصر الأب لعنصر معيّن.

في مثالنا، يمكن أن تكون بنية القاموس كما يلي:

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

  <TabItem value="xml" label="ملف التهيئة">
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