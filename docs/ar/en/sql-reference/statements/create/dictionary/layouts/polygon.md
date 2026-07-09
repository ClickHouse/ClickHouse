---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: 'قواميس Polygon'
sidebar_label: 'Polygon'
sidebar_position: 12
description: 'تهيئة قواميس Polygon لعمليات lookup من نوع point-in-polygon.'
doc_type: 'مرجع'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

قاموس `polygon` (`POLYGON`) مُحسَّن لاستعلامات النقطة داخل المضلع، وهو في الأساس مخصّص لعمليات بحث `&quot;reverse geocoding&quot;`.
فعند إعطائه إحداثيًا (خط العرض/خط الطول)، يمكنه بكفاءة تحديد المضلع/المنطقة التي تحتوي هذه النقطة (من بين مجموعة كبيرة من المضلعات، مثل حدود الدول أو المناطق).
وهو مناسب جدًا لربط إحداثيات المواقع بالمنطقة التي تقع ضمنها.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="قواميس Polygon في ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

مثال على تهيئة قاموس polygon:

<CloudDetails />

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY polygon_dict_name (
        key Array(Array(Array(Array(Float64)))),
        name String,
        value UInt64
    )
    PRIMARY KEY key
    LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التكوين">
    ```xml
    <dictionary>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>Array(Array(Array(Array(Float64))))</type>
                </attribute>
            </key>

            <attribute>
                <name>name</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>

            <attribute>
                <name>value</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>

        <layout>
            <polygon>
                <store_polygon_key_column>1</store_polygon_key_column>
            </polygon>
        </layout>

        ...
    </dictionary>
    ```
  </TabItem>
</Tabs>

<br />

عند تهيئة قاموس polygon، يجب أن يكون المفتاح بأحد نوعين:

* مضلع بسيط، وهو مصفوفة من النقاط.
* MultiPolygon، وهو مصفوفة من المضلعات. وكل مضلع هو مصفوفة ثنائية الأبعاد من النقاط. والعنصر الأول في هذه المصفوفة هو الحد الخارجي للمضلع، بينما تحدد العناصر اللاحقة المناطق التي يجب استبعادها منه.

يمكن تحديد النقاط كمصفوفة أو كـ tuple من إحداثياتها. وفي التنفيذ الحالي، لا يُدعَم سوى النقاط ثنائية الأبعاد.

يمكن للمستخدم تحميل بياناته الخاصة بأي من التنسيقات التي يدعمها ClickHouse.

تتوفر 3 أنواع من [التخزين داخل الذاكرة](./#storing-dictionaries-in-memory):

| التخطيط              | الوصف                                                                                                                                                                                                                                                                                                                |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POLYGON_SIMPLE`     | تنفيذ بدائي. يُجرى مرور خطي على جميع المضلعات لكل query، مع التحقق من الاحتواء من دون indexes إضافية.                                                                                                                                                                                                                |
| `POLYGON_INDEX_EACH` | يُنشأ index منفصل لكل مضلع، ما يتيح التحقق السريع من الاحتواء في معظم الحالات (وهو مُحسَّن للمناطق الجغرافية). وتُفرَض شبكة على المنطقة، مع تقسيم الخلايا تكراريًا إلى 16 جزءًا متساويًا. ويتوقف التقسيم عندما يصل عمق التكرار إلى `MAX_DEPTH` أو عندما لا تتقاطع الخلية مع أكثر من `MIN_INTERSECTIONS` من المضلعات. |
| `POLYGON_INDEX_CELL` | يُنشئ أيضًا الشبكة الموضحة أعلاه باستخدام الخيارات نفسها. ولكل خلية leaf، يُنشأ index على جميع أجزاء المضلعات التي تقع ضمنها، ما يتيح استجابات سريعة للاستعلامات.                                                                                                                                                    |
| `POLYGON`            | مرادف لـ `POLYGON_INDEX_CELL`.                                                                                                                                                                                                                                                                                       |

تُنفَّذ استعلامات القاموس باستخدام [الدوال](/ar/sql-reference/functions/ext-dict-functions.md) القياسية للعمل مع القواميس.
والفرق المهم هنا هو أن المفاتيح ستكون هي النقاط التي تريد العثور على المضلع الذي يحتويها.

**مثال**

مثال على العمل مع القاموس المعرّف أعلاه:

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

نتيجةً لتنفيذ الأمر الأخير لكل نقطة في جدول &#39;points&#39;، سيُعثر على مضلع بأقل مساحة يضم هذه النقطة، وستُعرَض السمات المطلوبة.

**مثال**

يمكنك قراءة الأعمدة من قواميس Polygon عبر استعلام SELECT، فقط فعِّل `store_polygon_key_column = 1` في تهيئة القاموس أو في استعلام DDL المقابل.

```sql title="Query"
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

```text title="Response"
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```