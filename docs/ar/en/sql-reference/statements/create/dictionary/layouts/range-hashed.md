---
slug: /sql-reference/statements/create/dictionary/layouts/range-hashed
title: 'أنواع تخطيط القاموس من نوع range_hashed'
sidebar_label: 'range_hashed'
sidebar_position: 5
description: 'تخزين قاموس في الذاكرة باستخدام جدول تجزئة مع نطاقات تاريخ/وقت مرتبة.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="range_hashed">
  ## range_hashed
</div>

يُخزَّن القاموس في الذاكرة على هيئة جدول تجزئة مع مصفوفة مرتبة من النطاقات والقيم المقابلة لها.

تعمل طريقة التخزين هذه بالطريقة نفسها مثل hashed، وتتيح استخدام نطاقات التاريخ/الوقت (أي نوع رقمي) بالإضافة إلى المفتاح.

مثال: يحتوي الجدول على خصومات لكل مُعلِن بالتنسيق التالي:

```text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

لاستخدام عيّنة مع نطاقات التاريخ، عرّف العنصرين `range_min` و`range_max` في [البنية](../attributes.md#composite-key). يجب أن يحتوي هذان العنصران على `name` و`type` (إذا لم يتم تحديد `type`، فسيُستخدم النوع default - Date). ويمكن أن يكون `type` أيًّا من الأنواع التالية: Date / DateTime / UInt64 / Int32 / وغيرها.

:::note
يجب أن تتوافق قيمتا `range_min` و`range_max` مع النوع `Int64`.
:::

مثال:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY discounts_dict (
        advertiser_id UInt64,
        discount_start_date Date,
        discount_end_date Date,
        amount Float64
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'discounts'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
    RANGE(MIN discount_start_date MAX discount_end_date)
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
        <range_hashed>
            <!-- الاستراتيجية الخاصة بالنطاقات المتداخلة (min/max). القيمة الافتراضية: min (إرجاع نطاق مطابق بالقيمة min(range_min -> range_max)) -->
            <range_lookup_strategy>min</range_lookup_strategy>
        </range_hashed>
    </layout>
    <structure>
        <id>
            <name>advertiser_id</name>
        </id>
        <range_min>
            <name>discount_start_date</name>
            <type>Date</type>
        </range_min>
        <range_max>
            <name>discount_end_date</name>
            <type>Date</type>
        </range_max>
        ...
    ```
  </TabItem>
</Tabs>

<br />

للعمل مع هذه القواميس، تحتاج إلى تمرير مُعامل إضافي إلى الدالة `dictGet`، يُختار النطاق على أساسه:

```sql
dictGet('dict_name', 'attr_name', id, date)
```

مثال على استعلام:

```sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

تعيد هذه الدالة القيمة للمعرّفات `id` المحددة ولنطاق التاريخ الذي يشمل التاريخ المُمرَّر.

تفاصيل الخوارزمية:

* إذا لم يتم العثور على `id` أو لم يتم العثور على نطاق له، فستُعيد القيمة الافتراضية لنوع السمة.
* إذا كانت هناك نطاقات متداخلة وكان `range_lookup_strategy=min`، فستُعيد نطاقًا مطابقًا له أصغر قيمة `range_min`، وإذا عُثر على عدة نطاقات، فستُعيد نطاقًا له أصغر قيمة `range_max`، وإذا عُثر مرة أخرى على عدة نطاقات (أي إذا كان لعدة نطاقات نفس `range_min` و`range_max`) فستُعيد نطاقًا عشوائيًا منها.
* إذا كانت هناك نطاقات متداخلة وكان `range_lookup_strategy=max`، فستُعيد نطاقًا مطابقًا له أكبر قيمة `range_min`، وإذا عُثر على عدة نطاقات، فستُعيد نطاقًا له أكبر قيمة `range_max`، وإذا عُثر مرة أخرى على عدة نطاقات (أي إذا كان لعدة نطاقات نفس `range_min` و`range_max`) فستُعيد نطاقًا عشوائيًا منها.
* إذا كانت `range_max` تساوي `NULL`، فسيكون النطاق مفتوحًا. وتُعامَل `NULL` على أنها أكبر قيمة ممكنة. أمّا بالنسبة إلى `range_min`، فيمكن استخدام `1970-01-01` أو `0` (-MAX&#95;INT) كقيمة مفتوحة.

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY somedict(
        Abcdef UInt64,
        StartTimeStamp UInt64,
        EndTimeStamp UInt64,
        XXXType String DEFAULT ''
    )
    PRIMARY KEY Abcdef
    RANGE(MIN StartTimeStamp MAX EndTimeStamp)
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <clickhouse>
        <dictionary>
            ...

            <layout>
                <range_hashed />
            </layout>

            <structure>
                <id>
                    <name>Abcdef</name>
                </id>
                <range_min>
                    <name>StartTimeStamp</name>
                    <type>UInt64</type>
                </range_min>
                <range_max>
                    <name>EndTimeStamp</name>
                    <type>UInt64</type>
                </range_max>
                <attribute>
                    <name>XXXType</name>
                    <type>String</type>
                    <null_value />
                </attribute>
            </structure>

        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

مثال على التهيئة مع نطاقات متداخلة ونطاقات مفتوحة:

```sql
CREATE TABLE discounts
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
ENGINE = Memory;

INSERT INTO discounts VALUES (1, '2015-01-01', Null, 0.1);
INSERT INTO discounts VALUES (1, '2015-01-15', Null, 0.2);
INSERT INTO discounts VALUES (2, '2015-01-01', '2015-01-15', 0.3);
INSERT INTO discounts VALUES (2, '2015-01-04', '2015-01-10', 0.4);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-15', 0.5);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-10', 0.6);

SELECT * FROM discounts ORDER BY advertiser_id, discount_start_date;
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│             1 │          2015-01-01 │              ᴺᵁᴸᴸ │    0.1 │
│             1 │          2015-01-15 │              ᴺᵁᴸᴸ │    0.2 │
│             2 │          2015-01-01 │        2015-01-15 │    0.3 │
│             2 │          2015-01-04 │        2015-01-10 │    0.4 │
│             3 │          1970-01-01 │        2015-01-15 │    0.5 │
│             3 │          1970-01-01 │        2015-01-10 │    0.6 │
└───────────────┴─────────────────────┴───────────────────┴────────┘

-- RANGE_LOOKUP_STRATEGY 'max'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.2 │ -- two ranges are matching, range_min 2015-01-15 (0.2) is bigger than 2015-01-01 (0.1)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.4 │ -- two ranges are matching, range_min 2015-01-04 (0.4) is bigger than 2015-01-01 (0.3)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.5 │ -- two ranges are matching, range_min are equal, 2015-01-15 (0.5) is bigger than 2015-01-10 (0.6)
└─────┘

DROP DICTIONARY discounts_dict;

-- RANGE_LOOKUP_STRATEGY 'min'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.1 │ -- two ranges are matching, range_min 2015-01-01 (0.1) is less than 2015-01-15 (0.2)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.3 │ -- two ranges are matching, range_min 2015-01-01 (0.3) is less than 2015-01-04 (0.4)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.6 │ -- two ranges are matching, range_min are equal, 2015-01-10 (0.6) is less than 2015-01-15 (0.5)
└─────┘
```

<div id="complex_key_range_hashed">
  ## complex_key_range_hashed
</div>

يُخزَّن القاموس في الذاكرة على هيئة جدول تجزئة، مع مصفوفة مرتبة من النطاقات والقيم المقابلة لها (راجع [range&#95;hashed](#range_hashed)). يُستخدم هذا النوع من التخزين مع [المفاتيح المركبة](../attributes.md#composite-key).

مثال على التهيئة:

```sql
CREATE DICTIONARY range_dictionary
(
  CountryID UInt64,
  CountryKey String,
  StartDate Date,
  EndDate Date,
  Tax Float64 DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);
```