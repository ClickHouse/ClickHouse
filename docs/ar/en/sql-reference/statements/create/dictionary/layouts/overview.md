---
description: 'أنواع تخطيط القواميس لتخزينها في الذاكرة'
sidebar_label: 'نظرة عامة'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: 'أنماط تخطيط القواميس'
doc_type: 'مرجع'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## أنواع تخطيط القواميس
</div>

توجد عدة طرق لتخزين القواميس في الذاكرة، ولكل منها مقايضات بين استخدام CPU وRAM.

| التخطيط                                                                                                    | الوصف                                                                                                                                               |
| ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| [flat](./flat.md)                                                                                          | يخزّن البيانات في مصفوفات مسطحة مفهرسة حسب المفتاح. وهو أسرع تخطيط، لكن يجب أن تكون المفاتيح من النوع `UInt64` وأن تكون مقيّدة بـ `max_array_size`. |
| [hashed](./hashed.md)                                                                                      | يخزّن البيانات في جدول تجزئة. لا يوجد حد لحجم المفتاح، ويدعم أي عدد من العناصر.                                                                     |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | مثل `hashed`، لكنه يبادل زيادة استخدام CPU مقابل تقليل استخدام الذاكرة.                                                                             |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | مثل `hashed`، للمفاتيح المركبة.                                                                                                                     |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | مثل `sparse_hashed`، للمفاتيح المركبة.                                                                                                              |
| [hashed&#95;array](./hashed-array.md)                                                                      | تُخزَّن السمات في مصفوفات مع جدول تجزئة يربط المفاتيح بفهارس المصفوفات. وهو فعّال من حيث الذاكرة عند وجود عدد كبير من السمات.                       |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | مثل `hashed_array`، للمفاتيح المركبة.                                                                                                               |
| [range&#95;hashed](./range-hashed.md)                                                                      | جدول تجزئة مع نطاقات مرتبة. يدعم عمليات البحث حسب المفتاح + نطاق التاريخ/الوقت.                                                                     |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | مثل `range_hashed`، للمفاتيح المركبة.                                                                                                               |
| [cache](./cache.md)                                                                                        | ذاكرة تخزين مؤقت داخل الذاكرة وبحجم ثابت. لا تُخزَّن إلا المفاتيح التي يُكثَر الوصول إليها.                                                         |
| [complex&#95;key&#95;cache](/ar/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | مثل `cache`، للمفاتيح المركبة.                                                                                                                      |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | مثل `cache`، لكنه يخزّن البيانات على SSD مع فهرس داخل الذاكرة.                                                                                      |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | مثل `ssd_cache`، للمفاتيح المركبة.                                                                                                                  |
| [direct](./direct.md)                                                                                      | بدون تخزين داخل الذاكرة — يستعلم المصدر مباشرةً لكل طلب.                                                                                            |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | مثل `direct`، للمفاتيح المركبة.                                                                                                                     |
| [ip&#95;trie](./ip-trie.md)                                                                                | بنية Trie لعمليات البحث السريعة عن بادئات IP (المعتمدة على CIDR).                                                                                   |

:::tip التخطيطات الموصى بها
توفّر [flat](./flat.md) و[hashed](./hashed.md) و[complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed) أفضل أداء للاستعلامات.
لا يُنصح بتخطيطات التخزين المؤقت بسبب احتمال ضعف الأداء وصعوبة ضبط المعلمات — راجع [cache](./cache.md) للتفاصيل.
:::

<div id="specify-dictionary-layout">
  ## حدِّد تخطيط القاموس
</div>

<CloudDetails />

يمكنك ضبط تخطيط القاموس باستخدام العبارة `LAYOUT` ‏(في DDL) أو الإعداد `layout` في تعريفات ملف التهيئة.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- إعدادات التخطيط
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- إعدادات التخطيط -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

راجع أيضًا [CREATE DICTIONARY](../overview.md) للاطلاع على بناء جملة DDL الكامل.

القواميس التي لا تتضمن العبارة `complex-key*` في التخطيط يكون مفتاحها من النوع [UInt64](/ar/sql-reference/data-types/int-uint.md)، أما قواميس `complex-key*` فلها مفتاح مركّب (أي مفتاح معقّد بأنواع اعتباطية).

**مثال على مفتاح رقمي** (العمود key&#95;column من النوع [UInt64](/ar/sql-reference/data-types/int-uint.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**مثال على مفتاح مركّب** (يحتوي المفتاح على عنصر واحد من النوع [String](/ar/sql-reference/data-types/string.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## تحسين أداء القاموس
</div>

هناك عدة طرق لتحسين أداء القاموس:

* استدعِ الدالة التي تعمل مع القاموس بعد `GROUP BY`.
* علِّم السمات المراد استخراجها على أنها حقنية.
  تُعدّ السمة حقنية إذا كانت المفاتيح المختلفة تقابلها قيم سمات مختلفة.
  لذلك، عندما يستخدم `GROUP BY` دالةً تجلب قيمة سمة حسب المفتاح، تُزال هذه الدالة تلقائيًا من `GROUP BY`.

يُصدر ClickHouse استثناءً عند وقوع أخطاء متعلقة بالقواميس.
ومن أمثلة هذه الأخطاء:

* تعذّر تحميل القاموس الجاري الوصول إليه.
* حدوث خطأ عند الاستعلام عن قاموس `cached`.

يمكنك عرض قائمة القواميس وحالاتها في جدول [system.dictionaries](/ar/operations/system-tables/dictionaries.md).