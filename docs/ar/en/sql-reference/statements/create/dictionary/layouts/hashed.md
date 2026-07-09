---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'أنواع تخطيط القاموس من نوع hashed'
sidebar_label: 'hashed'
sidebar_position: 3
description: 'تخزين قاموس في الذاكرة باستخدام جداول التجزئة: hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

يُخزَّن القاموس بالكامل في الذاكرة على هيئة جدول تجزئة. ويمكن أن يحتوي على أي عدد من العناصر بأي معرّفات. وعمليًا، قد يصل عدد المفاتيح إلى عشرات الملايين من العناصر.

نوع مفتاح القاموس هو [UInt64](/ar/sql-reference/data-types/int-uint.md).

جميع أنواع المصادر مدعومة. وعند التحديث، تُقرأ البيانات بالكامل (من ملف أو من جدول).

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

مثال على التهيئة مع الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <hashed>
        <!-- إذا كانت قيمة shards أكبر من 1 (القيمة الافتراضية هي `1`)، فسيحمّل القاموس
             البيانات بالتوازي، وهذا مفيد إذا كان لديك عدد كبير جدًا من العناصر في
             قاموس واحد. -->
        <shards>10</shards>

        <!-- حجم التراكم لوحدات blocks في قائمة الانتظار المتوازية.

             نظرًا لأن عنق الزجاجة في التحميل المتوازي هو إعادة التجزئة، فمن أجل تجنب
             التوقف بسبب انشغال أحد الـ thread بإعادة التجزئة، تحتاج إلى وجود
             قدر من التراكم.

             تمثل 10000 توازنًا جيدًا بين الذاكرة والسرعة.
             وحتى مع 10e10 عنصرًا، يمكنها استيعاب الحمل بالكامل دون تجويع. -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- الحد الأقصى لمعامل التحميل لجدول التجزئة. عند استخدام قيم أكبر،
             تُستغل الذاكرة بكفاءة أعلى (أي يُهدر قدر أقل من الذاكرة)، لكن قد
             يتراجع أداء القراءة/الأداء.

             القيم الصالحة: [0.5, 0.99]
             القيمة الافتراضية: 0.5 -->
        <max_load_factor>0.5</max_load_factor>
      </hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="sparse_hashed">
  ## sparse_hashed
</div>

مشابه لـ `hashed`، لكنه يستخدم ذاكرة أقل مقابل استخدام أكبر لـ CPU.

مفتاح القاموس من النوع [UInt64](/ar/sql-reference/data-types/int-uint.md).

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

يمكن أيضًا استخدام `shards` مع هذا النوع من القواميس، وهو أكثر أهمية لـ `sparse_hashed` منه لـ `hashed` لأن `sparse_hashed` أبطأ.

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

هذا النوع من التخزين مخصّص للاستخدام مع [المفاتيح المركبة](../attributes.md#composite-key). وهو مشابه لـ `hashed`.

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <complex_key_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_sparse_hashed">
  ## complex_key_sparse_hashed
</div>

هذا النوع من التخزين مخصّص للاستخدام مع [المفاتيح المركبة](../attributes.md#composite-key). وهو مشابه لـ [sparse&#95;hashed](#sparse_hashed).

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <complex_key_sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />