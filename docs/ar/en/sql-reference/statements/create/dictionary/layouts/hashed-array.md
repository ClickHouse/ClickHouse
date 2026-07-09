---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'أنواع تخطيط القاموس: hashed_array'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: 'تخزين قاموس في الذاكرة باستخدام جدول تجزئة مع مصفوفات السمات.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed_array">
  ## hashed_array
</div>

يُخزَّن القاموس بالكامل في الذاكرة. وتُخزَّن كل سمة في مصفوفة. وتُخزَّن سمة المفتاح على هيئة جدول تجزئة، بحيث تكون القيمة فهرسًا في مصفوفة السمات. ويمكن أن يحتوي القاموس على أي عدد من العناصر بأي معرّفات. وعمليًا، يمكن أن يصل عدد المفاتيح إلى عشرات الملايين من العناصر.

يكون مفتاح القاموس من النوع [UInt64](/ar/sql-reference/data-types/int-uint.md).

جميع أنواع المصادر مدعومة. وعند التحديث، تُقرأ البيانات (من ملف أو من جدول) كاملةً.

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <hashed_array>
      </hashed_array>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_hashed_array">
  ## complex_key_hashed_array
</div>

هذا النوع من التخزين مخصص للاستخدام مع [المفاتيح المركبة](../attributes.md#composite-key). وهو مشابه لـ [hashed&#95;array](#hashed_array).

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <complex_key_hashed_array />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />