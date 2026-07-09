---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'تخطيط القاموس المسطح'
sidebar_label: 'flat'
sidebar_position: 2
description: 'تخزين القاموس في الذاكرة على شكل مصفوفات مسطحة.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

باستخدام تخطيط `flat`، يُخزَّن القاموس بالكامل في الذاكرة على هيئة مصفوفات مسطّحة.
وتتناسب كمية الذاكرة المستخدمة طرديًا مع قيمة أكبر مفتاح (من حيث المساحة المستخدمة).

:::tip
يوفّر هذا النوع من التخطيط أفضل أداء بين جميع الطرق المتاحة لتخزين القاموس.
:::

يكون مفتاح القاموس من النوع [UInt64](/ar/sql-reference/data-types/int-uint.md)، وتكون قيمة المفتاح مقيّدة بـ `max_array_size` (افتراضيًا — 500,000).
إذا تم اكتشاف مفتاح أكبر عند إنشاء القاموس، يطرح ClickHouse استثناء ولا يُنشئ القاموس.
ويتحكم الإعداد `initial_array_size` في الحجم الأولي للمصفوفات المسطّحة الخاصة بالقاموس (افتراضيًا — 1024).

جميع أنواع المصادر مدعومة.
وعند تحديث القاموس، تُقرأ البيانات (من ملف أو من جدول) بالكامل.

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <flat>
        <initial_array_size>50000</initial_array_size>
        <max_array_size>5000000</max_array_size>
      </flat>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />