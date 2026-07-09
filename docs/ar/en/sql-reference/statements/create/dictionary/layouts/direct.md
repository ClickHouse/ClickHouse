---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'التخطيط direct للقاموس'
sidebar_label: 'direct'
sidebar_position: 9
description: 'تخطيط قاموس يستعلم من المصدر مباشرةً من دون تخزين مؤقت.'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

لا يُخزَّن القاموس في الذاكرة، بل يُراجِع المصدر مباشرةً أثناء معالجة الطلب.

يكون نوع مفتاح القاموس هو [UInt64](/ar/sql-reference/data-types/int-uint.md).

جميع أنواع [المصادر](../sources/#dictionary-sources)، باستثناء الملفات المحلية، مدعومة.

مثال على التهيئة:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <layout>
      <direct />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_direct">
  ## complex_key_direct
</div>

هذا النوع من التخزين مخصّص للاستخدام مع [المفاتيح المركّبة](../attributes.md#composite-key). وهو مشابه لـ `direct`.