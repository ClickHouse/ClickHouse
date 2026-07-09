---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'أنواع تخطيط قاموس ssd_cache'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: 'تخزين بيانات القاموس على SSD مع فهرس داخل الذاكرة: النوعان ssd_cache أو complex_key_ssd_cache'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="ssd_cache">
  ## ssd_cache
</div>

مشابه لـ `cache`، لكنه يخزّن البيانات على SSD والفهرس في RAM. ويمكن أيضًا تطبيق جميع إعدادات قواميس `cache` المرتبطة بقائمة انتظار التحديث على قواميس `ssd_cache`.

يكون مفتاح القاموس من النوع [UInt64](/ar/sql-reference/data-types/int-uint.md).

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
        PATH '/var/lib/clickhouse/user_files/test_dict'))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التكوين">
    ```xml
    <layout>
        <ssd_cache>
            <!-- حجم كتلة القراءة الأساسية بالبايت. يُوصى بأن يكون مساويًا لحجم صفحة SSD. -->
            <block_size>4096</block_size>
            <!-- الحد الأقصى لحجم ملف cache بالبايت. -->
            <file_size>16777216</file_size>
            <!-- حجم مخزن RAM المؤقت بالبايت لقراءة العناصر من SSD. -->
            <read_buffer_size>131072</read_buffer_size>
            <!-- حجم مخزن RAM المؤقت بالبايت لتجميع العناصر قبل تفريغها إلى SSD. -->
            <write_buffer_size>1048576</write_buffer_size>
            <!-- المسار الذي سيُخزَّن فيه ملف cache. -->
            <path>/var/lib/clickhouse/user_files/test_dict</path>
        </ssd_cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_ssd_cache">
  ## complex_key_ssd_cache
</div>

هذا النوع من التخزين مخصّص للاستخدام مع [المفاتيح المركبة](../attributes.md#composite-key). وهو مشابه لـ `ssd_cache`.