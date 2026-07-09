---
slug: /sql-reference/statements/create/dictionary/sources/redis
title: 'مصدر قاموس Redis'
sidebar_position: 10
sidebar_label: 'Redis'
description: 'تهيئة Redis كمصدر قاموس في ClickHouse.'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(REDIS(
        host 'localhost'
        port 6379
        storage_type 'simple'
        db_index 0
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف الإعدادات">
    ```xml
    <source>
        <redis>
            <host>localhost</host>
            <port>6379</port>
            <storage_type>simple</storage_type>
            <db_index>0</db_index>
        </redis>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

حقول الإعدادات:

| الإعداد        | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`         | مضيف Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `port`         | المنفذ على خادم Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `storage_type` | بنية تخزين Redis الداخلية المستخدمة للتعامل مع المفاتيح. يستخدم `simple` خريطة مفتاح-قيمة مسطحة، ويدعم تخطيطات المفاتيح البسيطة، بالإضافة إلى تخطيطات المفاتيح المركبة أحادية العمود (مثل `complex_key_cache` و`complex_key_direct`). ويستخدم `hash_map` بنية hash في Redis، وهو مطلوب للمفاتيح المركبة متعددة الحقول؛ كما يتوقع عمودي مفتاح بالضبط. يجب أن تكون أعمدة المفتاح من النوع الصحيح أو النصي. التخطيطات النطاقية غير مدعومة. القيمة الافتراضية هي `simple`. اختياري. |
| `db_index`     | الفهرس الرقمي المحدد لقاعدة البيانات المنطقية في Redis. القيمة الافتراضية هي `0`. اختياري.                                                                                                                                                                                                                                                                                                                                                                                      |