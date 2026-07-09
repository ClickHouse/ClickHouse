---
slug: /sql-reference/statements/create/dictionary/sources/clickhouse
title: 'مصدر القاموس في ClickHouse'
sidebar_position: 8
sidebar_label: 'ClickHouse'
description: 'تهيئة جدول في ClickHouse كمصدر للقاموس.'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CLICKHOUSE(
        host 'example01-01-1'
        port 9000
        user 'default'
        password ''
        db 'default'
        table 'ids'
        where 'id=10'
        secure 1
        query 'SELECT id, value_1, value_2 FROM default.ids'
    ));
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التكوين">
    ```xml
    <source>
        <clickhouse>
            <host>example01-01-1</host>
            <port>9000</port>
            <user>default</user>
            <password></password>
            <db>default</db>
            <table>ids</table>
            <where>id=10</where>
            <secure>1</secure>
            <query>SELECT id, value_1, value_2 FROM default.ids</query>
        </clickhouse>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

حقول الإعدادات:

| الإعداد            | الوصف                                                                                                                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`             | مضيف ClickHouse. إذا كان مضيفًا محليًا، فسيُعالَج الاستعلام من دون أي نشاط على الشبكة. ولتحسين تحمّل الأعطال، يمكنك إنشاء جدول [Distributed](/ar/engines/table-engines/special/distributed) واستخدامه في إعدادات لاحقة. |
| `port`             | المنفذ على خادم ClickHouse.                                                                                                                                                                                          |
| `user`             | اسم مستخدم ClickHouse.                                                                                                                                                                                               |
| `password`         | كلمة مرور مستخدم ClickHouse.                                                                                                                                                                                         |
| `db`               | اسم قاعدة البيانات.                                                                                                                                                                                                  |
| `table`            | اسم الجدول.                                                                                                                                                                                                          |
| `where`            | معايير التصفية. اختياري.                                                                                                                                                                                             |
| `invalidate_query` | استعلام للتحقق من حالة القاموس. اختياري. اقرأ المزيد في قسم [تحديث بيانات القاموس باستخدام LIFETIME](../lifetime.md).                                                                                                |
| `secure`           | استخدم SSL للاتصال.                                                                                                                                                                                                  |
| `query`            | الاستعلام المخصص. اختياري.                                                                                                                                                                                           |

:::note
لا يمكن استخدام الحقل `table` أو الحقل `where` مع الحقل `query`. ويجب التصريح بأحد الحقلين `table` أو `query`.
:::