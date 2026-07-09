---
slug: /sql-reference/statements/create/dictionary/sources/cassandra
title: 'مصدر القاموس: Cassandra'
sidebar_position: 11
sidebar_label: 'Cassandra'
description: 'تهيئة Cassandra كمصدر للقاموس في ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CASSANDRA(
        host 'localhost'
        port 9042
        user 'username'
        password 'qwerty123'
        keyspace 'database_name'
        column_family 'table_name'
        allow_filtering 1
        partition_key_prefix 1
        consistency 'One'
        where '"SomeColumn" = 42'
        max_threads 8
        query 'SELECT id, value_1, value_2 FROM database_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف الإعدادات">
    ```xml
    <source>
        <cassandra>
            <host>localhost</host>
            <port>9042</port>
            <user>username</user>
            <password>qwerty123</password>
            <keyspase>database_name</keyspase>
            <column_family>table_name</column_family>
            <allow_filtering>1</allow_filtering>
            <partition_key_prefix>1</partition_key_prefix>
            <consistency>One</consistency>
            <where>"SomeColumn" = 42</where>
            <max_threads>8</max_threads>
            <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
        </cassandra>
    </source>
    ```
  </TabItem>
</Tabs>

حقول الإعدادات:

| الإعداد                | الوصف                                                                                                                                                                                                                                                                                |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`                 | مضيف Cassandra أو قائمة بالمضيفين مفصولة بفواصل.                                                                                                                                                                                                                                     |
| `port`                 | المنفذ على خوادم Cassandra. إذا لم يتم تحديده، فسيُستخدم المنفذ الافتراضي `9042`.                                                                                                                                                                                                    |
| `user`                 | اسم مستخدم Cassandra.                                                                                                                                                                                                                                                                |
| `password`             | كلمة مرور مستخدم Cassandra.                                                                                                                                                                                                                                                          |
| `keyspace`             | اسم حيّز المفاتيح (قاعدة البيانات).                                                                                                                                                                                                                                                  |
| `column_family`        | اسم عائلة الأعمدة (الجدول).                                                                                                                                                                                                                                                          |
| `allow_filtering`      | خيار للسماح بالشروط التي قد تكون مكلفة على أعمدة مفتاح التجميع أو منعها. القيمة الافتراضية هي `1`.                                                                                                                                                                                   |
| `partition_key_prefix` | عدد أعمدة مفتاح التقسيم في المفتاح الأساسي لجدول Cassandra. وهو مطلوب لقواميس المفاتيح المركبة. يجب أن يكون ترتيب أعمدة المفتاح في تعريف القاموس مطابقًا لترتيبها في Cassandra. القيمة الافتراضية هي `1` (عمود المفتاح الأول هو مفتاح تقسيم، وأعمدة المفتاح الأخرى هي مفاتيح تجميع). |
| `consistency`          | مستوى الاتساق. القيم الممكنة: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. القيمة الافتراضية هي `One`.                                                                                                                  |
| `where`                | معايير اختيارية للتصفية.                                                                                                                                                                                                                                                             |
| `max_threads`          | الحد الأقصى لعدد سلاسل التنفيذ المستخدمة لتحميل البيانات من عدة partitions في قواميس المفاتيح المركبة.                                                                                                                                                                               |
| `query`                | الاستعلام المخصص. اختياري.                                                                                                                                                                                                                                                           |

:::note
لا يمكن استخدام أيٍّ من الحقلين `column_family` و`where` مع الحقل `query` في الوقت نفسه. ويجب التصريح بأحد الحقلين `column_family` أو `query`.
:::