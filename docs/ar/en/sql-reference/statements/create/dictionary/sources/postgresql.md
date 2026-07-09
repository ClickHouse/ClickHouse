---
slug: /sql-reference/statements/create/dictionary/sources/postgresql
title: 'مصدر قاموس PostgreSQL'
sidebar_position: 12
sidebar_label: 'PostgreSQL'
description: 'إعداد PostgreSQL كمصدر قاموس في ClickHouse.'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(POSTGRESQL(
        port 5432
        host 'postgresql-hostname'
        user 'postgres_user'
        password 'postgres_password'
        db 'db_name'
        table 'table_name'
        replica(host 'example01-1' port 5432 priority 1)
        replica(host 'example01-2' port 5432 priority 2)
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف الإعدادات">
    ```xml
    <source>
      <postgresql>
          <host>postgresql-hostname</hoat>
          <port>5432</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </postgresql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

حقول الإعدادات:

| Setting                | Description                                                                                                                                     |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                 | اسم المضيف على خادم PostgreSQL. يمكنك تحديده لجميع النسخ المتماثلة أو لكل نسخة على حدة (داخل `<replica>`).                                      |
| `port`                 | المنفذ على خادم PostgreSQL. يمكنك تحديده لجميع النسخ المتماثلة أو لكل نسخة على حدة (داخل `<replica>`).                                          |
| `user`                 | اسم مستخدم PostgreSQL. يمكنك تحديده لجميع النسخ المتماثلة أو لكل نسخة على حدة (داخل `<replica>`).                                               |
| `password`             | كلمة مرور مستخدم PostgreSQL. يمكنك تحديدها لجميع النسخ المتماثلة أو لكل نسخة على حدة (داخل `<replica>`).                                        |
| `replica`              | قسم إعدادات النسخ المتماثلة. ويمكن أن يوجد أكثر من قسم واحد.                                                                                    |
| `replica/host`         | مضيف PostgreSQL.                                                                                                                                |
| `replica/port`         | منفذ PostgreSQL.                                                                                                                                |
| `replica/priority`     | أولوية النسخة المتماثلة. عند محاولة الاتصال، يتنقل ClickHouse بين النسخ المتماثلة حسب ترتيب الأولوية. وكلما كان الرقم أصغر، كانت الأولوية أعلى. |
| `db`                   | اسم قاعدة البيانات.                                                                                                                             |
| `table`                | اسم الجدول.                                                                                                                                     |
| `where`                | معايير التصفية. صياغة الشروط هي نفسها كما في عبارة `WHERE` في PostgreSQL. على سبيل المثال: `id > 10 AND id < 20`. هذا الحقل اختياري.            |
| `invalidate_query`     | استعلام للتحقق من حالة القاموس. هذا الحقل اختياري. اقرأ المزيد في قسم [تحديث بيانات القاموس باستخدام LIFETIME](../lifetime.md).                 |
| `background_reconnect` | إعادة الاتصال بالنسخة المتماثلة في الخلفية إذا فشل الاتصال. هذا الحقل اختياري.                                                                  |
| `query`                | الاستعلام المخصص. هذا الحقل اختياري.                                                                                                            |

:::note
لا يمكن استخدام الحقلين `table` أو `where` مع الحقل `query` في الوقت نفسه. ويجب تعريف أحد الحقلين `table` أو `query`.
:::