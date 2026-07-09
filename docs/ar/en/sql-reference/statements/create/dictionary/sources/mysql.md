---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'مصدر قاموس MySQL'
sidebar_position: 7
sidebar_label: 'MySQL'
description: 'هيّئ MySQL كمصدر للقاموس في ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        port 3306
        user 'clickhouse'
        password 'qwerty'
        replica(host 'example01-1' priority 1)
        replica(host 'example01-2' priority 1)
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
        enable_compression 1
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <source>
      <mysql>
          <port>3306</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <replica>
              <host>example01-1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>example01-2</host>
              <priority>1</priority>
          </replica>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
          <enable_compression>1</enable_compression>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

حقول الإعدادات:

| الإعداد                   | الوصف                                                                                                                                                                                                                                                                                                       |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `port`                    | المنفذ على MySQL server. يمكنك تحديده لجميع النسخ المتماثلة، أو لكل نسخة على حدة (داخل `<replica>`).                                                                                                                                                                                                        |
| `user`                    | اسم MySQL user. يمكنك تحديده لجميع النسخ المتماثلة، أو لكل نسخة على حدة (داخل `<replica>`).                                                                                                                                                                                                                 |
| `password`                | كلمة مرور MySQL user. يمكنك تحديدها لجميع النسخ المتماثلة، أو لكل نسخة على حدة (داخل `<replica>`).                                                                                                                                                                                                          |
| `replica`                 | قسم تهيئة النسخ المتماثلة. يمكن أن يوجد أكثر من قسم.                                                                                                                                                                                                                                                        |
| `replica/host`            | مضيف MySQL.                                                                                                                                                                                                                                                                                                 |
| `replica/priority`        | أولوية النسخة المتماثلة. عند محاولة الاتصال، يستعرض ClickHouse النسخ المتماثلة حسب ترتيب الأولوية. وكلما كان الرقم أصغر، كانت الأولوية أعلى.                                                                                                                                                                |
| `db`                      | اسم قاعدة البيانات.                                                                                                                                                                                                                                                                                         |
| `table`                   | اسم الجدول.                                                                                                                                                                                                                                                                                                 |
| `where`                   | معايير التحديد. صياغة الشروط هي نفسها المستخدمة في عبارة `WHERE` في MySQL، على سبيل المثال: `id > 10 AND id < 20`. هذا الحقل اختياري.                                                                                                                                                                       |
| `invalidate_query`        | استعلام للتحقق من حالة القاموس. هذا الحقل اختياري. اقرأ المزيد في قسم [Refreshing dictionary data using LIFETIME](../lifetime.md).                                                                                                                                                                          |
| `fail_on_connection_loss` | يتحكم في سلوك server عند فقدان الاتصال. إذا كانت القيمة `true`، فسيتم طرح استثناء فورًا عند فقدان الاتصال بين client وserver. وإذا كانت `false`، فسيعيد server محاولة جلب البيانات ثلاث مرات على الأقل قبل الإبلاغ عن خطأ. لاحظ أن إعادة المحاولة تؤدي إلى زيادة زمن الاستجابة. القيمة الافتراضية: `false`. |
| `query`                   | الاستعلام المخصص. هذا الحقل اختياري.                                                                                                                                                                                                                                                                        |
| `enable_compression`      | يفعّل ضغط zlib لاتصال MySQL protocol. عند ضبطه على `1`، يطلب ClickHouse ضغطًا على مستوى البروتوكول من MySQL server. ويمكن أيضًا ضبطه لكل نسخة متماثلة داخل `<replica>`. القيمة الافتراضية: `0`.                                                                                                             |

:::note
لا يمكن استخدام الحقلين `table` أو `where` مع الحقل `query` في الوقت نفسه. ويجب التصريح بأحد الحقلين `table` أو `query`.
:::

:::note
لا توجد parameter صريحة باسم `secure`. وعند إنشاء اتصال SSL، يكون الأمان إلزاميًا.
:::

يمكن الاتصال بـ MySQL على المضيف المحلي عبر sockets. للقيام بذلك، اضبط `host` و`socket`.

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        host 'localhost'
        socket '/path/to/socket/file.sock'
        user 'clickhouse'
        password 'qwerty'
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <source>
      <mysql>
          <host>localhost</host>
          <socket>/path/to/socket/file.sock</socket>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>