---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'مصدر قاموس MongoDB'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: 'إعداد MongoDB كمصدر قاموس في ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MONGODB(
        host 'localhost'
        port 27017
        user ''
        password ''
        db 'test'
        collection 'dictionary_source'
        options 'ssl=true'
    ))
    ```

    أو باستخدام عنوان URI:

    ```sql
    SOURCE(MONGODB(
        uri 'mongodb://localhost:27017/clickhouse'
        collection 'dictionary_source'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <source>
        <mongodb>
            <host>localhost</host>
            <port>27017</port>
            <user></user>
            <password></password>
            <db>test</db>
            <collection>dictionary_source</collection>
            <options>ssl=true</options>
        </mongodb>
    </source>
    ```

    أو باستخدام عنوان URI:

    ```xml
    <source>
        <mongodb>
            <uri>mongodb://localhost:27017/test?ssl=true</uri>
            <collection>dictionary_source</collection>
        </mongodb>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

حقول الإعدادات:

| الإعداد      | الوصف                                                                |
| ------------ | -------------------------------------------------------------------- |
| `host`       | اسم المضيف لـ MongoDB.                                               |
| `port`       | المنفذ على خادم MongoDB.                                             |
| `user`       | اسم مستخدم MongoDB.                                                  |
| `password`   | كلمة مرور مستخدم MongoDB.                                            |
| `db`         | اسم قاعدة البيانات.                                                  |
| `collection` | اسم المجموعة.                                                        |
| `options`    | خيارات سلسلة الاتصال الخاصة بـ MongoDB. اختياري.                     |
| `uri`        | عنوان URI لإنشاء الاتصال (بديل عن حقول `host`/`port`/`db` المنفصلة). |

[مزيد من المعلومات حول المحرك](/ar/engines/table-engines/integrations/mongodb)