---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'مصدر القاموس HTTP(S)'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: 'هيّئ نقطة نهاية HTTP أو HTTPS كمصدر قاموس في ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

يعتمد العمل مع خادم HTTP(S) على [كيفية تخزين القاموس في الذاكرة](../layouts/). إذا كان القاموس مخزّنًا باستخدام `cache` و`complex_key_cache`، فإن ClickHouse يطلب المفاتيح المطلوبة عبر إرسال طلب باستخدام الطريقة `POST`.

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف الإعدادات">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

لكي يتمكن ClickHouse من الوصول إلى مورد عبر HTTPS، يجب [تهيئة openSSL](/ar/operations/server-configuration-parameters/settings#openssl) في إعدادات الخادم.

حقول الإعدادات:

| Setting       | Description                                                                      |
| ------------- | -------------------------------------------------------------------------------- |
| `url`         | عنوان URL للمصدر.                                                                |
| `format`      | تنسيق الملف. جميع التنسيقات الموضحة في [Formats](/ar/sql-reference/formats) مدعومة. |
| `credentials` | مصادقة HTTP الأساسية. اختياري.                                                   |
| `user`        | اسم المستخدم المطلوب للمصادقة.                                                   |
| `password`    | كلمة المرور المطلوبة للمصادقة.                                                   |
| `headers`     | جميع رؤوس HTTP المخصصة المستخدمة في طلب HTTP. اختياري.                           |
| `header`      | رأس HTTP واحد.                                                                   |
| `name`        | اسم المعرّف المستخدم للرأس المُرسل في الطلب.                                     |
| `value`       | القيمة المعيّنة لاسم معرّف محدد.                                                 |

عند إنشاء قاموس باستخدام أمر DDL (`CREATE DICTIONARY ...`)، يتم التحقق من المضيفين البعيدين لقواميس HTTP مقارنةً بمحتويات القسم `remote_url_allow_hosts` في ملف `config`، وذلك لمنع مستخدمي قاعدة البيانات من الوصول إلى أي خادم HTTP بشكل عشوائي.