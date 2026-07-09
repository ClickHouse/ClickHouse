---
description: 'يتيح هذا المحرك الاستعلام عن مجموعات البيانات البعيدة وإدراج البيانات فيها عبر بروتوكول Apache Arrow Flight.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'محرك جدول ArrowFlight'
doc_type: 'مرجع'
---

يُمكّن محرك جدول ArrowFlight نظام ClickHouse من القراءة من مجموعات البيانات البعيدة والكتابة إليها عبر بروتوكول [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).
ويتيح هذا التكامل لـ ClickHouse التفاعل مع خوادم خارجية تدعم Flight باستخدام تنسيق Arrow العمودي وبأداء عالٍ.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**معلمات المحرك**

* `host:port` — عنوان خادم Arrow Flight البعيد. إذا لم يُحدَّد المنفذ، فسيُستخدم المنفذ الافتراضي `8815`. [String](../../../sql-reference/data-types/string.md).
* `dataset_name` — معرّف مجموعة البيانات على خادم Flight (يُستخدم كواصف PATH أو ضمن استعلام `SELECT *`، وذلك بحسب الإعداد `arrow_flight_request_descriptor_type`). [String](../../../sql-reference/data-types/string.md).
* `username` — اسم المستخدم للمصادقة الأساسية عبر HTTP. [String](../../../sql-reference/data-types/string.md).
* `password` — كلمة المرور للمصادقة الأساسية عبر HTTP. [String](../../../sql-reference/data-types/string.md).

إذا لم يتم تحديد `username` و`password`، فلن يُستخدم الاستيثاق (ولا يعمل ذلك إلا إذا كان خادم Arrow Flight يسمح بالوصول من دون استيثاق).

قائمة الأعمدة اختيارية — وإذا لم يتم تحديدها، فسيُستدل على المخطط من خادم Arrow Flight البعيد عبر `GetSchema`.

<div id="named-collections">
  ## المجموعات المسماة
</div>

يدعم المحرّك [المجموعات المسماة](/ar/operations/named-collections) لتخزين معلمات الاتصال:

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

معلمات المجموعة المُسمّاة:

| المعلمة                    | مطلوب                    | الافتراضي | الوصف                                       |
| -------------------------- | ------------------------ | --------- | ------------------------------------------- |
| `host` or `hostname`       | لا                       | `""`      | اسم مضيف الخادم.                            |
| `port`                     | نعم                      | —         | منفذ الخادم.                                |
| `dataset`                  | لا                       | `""`      | اسم مجموعة البيانات أو الواصف.              |
| `use_basic_authentication` | لا                       | `true`    | تفعيل المصادقة الأساسية.                    |
| `user` or `username`       | إذا كانت المصادقة مفعّلة | —         | اسم المستخدم للمصادقة.                      |
| `password`                 | لا                       | `""`      | كلمة المرور للمصادقة.                       |
| `enable_ssl`               | لا                       | `false`   | تفعيل تشفير TLS.                            |
| `ssl_ca`                   | لا                       | `""`      | مسار ملف شهادة CA للتحقق من TLS.            |
| `ssl_override_hostname`    | لا                       | `""`      | تجاوز اسم المضيف المستخدم في التحقق من TLS. |

<div id="settings">
  ## الإعدادات
</div>

* `arrow_flight_request_descriptor_type` — يحدد كيفية إرسال اسم مجموعة البيانات إلى خادم Flight. القيم المحتملة: `path` (الافتراضي، يُرسل كـ واصف PATH) أو `command` (يُرسل كـ واصف CMD مع `SELECT * FROM <dataset>`). استخدم `command` مع خوادم Flight التي تتوقع أوامر SQL (مثل Dremio).

<div id="usage-example">
  ## مثال على الاستخدام
</div>

قراءة البيانات من خادم Arrow Flight بعيد:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

إدراج البيانات في خادم Arrow Flight عن بُعد:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

<div id="notes">
  ## ملاحظات
</div>

* إذا جرى تحديد الأعمدة في عبارة `CREATE TABLE`، فيجب أن تطابق المخطط الذي يعيده خادم Flight.
* إذا لم تُحدَّد الأعمدة، فسيُستنتج المخطط تلقائيًا من الخادم البعيد.
* كلٌّ من القراءة (`SELECT`) والكتابة (`INSERT`) مدعومان.
* يتحكم الإعداد `arrow_flight_request_descriptor_type` في ما إذا كان اسم مجموعة البيانات يُرسَل كواصف PATH أو كواصف CMD يغلّف استعلام `SELECT *`.

<div id="see-also">
  ## انظر أيضًا
</div>

* [دالة الجدول arrowFlight](/ar/sql-reference/table-functions/arrowflight)
* [واجهة Arrow Flight](/ar/interfaces/arrowflight)
* [مواصفة Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
* [تنسيق Arrow في ClickHouse](/ar/interfaces/formats/Arrow)