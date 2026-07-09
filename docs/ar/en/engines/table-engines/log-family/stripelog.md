---
description: 'توثيق محرك الجدول StripeLog'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'محرك الجدول StripeLog'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # محرك الجدول StripeLog
</div>

<CloudNotSupportedBadge />

ينتمي هذا المحرك إلى عائلة محركات السجل. اطّلع على الخصائص المشتركة لمحركات السجل والفروق بينها في مقالة [عائلة محركات Log](../../../engines/table-engines/log-family/index.md).

استخدم هذا المحرك في الحالات التي تحتاج فيها إلى الكتابة إلى عدد كبير من الجداول مع كمية صغيرة من البيانات (أقل من مليون صف). على سبيل المثال، يمكن استخدام هذا الجدول لتخزين دفعات البيانات الواردة تمهيدًا لتحويلها عندما تكون هناك حاجة إلى معالجتها بصورة ذرّية. يمكن لخادم ClickHouse دعم 100 ألف مثيل من هذا النوع من الجداول. ويُفضَّل استخدام محرك الجدول هذا بدلًا من [Log](./log.md) عند الحاجة إلى عدد كبير من الجداول، وذلك على حساب كفاءة القراءة.

<div id="table_engines-stripelog-creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

راجع الوصف التفصيلي للاستعلام [CREATE TABLE](/ar/sql-reference/statements/create/table).

<div id="table_engines-stripelog-writing-the-data">
  ## كتابة البيانات
</div>

يخزّن المحرّك `StripeLog` جميع الأعمدة في ملف واحد. ومع كل استعلام `INSERT`، يُلحِق ClickHouse كتلة البيانات بنهاية ملف الجدول، ويكتب الأعمدة واحدًا تلو الآخر.

لكل جدول، يكتب ClickHouse الملفات التالية:

* `data.bin` — ملف البيانات.
* `index.mrk` — ملف العلامات. تحتوي العلامات على الإزاحات الخاصة بكل عمود في كل كتلة بيانات أُدرجت.

لا يدعم المحرّك `StripeLog` عمليتي `ALTER UPDATE` و`ALTER DELETE`.

<div id="table_engines-stripelog-reading-the-data">
  ## قراءة البيانات
</div>

يتيح الملف الذي يحتوي على العلامات في ClickHouse قراءة البيانات على التوازي. وهذا يعني أن استعلام `SELECT` يُرجع الصفوف بترتيب غير متوقَّع. استخدم عبارة `ORDER BY` لفرز الصفوف.

<div id="table_engines-stripelog-example-of-use">
  ## مثال للاستخدام
</div>

إنشاء جدول:

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

إدراج البيانات:

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

استخدمنا استعلامَي `INSERT` لإنشاء كتلتَي بيانات داخل الملف `data.bin`.

يستخدم ClickHouse عدة خيوط تنفيذ عند تحديد البيانات. يقرأ كل خيط تنفيذ كتلة بيانات منفصلة ويُرجع الصفوف الناتجة بشكل مستقل فور انتهائه. ونتيجة لذلك، لا يتطابق ترتيب كتل الصفوف في المخرجات، في معظم الحالات، مع ترتيب هذه الكتل نفسها في الإدخال. على سبيل المثال:

```sql
SELECT * FROM stripe_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

فرز النتائج (تصاعديًا افتراضيًا):

```sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```