---
description: 'توثيق Log'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'محرك الجدول Log'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # محرك الجدول Log
</div>

<CloudNotSupportedBadge />

ينتمي هذا المحرك إلى عائلة محركات `Log`. اطّلع على الخصائص المشتركة لمحركات `Log` والفروق بينها في مقال [عائلة محركات Log](../../../engines/table-engines/log-family/index.md).

يختلف `Log` عن [TinyLog](../../../engines/table-engines/log-family/tinylog.md) في وجود ملف صغير من &quot;العلامات&quot; إلى جانب ملفات الأعمدة. وتُكتب هذه العلامات مع كل كتلة بيانات، وتحتوي على إزاحات تشير إلى موضع بدء قراءة الملف لتخطي العدد المحدد من الصفوف. وهذا يتيح قراءة بيانات الجدول باستخدام عدة خيوط تنفيذ.
وعند الوصول المتزامن إلى البيانات، يمكن تنفيذ عمليات القراءة في الوقت نفسه، بينما تحجب عمليات الكتابة عمليات القراءة وتحجب بعضها بعضًا.
لا يدعم محرك `Log` الفهارس. وبالمثل، إذا فشلت الكتابة إلى جدول، يتعطل الجدول، وتؤدي القراءة منه إلى إرجاع خطأ. ويُعد محرك `Log` مناسبًا للبيانات المؤقتة، والجداول التي تُكتب مرة واحدة، وأغراض الاختبار أو العرض التوضيحي.

<div id="table_engines-log-creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

اطّلع على الوصف التفصيلي للاستعلام [CREATE TABLE](/ar/sql-reference/statements/create/table).

<div id="table_engines-log-writing-the-data">
  ## كتابة البيانات
</div>

يخزّن محرك `Log` البيانات بكفاءة من خلال كتابة كل عمود في ملفه الخاص. ولكل جدول، يكتب محرك Log الملفات التالية إلى مسار التخزين المحدد:

* `<column>.bin`: ملف بيانات لكل عمود، يحتوي على بيانات مُسلسلة ومضغوطة.
  `__marks.mrk`: ملف علامات يخزّن الإزاحات وأعداد الصفوف لكل كتلة بيانات تم إدراجها. وتُستخدم هذه العلامات لتسهيل تنفيذ الاستعلامات بكفاءة، إذ تتيح للمحرك تخطي كتل البيانات غير ذات الصلة أثناء القراءة.

<div id="writing-process">
  ### عملية الكتابة
</div>

عند كتابة البيانات في جدول `Log`:

1. تُسلسَل البيانات وتُضغط ضمن كتل.
2. لكل عمود، تُضاف البيانات المضغوطة إلى نهاية ملف `<column>.bin` الخاص به.
3. تُضاف الإدخالات المقابلة إلى ملف `__marks.mrk` لتسجيل الإزاحة وعدد صفوف البيانات التي أُدرجت حديثًا.

<div id="table_engines-log-reading-the-data">
  ## قراءة البيانات
</div>

يتيح ملف العلامات لـ ClickHouse قراءة البيانات بالتوازي. وهذا يعني أن استعلام `SELECT` يعيد الصفوف بترتيب غير متوقّع. استخدم عبارة `ORDER BY` لفرز الصفوف.

<div id="table_engines-log-example-of-use">
  ## مثال للاستخدام
</div>

إنشاء جدول:

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

إدراج البيانات:

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

استخدمنا استعلامَي `INSERT` لإنشاء كتلتَي بيانات داخل ملفات `<column>.bin`.

يستخدم ClickHouse عدة خيوط تنفيذ عند قراءة البيانات. يقرأ كل خيط تنفيذ كتلة بيانات منفصلة ويُرجع الصفوف الناتجة بشكل مستقل عند انتهائه. ونتيجةً لذلك، قد لا يتطابق ترتيب كتل الصفوف في المخرجات مع ترتيب الكتل نفسها في المدخلات. على سبيل المثال:

```sql
SELECT * FROM log_table
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
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```