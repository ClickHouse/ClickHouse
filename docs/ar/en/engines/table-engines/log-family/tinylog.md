---
description: 'توثيق محرك الجدول TinyLog'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'محرك الجدول TinyLog'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="tinylog-table-engine">
  # محرك الجدول TinyLog
</div>

<CloudNotSupportedBadge />

ينتمي هذا المحرك إلى عائلة محركات Log. راجع [عائلة محركات Log](../../../engines/table-engines/log-family/index.md) للتعرّف على الخصائص المشتركة لمحركات السجل والفروق بينها.

يُستخدم محرك الجدول هذا عادةً وفق نمط الكتابة لمرة واحدة: تُكتب البيانات مرة واحدة، ثم تُقرأ بعد ذلك كلما دعت الحاجة. على سبيل المثال، يمكنك استخدام الجداول من النوع `TinyLog` للبيانات الوسيطة التي تُعالَج على دفعات صغيرة. لاحظ أن تخزين البيانات في عدد كبير من الجداول الصغيرة غير فعّال.

تُنفَّذ الاستعلامات في تدفق واحد. وبعبارة أخرى، صُمِّم هذا المحرك للجداول الصغيرة نسبيًا (حتى نحو 1,000,000 صف). ويكون استخدام محرك الجدول هذا منطقيًا إذا كان لديك عدد كبير من الجداول الصغيرة، لأنه أبسط من محرك [Log](../../../engines/table-engines/log-family/log.md) (إذ يلزم فتح عدد أقل من الملفات).

<div id="characteristics">
  ## الخصائص
</div>

* **بنية أبسط**: بخلاف محرك Log، لا يستخدم TinyLog ملفات العلامات. وهذا يقلل من التعقيد، لكنه يحد أيضًا من تحسينات الأداء لمجموعات البيانات الكبيرة.
* **استعلامات أحادية التدفق**: تُنفَّذ الاستعلامات على جداول TinyLog ضمن تدفق واحد، مما يجعله مناسبًا للجداول الصغيرة نسبيًا، وعادةً حتى 1,000,000 صف.
* **فعّال للجداول الصغيرة**: تجعل بساطة محرك TinyLog منه خيارًا مناسبًا عند إدارة عدد كبير من الجداول الصغيرة، إذ يتطلب عمليات على الملفات أقل مقارنةً بمحرك Log.

بخلاف محرك Log، لا يستخدم TinyLog ملفات العلامات. وهذا يقلل من التعقيد، لكنه يحد أيضًا من تحسينات الأداء لمجموعات البيانات الأكبر.

<div id="table_engines-tinylog-creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

اطّلع على الوصف التفصيلي لاستعلام [CREATE TABLE](/ar/sql-reference/statements/create/table).

<div id="table_engines-tinylog-writing-the-data">
  ## كتابة البيانات
</div>

يُخزّن المحرّك `TinyLog` جميع الأعمدة في ملف واحد. ولكل استعلام `INSERT`، يُلحِق ClickHouse كتلة البيانات بنهاية ملف الجدول، ويكتب الأعمدة واحدًا تلو الآخر.

لكل جدول، يكتب ClickHouse الملفات التالية:

* `<column>.bin`: ملف بيانات لكل عمود، يحتوي على البيانات المُسلسلة والمضغوطة.

لا يدعم المحرّك `TinyLog` العمليتين `ALTER UPDATE` و`ALTER DELETE`.

<div id="table_engines-tinylog-example-of-use">
  ## مثال على الاستخدام
</div>

إنشاء جدول:

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

إدراج البيانات:

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

استخدمنا استعلامَي `INSERT` لإنشاء كتلتَي بيانات داخل ملفات `<column>.bin`.

يستخدم ClickHouse تدفقًا واحدًا عند قراءة البيانات. ونتيجةً لذلك، يتطابق ترتيب كتل الصفوف في المخرجات مع ترتيب الكتل نفسها في المدخلات. على سبيل المثال:

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```