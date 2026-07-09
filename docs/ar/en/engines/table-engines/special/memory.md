---
description: 'يخزن محرك Memory البيانات في RAM، بصيغة غير مضغوطة. وتُخزَّن البيانات
  تمامًا بالشكل الذي تُستقبل به عند قراءتها. وبعبارة أخرى، فإن القراءة من هذا
  الجدول لا تكاد تكلّف شيئًا.'
sidebar_label: 'Memory'
sidebar_position: 110
slug: /engines/table-engines/special/memory
title: 'محرك الجدول Memory'
doc_type: 'مرجع'
---

:::note
عند استخدام محرك الجدول Memory على ClickHouse Cloud، لا تُنسخ البيانات عبر جميع العُقد (بحكم التصميم). ولضمان توجيه جميع الاستعلامات إلى العُقدة نفسها وأن يعمل محرك الجدول Memory كما هو متوقع، يمكنك تنفيذ أحد الإجراءين التاليين:

* نفّذ جميع العمليات ضمن الجلسة نفسها
* استخدم عميلاً يعتمد TCP أو الواجهة الأصلية (مما يتيح دعم الاتصالات الثابتة)، مثل [clickhouse-client](/ar/interfaces/client)
  :::

يخزن محرك Memory البيانات في RAM، بصيغة غير مضغوطة. وتُخزَّن البيانات تمامًا بالشكل الذي تُستقبل به عند قراءتها. وبعبارة أخرى، فإن القراءة من هذا الجدول لا تكاد تكلّف شيئًا.
تتم مزامنة الوصول المتزامن إلى البيانات. والأقفال قصيرة: فلا تحجب عمليات القراءة والكتابة بعضها بعضًا.
الفهارس غير مدعومة. وتُنَفَّذ القراءة بشكل متوازٍ.

يتحقق أعلى أداء (أكثر من 10 GB/sec) في الاستعلامات البسيطة، لأنه لا توجد قراءة من القرص، ولا فك ضغط، ولا فك تسلسل للبيانات. (ويجدر التنبيه إلى أن أداء محرك MergeTree يكون، في كثير من الحالات، مرتفعًا تقريبًا بالقدر نفسه.)
عند إعادة تشغيل الخادم، تختفي البيانات من الجدول ويصبح الجدول فارغًا.
في العادة، لا يكون استخدام محرك الجدول هذا مبررًا. ومع ذلك، يمكن استخدامه للاختبارات، وللمهام التي تتطلب أقصى سرعة مع عدد صغير نسبيًا من الصفوف (حتى نحو 100,000,000).

يستخدم النظام محرك Memory للجداول المؤقتة ذات البيانات الخارجية للاستعلام (راجع قسم &quot;البيانات الخارجية لمعالجة استعلام&quot;)، وكذلك لتنفيذ `GLOBAL IN` (راجع قسم &quot;عوامل التشغيل `IN`&quot;).

يمكن تحديد حدين أدنى وأعلى لتقييد حجم جدول محرك Memory، مما يتيح له عمليًا العمل كمخزن مؤقت دائري (راجع [معلمات المحرك](#engine-parameters)).

<div id="engine-parameters">
  ## معلمات المحرك
</div>

* `min_bytes_to_keep` — الحد الأدنى من البايتات التي يجب الاحتفاظ بها عندما يكون حجم جدول الذاكرة مقيّدًا.
  * القيمة الافتراضية: `0`
  * يتطلب `max_bytes_to_keep`
* `max_bytes_to_keep` — الحد الأقصى من البايتات التي يجب الاحتفاظ بها داخل جدول الذاكرة، حيث تُحذف أقدم الصفوف عند كل عملية إدراج (أي كمخزن مؤقت دائري). وقد يتجاوز الحد الأقصى للبايتات الحدَّ المذكور إذا كانت أقدم دفعة من الصفوف المطلوب إزالتها ستنخفض عن حد `min_bytes_to_keep` عند إضافة كتلة كبيرة.
  * القيمة الافتراضية: `0`
* `min_rows_to_keep` — الحد الأدنى من الصفوف التي يجب الاحتفاظ بها عندما يكون حجم جدول الذاكرة مقيّدًا.
  * القيمة الافتراضية: `0`
  * يتطلب `max_rows_to_keep`
* `max_rows_to_keep` — الحد الأقصى من الصفوف التي يجب الاحتفاظ بها داخل جدول الذاكرة، حيث تُحذف أقدم الصفوف عند كل عملية إدراج (أي كمخزن مؤقت دائري). وقد يتجاوز الحد الأقصى للصفوف الحدَّ المذكور إذا كانت أقدم دفعة من الصفوف المطلوب إزالتها ستنخفض عن حد `min_rows_to_keep` عند إضافة كتلة كبيرة.
  * القيمة الافتراضية: `0`
* `compress` - ما إذا كان يجب ضغط البيانات في الذاكرة.
  * القيمة الافتراضية: `false`

<div id="usage">
  ## الاستخدام
</div>

**تهيئة الإعدادات**

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**تعديل الإعدادات**

```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**ملاحظة:** يمكن تعيين معامِلَي التقييد `bytes` و`rows` في الوقت نفسه، ولكن سيُلتزم بالقيمة الأقل من `max` و`min`.

<div id="examples">
  ## أمثلة
</div>

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

وكذلك بالنسبة للصفوف:

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```