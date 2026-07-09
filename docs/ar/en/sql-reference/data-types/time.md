---
description: 'توثيق لنوع البيانات Time في ClickHouse، الذي يخزن
  الوقت بدقة الثواني'
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: 'Time'
title: 'Time'
doc_type: 'reference'
---

يمثل نوع البيانات `Time` وقتًا يتكوّن من الساعة والدقيقة والثانية.
وهو مستقل عن أي تاريخ تقويمي، ومناسب للقيم التي لا تحتاج إلى مكوّنات اليوم والشهر والسنة.

البنية:

```sql
Time
```

نطاق تمثيل النص: ‎[-999:59:59, 999:59:59].

الدقة: ثانية واحدة.

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

**التمثيل والأداء**.
يخزّن نوع البيانات `Time` داخليًا عددًا صحيحًا موقّعًا من 32 بت يمثّل الثواني.
ولأن القيم من النوعين `Time` و`DateTime` لها الحجم نفسه بالبايت، فإن أداءهما متقارب.

**التطبيع**.
عند تحليل السلاسل النصية إلى `Time`، تُطبَّع مكوّنات الوقت من دون التحقق من صحتها.
على سبيل المثال، يُفسَّر `25:70:70` على أنه `26:11:10`.

**القيم السالبة**.
إشارات الطرح في البداية مدعومة ويُحتفَظ بها.
وتنشأ القيم السالبة عادةً من العمليات الحسابية على قيم `Time`.
وبالنسبة إلى النوع `Time`، يُحتفَظ بالمدخلات السالبة سواء كانت نصية (مثل `'-01:02:03'`) أو رقمية (مثل `-3723`).

**الإشباع**.
يُقيَّد مكوّن وقت اليوم ضمن النطاق [-999:59:59, 999:59:59].
وتُمثَّل القيم التي تتجاوز ساعاتها 999 (أو تقل عن -999) ويُحتفَظ بها عند تحويلها إلى نص ثم قراءتها مرة أخرى بالشكل `999:59:59` (أو `-999:59:59`).

**المناطق الزمنية**.
لا يدعم `Time` المناطق الزمنية، أي إن قيم `Time` تُفسَّر من دون سياق زمني إقليمي.
ويؤدي تحديد منطقة زمنية لـ `Time` كمعامل نوع أو أثناء إنشاء القيمة إلى حدوث خطأ.
وبالمثل، فإن محاولات تطبيق منطقة زمنية على أعمدة `Time` أو تغييرها غير مدعومة وتؤدي إلى خطأ.
ولا يُعاد تفسير قيم `Time` بصمت ضمن مناطق زمنية مختلفة.

<div id="examples">
  ## أمثلة
</div>

**1.** إنشاء جدول يحتوي على عمود من نوع `Time` وإدراج البيانات فيه:

```sql
CREATE TABLE tab
(
    `event_id` UInt8,
    `time` Time
)
ENGINE = TinyLog;
```

```sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 00:00:00.
INSERT INTO tab VALUES (1, '14:30:25'), (2, 52225);

SELECT * FROM tab ORDER BY event_id;
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**2.** التصفية استنادًا إلى قيم `Time`

```sql
SET use_legacy_to_time = 0;
SELECT * FROM tab WHERE time = toTime('14:30:25')
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

يمكن تصفية قيم عمود `Time` باستخدام قيمة نصية في الشرط `WHERE`. وستُحوَّل تلقائيًا إلى `Time`:

```sql
SELECT * FROM tab WHERE time = '14:30:25'
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**3.** التحقق من النوع الناتج:

```sql
SELECT CAST('14:30:25' AS Time) AS column, toTypeName(column) AS type
```

```text
   ┌────column─┬─type─┐
1. │ 14:30:25 │ Time │
   └───────────┴──────┘
```

<div id="addition-with-date">
  ## الجمع مع Date
</div>

يمكن إضافة قيمة من نوع [Time](time.md) إلى قيمة من نوع [Date](date.md) أو [Date32](date32.md) للحصول على [DateTime](datetime.md) أو [DateTime64](datetime64.md):

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') as datetime;
```

```text
   ┌────────────datetime─┐
1. │ 2024-07-15 14:30:25 │
   └─────────────────────┘
```

راجع [إضافة التاريخ والوقت](../operators/index.md#date-time-addition) للاطّلاع على تفاصيل جميع التركيبات المدعومة وأنواع النتائج.

<div id="see-also">
  ## انظر أيضًا
</div>

* [دوال تحويل الأنواع](../functions/type-conversion-functions.md)
* [دوال للتعامل مع التواريخ والأوقات](../functions/date-time-functions.md)
* [دوال للتعامل مع المصفوفات](../functions/array-functions.md)
* [الإعداد `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [الإعداد `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [معلمة تهيئة الخادم `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [الإعداد `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [نوع البيانات `DateTime`](datetime.md)
* [نوع البيانات `Date`](date.md)