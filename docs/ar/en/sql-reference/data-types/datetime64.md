---
description: 'توثيق لنوع البيانات DateTime64 في ClickHouse، الذي يخزّن
  طوابع زمنية بدقة دون الثانية'
sidebar_label: 'DateTime64'
sidebar_position: 18
slug: /sql-reference/data-types/datetime64
title: 'DateTime64'
doc_type: 'reference'
---

يتيح تخزين لحظة زمنية يمكن التعبير عنها كتاريخ تقويمي ووقت من اليوم، بدقة محددة لأجزاء الثانية

حجم tick ‏(دقة): 10<sup>-precision</sup> ثانية. النطاق الصالح: [ 0 : 9 ].
وعادةً ما تُستخدم القيم التالية: 3 (ميلي ثانية)، 6 (ميكروثانية)، 9 (نانوثانية).

القيمة الافتراضية: 3 (ميلي ثانية).

**الصياغة:**

```sql
DateTime64(precision, [timezone])
```

داخليًا، يخزّن البيانات على شكل عدد من &#39;tick&#39; منذ بداية epoch ‏(1970-01-01 00:00:00 UTC) بصيغة Int64. ويُحدَّد مستوى دقة `tick` بواسطة معامل الدقة. بالإضافة إلى ذلك، يمكن للنوع `DateTime64` تخزين منطقة زمنية واحدة مشتركة للـ عمود بأكمله، ما يؤثر في كيفية عرض قيم النوع `DateTime64` بتنسيق نصي وكيفية تحليل القيم المحددة كسلاسل نصية (&#39;2020-01-01 05:00:01.000&#39;). ولا تُخزَّن المنطقة الزمنية في rows الخاصة بالـ جدول (أو في resultset)، وإنما تُخزَّن في metadata الخاصة بالـ عمود. راجع التفاصيل في [DateTime](../../sql-reference/data-types/datetime.md).

النطاق المدعوم للقيم: [1900-01-01 00:00:00, 2299-12-31 23:59:59.999999999]

يعتمد عدد الأرقام بعد decimal point على معامل الدقة.

ملاحظة: دقة القيمة القصوى هي 8. وإذا استُخدمت الدقة القصوى البالغة 9 أرقام (nanoseconds)، فإن الحد الأقصى للقيمة المدعومة هو `2262-04-11 23:47:16` بتوقيت UTC.

<div id="examples">
  ## أمثلة
</div>

1. إنشاء جدول بعمود من النوع `DateTime64` وإدراج البيانات فيه:

```sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = MergeTree;
```

```sql
-- Parse DateTime
-- - from an integer interpreted as the number of milliseconds (because of precision 3) since 1970-01-01,
-- - from a decimal interpreted as the number of seconds before the decimal part, and based on the precision after the decimal point,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800123, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

* عند إدراج قيمة datetime كعدد صحيح، تُعامَل على أنها Unix Timestamp ‏(UTC) مضبوط بالمقياس المناسب. تمثل القيمة `1546300800000` (بدقة 3) `'2019-01-01 00:00:00'` بتوقيت UTC. ولكن بما أن العمود `timestamp` محدد له منطقة زمنية ‏`Asia/Istanbul` ‏(UTC+3)، فعند إخراج القيمة كسلسلة نصية ستظهر على أنها `'2019-01-01 03:00:00'`. أما عند إدراج datetime كعدد عشري، فتُعامَل بالطريقة نفسها تقريبًا كما في العدد الصحيح، باستثناء أن القيمة الواقعة قبل decimal point تكون هي Unix Timestamp حتى مستوى الثواني بما في ذلك الثواني نفسها، وما بعد decimal point يُعامَل على أنه دقة.
* عند إدراج قيمة string كـ datetime، تُعامَل على أنها ضمن المنطقة الزمنية الخاصة بالعمود. وستُعامَل `'2019-01-01 00:00:00'` على أنها ضمن منطقة زمنية ‏`Asia/Istanbul` وتُخزَّن على شكل `1546290000000`.

2. التصفية على قيم `DateTime64`

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

بخلاف `DateTime`، لا تُحوَّل قيم `DateTime64` تلقائيًا من `String`.

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

بخلاف الإدراج، ستتعامل الدالة `toDateTime64` مع جميع القيم بوصفها قيماً عشرية، لذا يجب
تحديد الدقة بعد النقطة العشرية.

3. الحصول على المنطقة الزمنية لقيمة من النوع `DateTime64`:

```sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. التحويل بين المناطق الزمنية

```sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') AS lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') AS istanbul_time
FROM dt64;
```

```text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**راجع أيضًا**

* [دوال تحويل الأنواع](../../sql-reference/functions/type-conversion-functions.md)
* [الدوال الخاصة بالتعامل مع التواريخ والأوقات](../../sql-reference/functions/date-time-functions.md)
* [الإعداد `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [الإعداد `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [مَعلمة إعداد الخادم `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [الإعداد `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [المعاملات الخاصة بالتعامل مع التواريخ والأوقات](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [نوع البيانات `Date`](../../sql-reference/data-types/date.md)
* [نوع البيانات `DateTime`](../../sql-reference/data-types/datetime.md)