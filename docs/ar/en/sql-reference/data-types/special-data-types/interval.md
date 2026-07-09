---
description: 'توثيق لنوع البيانات الخاص فترة'
sidebar_label: 'فترة'
sidebar_position: 61
slug: /sql-reference/data-types/special-data-types/interval
title: 'فترة'
doc_type: 'reference'
---

فئة أنواع البيانات التي تمثل الفترات الزمنية وفترات التاريخ. وهي الأنواع الناتجة عن العامل [INTERVAL](/ar/sql-reference/operators#interval).

البنية:

* فاصل زمني أو تاريخي ممثّل كقيمة عدد صحيح غير موقعة.
* نوع الفاصل.

أنواع فترة المدعومة:

* `NANOSECOND`
* `MICROSECOND`
* `MILLISECOND`
* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

لكل نوع فترة، يوجد نوع بيانات منفصل. على سبيل المثال، يقابل الفاصل `DAY` نوع البيانات `IntervalDay`:

```sql
SELECT toTypeName(INTERVAL 4 DAY)
```

```text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

<div id="usage-remarks">
  ## ملاحظات الاستخدام
</div>

يمكنك استخدام قيم من نوع `Interval` في العمليات الحسابية مع قيم من نوع [Date](../../../sql-reference/data-types/date.md) و[DateTime](../../../sql-reference/data-types/datetime.md). على سبيل المثال، يمكنك إضافة 4 أيام إلى الوقت الحالي:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY
```

```text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

ويمكن أيضًا استخدام عدة فترات في الوقت نفسه:

```sql
SELECT now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

```text
┌───current_date_time─┬─plus(current_date_time, plus(toIntervalDay(4), toIntervalHour(3)))─┐
│ 2024-08-08 18:31:39 │                                                2024-08-12 21:31:39 │
└─────────────────────┴────────────────────────────────────────────────────────────────────┘
```

ولمقارنة القيم ذات الفترات المختلفة:

```sql
SELECT toIntervalMicrosecond(179999999) < toIntervalMinute(3);
```

```text
┌─less(toIntervalMicrosecond(179999999), toIntervalMinute(3))─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
```

```sql
SELECT toIntervalMicrosecond(3600000000) = toIntervalHour(1);
```

```text
┌─equals(toIntervalMicrosecond(3600000000), toIntervalHour(1))─┐
│                                                            1 │
└──────────────────────────────────────────────────────────────┘
```

<div id="mixed-type-intervals">
  ## الفترات المختلطة النوع
</div>

يمكن إنشاء الفترات المختلطة النوع، مثل عدة ساعات وعدة دقائق، باستخدام صياغة `INTERVAL 'value' <from_kind> TO <to_kind>`.
وتكون النتيجة Tuple يتكون من فترتين أو أكثر.

التركيبات المدعومة:

| الصياغة            | تنسيق السلسلة | مثال                                  |
| ------------------ | ------------- | ------------------------------------- |
| `YEAR TO MONTH`    | `Y-M`         | `INTERVAL '2-6' YEAR TO MONTH`        |
| `DAY TO HOUR`      | `D H`         | `INTERVAL '5 12' DAY TO HOUR`         |
| `DAY TO MINUTE`    | `D H:M`       | `INTERVAL '5 12:30' DAY TO MINUTE`    |
| `DAY TO SECOND`    | `D H:M:S`     | `INTERVAL '5 12:30:45' DAY TO SECOND` |
| `HOUR TO MINUTE`   | `H:M`         | `INTERVAL '1:30' HOUR TO MINUTE`      |
| `HOUR TO SECOND`   | `H:M:S`       | `INTERVAL '1:30:45' HOUR TO SECOND`   |
| `MINUTE TO SECOND` | `M:S`         | `INTERVAL '5:30' MINUTE TO SECOND`    |

تُتحقق القيم في الحقول غير الأولية وفقًا لمعيار SQL: `MONTH` من 0 إلى 11، و`HOUR` من 0 إلى 23، و`MINUTE` من 0 إلى 59، و`SECOND` من 0 إلى 59.

```sql
SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

تسري العلامة الاختيارية `+` أو `-` في البداية على جميع المكوّنات:

```sql
SELECT INTERVAL '+1:30' HOUR TO MINUTE;
-- this is equivalent to:
-- SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

<div id="see-also">
  ## راجع أيضًا
</div>

* المعامل [INTERVAL](/ar/sql-reference/operators#interval)
* دوال تحويل الأنواع [toInterval](/ar/sql-reference/functions/type-conversion-functions#toIntervalYear)