---
description: 'توثيق دالة النافذة nonNegativeDerivative'
sidebar_label: 'nonNegativeDerivative'
sidebar_position: 12
slug: /sql-reference/window-functions/nonNegativeDerivative
title: 'nonNegativeDerivative'
doc_type: 'reference'
---

تحسب المشتقة غير السالبة لـ `metric_column` بالنسبة إلى `timestamp_column`.
هذه دالة نافذة خاصة بـ ClickHouse وليست جزءاً من معيار SQL.

لكل صف، تُحسب المشتقة بالنسبة إلى *الصف السابق في ترتيب تقييم النافذة*، الذي تحدده عبارة `ORDER BY` الخاصة بالنافذة - وليس `timestamp_column`.
يُستخدم الوسيط `timestamp_column` فقط لقياس الوقت المنقضي بين الصف الحالي والصف السابق؛ ولا يتحكم في ترتيب الصفوف بحد ذاته.

:::warning
لا تُرتّب `nonNegativeDerivative` الصفوف بحسب `timestamp_column`؛ بل تتولى ذلك عبارة `ORDER BY` الخاصة بالنافذة.
لكي تنطبق الصيغة أدناه، يجب أن يكون `timestamp_column` متزايداً تزايداً صارماً في ترتيب تقييم النافذة، لذا ينبغي في العادة ترتيب النافذة بحسب `timestamp_column` تصاعدياً (مثلاً `... OVER (ORDER BY ts ASC)` مع `nonNegativeDerivative(metric, ts)`).
في كل مرة يكون فيها الوقت المنقضي بين الصف الحالي والصف السابق غير موجب - وهو ما يحدث عند استخدام `ORDER BY timestamp_column DESC` أو عند تكرار الطوابع الزمنية (تساويها) - تُعيد الدالة `0` لذلك الصف بدلاً من تطبيق الصيغة.
:::

النتيجة هي معدل تغير المقياس لكل `INTERVAL`، مع تثبيت أي قيمة سالبة عند `0`.
يُفيد هذا في المقاييس المتزايدة تزايداً رتيباً، كالعدادات، إذ يشير الانخفاض فيها عادةً إلى إعادة ضبط وليس إلى معدل سالب حقيقي.

**Syntax**

```sql
nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name AS ([PARTITION BY grouping_column] [ORDER BY sorting_column] [ROWS or RANGE expression_to_bound_rows_within_the_group])
```

لمزيد من التفاصيل حول صياغة دوال النافذة، راجع: [Window Functions - Syntax](./index.md/#syntax).

**Arguments**

- `metric_column` — العمود الذي تُحسب مشتقته. [(U)Int*](../data-types/int-uint.md) أو [Float*](../data-types/float.md).
- `timestamp_column` — العمود المستخدم لقياس الوقت المنقضي بين الصف الحالي والصف السابق في ترتيب النافذة. لا يتحكم في ترتيب الصفوف؛ بل تتولى ذلك عبارة `ORDER BY` الخاصة بالنافذة، وينبغي عادةً أن تستخدم هذا العمود نفسه. [DateTime](../data-types/datetime.md) أو [DateTime64](../data-types/datetime64.md).
- `INTERVAL X UNITS` — اختياري. وحدة الزمن التي تُقاس إليها النتيجة. القيمة الافتراضية هي `INTERVAL 1 SECOND`. الوحدات ذات الطول الثابت فقط مدعومة (`NANOSECOND`، `MICROSECOND`، `MILLISECOND`، `SECOND`، `MINUTE`، `HOUR`، `DAY`، `WEEK`)؛ أما الوحدات ذات الطول المتغير (`MONTH`، `QUARTER`، `YEAR`) فتُطلق استثناءً.

**Returned value**

لكل صف، تُحسب القيمة على النحو التالي:

- `0` للصف الأول؛
- `0` لأي صف يكون فيه الوقت المنقضي منذ الصف السابق غير موجب (أي $\text{timestamp}_i - \text{timestamp}_{i-1} \le 0$، كما يحدث مع الترتيب التنازلي أو الطوابع الزمنية المكررة)؛ و
- ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}} * \text{interval}$ في جميع الحالات الأخرى.

إذا كانت القيمة المحسوبة سالبة، تُثبَّت عند `0`. نوع القيمة المُعادة هو [Float64](../data-types/float.md).

**Example**

يحسب المثال التالي معدل التغير في الثانية لقراءة مستشعر.
لاحظ أن الصف الثالث ينخفض من `110` إلى `105`، لذا تُثبَّت مشتقته عند `0`.

```sql title="Query"
CREATE TABLE sensor_readings
(
    `sensor_id` UInt32,
    `ts`        DateTime,
    `reading`   Float64
)
ENGINE = Memory;

INSERT INTO sensor_readings VALUES
    (1, '2024-01-01 00:00:00', 100),
    (1, '2024-01-01 00:00:10', 110),
    (1, '2024-01-01 00:00:20', 105),
    (1, '2024-01-01 00:00:30', 130);
```

```sql title="Query"
SELECT
    ts,
    reading,
    nonNegativeDerivative(reading, ts) OVER (ORDER BY ts ASC) AS deriv_per_second
FROM sensor_readings
ORDER BY ts ASC;
```

```response title="Response"
   ┌──────────────────ts─┬─reading─┬─deriv_per_second─┐
1. │ 2024-01-01 00:00:00 │     100 │                0 │
2. │ 2024-01-01 00:00:10 │     110 │                1 │
3. │ 2024-01-01 00:00:20 │     105 │                0 │
4. │ 2024-01-01 00:00:30 │     130 │              2.5 │
   └─────────────────────┴─────────┴──────────────────┘
```