---
description: 'توثيق الدالة النافذة lagInFrame'
sidebar_label: 'lagInFrame'
sidebar_position: 9
slug: /sql-reference/window-functions/lagInFrame
title: 'lagInFrame'
doc_type: 'reference'
---

تعيد قيمةً جرى تقييمها عند صف يقع قبل الصف الحالي بعدد محدد من الصفوف ضمن الإطار المرتب.

:::warning
يختلف سلوك `lagInFrame` عن سلوك الدالة النافذة القياسية `lag` في SQL.
تراعي الدالة النافذة `lagInFrame` في ClickHouse إطار النافذة.
للحصول على سلوك مطابق تمامًا لـ `lag`، استخدم `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
:::

**الصياغة**

```sql
lagInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

لمزيد من التفاصيل حول بنية دوال النافذة، راجع: [دوال النافذة - البنية](./index.md/#syntax).

**المعلمات**

* `x` — اسم العمود.
* `offset` — الإزاحة المراد تطبيقها. [(U)Int*](../data-types/int-uint.md). (اختياري - `1` افتراضيًا).
* `default` — القيمة التي تُعاد إذا تجاوز الصف المحسوب حدود إطار النافذة. (اختياري - القيمة الافتراضية لنوع العمود عند عدم تحديدها).

**القيمة المُعادة**

* القيمة التي تُقيَّم عند الصف الواقع على إزاحة فعلية محددة قبل الصف الحالي ضمن الإطار المرتَّب.

**مثال**

يعرض هذا المثال بيانات تاريخية لسهم محدد، ويستخدم الدالة `lagInFrame` لحساب دلتا يومية والتغير المئوي في سعر إغلاق السهم.

```sql title="Query"
CREATE TABLE stock_prices
(
    `date`   Date,
    `open`   Float32, -- opening price
    `high`   Float32, -- daily high
    `low`    Float32, -- daily low
    `close`  Float32, -- closing price
    `volume` UInt32   -- trade volume
)
Engine = Memory;

INSERT INTO stock_prices FORMAT Values
    ('2024-06-03', 113.62, 115.00, 112.00, 115.00, 438392000),
    ('2024-06-04', 115.72, 116.60, 114.04, 116.44, 403324000),
    ('2024-06-05', 118.37, 122.45, 117.47, 122.44, 528402000),
    ('2024-06-06', 124.05, 125.59, 118.32, 121.00, 664696000),
    ('2024-06-07', 119.77, 121.69, 118.02, 120.89, 412386000);
```

```sql title="Query"
SELECT
    date,
    close,
    lagInFrame(close, 1, close) OVER (ORDER BY date ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
     ) AS previous_day_close,
    COALESCE(ROUND(close - previous_day_close, 2)) AS delta,
    COALESCE(ROUND((delta / previous_day_close) * 100, 2)) AS percent_change
FROM stock_prices
ORDER BY date DESC
```

```response title="Response"
   ┌───────date─┬──close─┬─previous_day_close─┬─delta─┬─percent_change─┐
1. │ 2024-06-07 │ 120.89 │                121 │ -0.11 │          -0.09 │
2. │ 2024-06-06 │    121 │             122.44 │ -1.44 │          -1.18 │
3. │ 2024-06-05 │ 122.44 │             116.44 │     6 │           5.15 │
4. │ 2024-06-04 │ 116.44 │                115 │  1.44 │           1.25 │
5. │ 2024-06-03 │    115 │                115 │     0 │              0 │
   └────────────┴────────┴────────────────────┴───────┴────────────────┘
```