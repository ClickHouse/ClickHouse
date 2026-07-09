---
description: 'توثيق الدالة النافذة leadInFrame'
sidebar_label: 'leadInFrame'
sidebar_position: 10
slug: /sql-reference/window-functions/leadInFrame
title: 'leadInFrame'
doc_type: 'reference'
---

تعيد قيمةً مُقيَّمة عند الصف الواقع بعد الصف الحالي بمقدار `offset` من الصفوف داخل الإطار المرتّب.

:::warning
يختلف سلوك `leadInFrame` عن الدالة النافذة القياسية `lead` في SQL.
تحترم الدالة النافذة `leadInFrame` في ClickHouse إطار النافذة.
للحصول على سلوك مطابق تمامًا لـ `lead`، استخدم `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
:::

**الصياغة**

```sql
leadInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

لمزيد من التفاصيل حول صياغة دوال النوافذ، انظر: [الدوال النافذة - الصياغة](./index.md/#syntax).

**المعاملات**

* `x` — اسم العمود.
* `offset` — الإزاحة المطلوب تطبيقها. [(U)Int*](../data-types/int-uint.md). (اختياري - `1` افتراضيًا).
* `default` — القيمة التي تُعاد إذا تجاوز الصف المحسوب حدود إطار النافذة. (اختياري - تُستخدم القيمة الافتراضية لنوع العمود عند عدم تحديدها).

**القيمة المُعادة**

* القيمة التي جرى تقييمها في الصف الواقع بعد الصف الحالي بعدد الصفوف المحدد بواسطة الإزاحة ضمن الإطار المرتب.

**مثال**

يستعرض هذا المثال [بيانات تاريخية](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data) للفائزين بجائزة نوبل، ويستخدم الدالة `leadInFrame` لإرجاع قائمة بالفائزين المتعاقبين في فئة الفيزياء.

```sql title="Query"
CREATE OR REPLACE VIEW nobel_prize_laureates
AS SELECT *
FROM file('nobel_laureates_data.csv');
```

```sql title="Query"
SELECT
    fullName,
    leadInFrame(year, 1, year) OVER (PARTITION BY category ORDER BY year ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS year,
    category,
    motivation
FROM nobel_prize_laureates
WHERE category = 'physics'
ORDER BY year DESC
LIMIT 9
```

```response title="Response"
   ┌─fullName─────────┬─year─┬─category─┬─motivation─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
1. │ Anne L Huillier  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
2. │ Pierre Agostini  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
3. │ Ferenc Krausz    │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
4. │ Alain Aspect     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
5. │ Anton Zeilinger  │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
6. │ John Clauser     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
7. │ Giorgio Parisi   │ 2021 │ physics  │ for the discovery of the interplay of disorder and fluctuations in physical systems from atomic to planetary scales                │
8. │ Klaus Hasselmann │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
9. │ Syukuro Manabe   │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
   └──────────────────┴──────┴──────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```