---
description: 'توثيق دالة النافذة lead'
sidebar_label: 'lead'
sidebar_position: 10
slug: /sql-reference/window-functions/lead
title: 'lead'
doc_type: 'reference'
---

تعيد قيمةً جرى تقييمها عند الصف الواقع بعد `offset` صفوف من الصف الحالي ضمن الإطار المرتّب.
تشبه هذه الدالة [`leadInFrame`](./leadInFrame.md)، لكنها تستخدم دائمًا الإطار `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

**الصياغة**

```sql
lead(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

لمزيد من التفاصيل حول صياغة دوال النوافذ، راجع: [دوال النوافذ - الصياغة](./index.md/#syntax).

**المعاملات**

* `x` — اسم العمود.
* `offset` — الإزاحة المراد تطبيقها. [(U)Int*](../data-types/int-uint.md). (اختياري - `1` افتراضيًا).
* `default` — القيمة التي ستُعاد إذا تجاوز الصف المحسوب حدود إطار النافذة. (اختياري - القيمة الافتراضية لنوع العمود عند عدم تحديدها).

**القيمة المعادة**

* القيمة المُقيَّمة في الصف الذي يقع بعد الصف الحالي بعدد الصفوف المحدد بواسطة الإزاحة ضمن الإطار المرتَّب.

**مثال**

يستعرض هذا المثال [البيانات التاريخية](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data) للفائزين بجائزة نوبل، ويستخدم الدالة `lead` لإرجاع قائمة بالفائزين المتعاقبين في فئة الفيزياء.

```sql title="Query"
CREATE OR REPLACE VIEW nobel_prize_laureates
AS SELECT *
FROM file('nobel_laureates_data.csv');
```

```sql title="Query"
SELECT
    fullName,
    lead(year, 1, year) OVER (PARTITION BY category ORDER BY year ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS year,
    category,
    motivation
FROM nobel_prize_laureates
WHERE category = 'physics'
ORDER BY year DESC
LIMIT 9
```

```response title="Query"
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