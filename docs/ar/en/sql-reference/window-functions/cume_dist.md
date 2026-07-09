---
description: 'توثيق الدالة النافذة cume_dist'
sidebar_label: 'cume_dist'
sidebar_position: 11
slug: /sql-reference/window-functions/cume_dist
title: 'cume_dist'
doc_type: 'reference'
---

تحسب التوزيع التراكمي لقيمة ضمن مجموعة من القيم، أي النسبة المئوية للصفوف التي تكون قيمها أقل من أو تساوي قيمة الصف الحالي. ويمكن استخدامها لتحديد الموقع النسبي لقيمة ضمن قسم.

**الصيغة**

```sql
cume_dist ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

تعريف إطار النافذة الافتراضي والمطلوب هو `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

لمزيد من التفاصيل حول بنية دوال النافذة، راجع: [دوال النافذة - البنية](./index.md/#syntax).

**القيمة المعادة**

* الرتبة النسبية للصف الحالي. نوع الإرجاع هو Float64 ضمن النطاق [0, 1]. [Float64](../data-types/float.md).

**مثال**

يحسب المثال التالي التوزيع التراكمي للرواتب ضمن فريق:

```sql title="Query"
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       cume_dist() OVER (ORDER BY salary DESC) AS cume_dist
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬───────────cume_dist─┐
1. │ Robert George   │ 195000 │  0.2857142857142857 │
2. │ Gary Chen       │ 195000 │  0.2857142857142857 │
3. │ Charles Juarez  │ 190000 │ 0.42857142857142855 │
4. │ Douglas Benson  │ 150000 │  0.8571428571428571 │
5. │ Michael Stanley │ 150000 │  0.8571428571428571 │
6. │ Scott Harrison  │ 150000 │  0.8571428571428571 │
7. │ James Henderson │ 140000 │                   1 │
   └─────────────────┴────────┴─────────────────────┘
```

**تفاصيل التنفيذ**

تحسب الدالة `cume_dist()` المرتبة النسبية باستخدام الصيغة التالية:

```text
cume_dist = (number of rows ≤ current row value) / (total number of rows in partition)
```

تحصل الصفوف ذات القيم المتساوية (النظراء) على قيمة التوزيع التراكمي نفسها، وهي تقابل أعلى ترتيب ضمن مجموعة النظراء.