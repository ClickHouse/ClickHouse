---
description: 'توثيق دالة النافذة dense_rank'
sidebar_label: 'dense_rank'
sidebar_position: 7
slug: /sql-reference/window-functions/dense_rank
title: 'dense_rank'
doc_type: 'reference'
---

ترتّب الصف الحالي داخل القسم الخاص به من دون فجوات. وبعبارة أخرى، إذا كانت قيمة أي صف جديد تتم مواجهته مساوية لقيمة أحد الصفوف السابقة، فسيحصل على الرتبة التالية مباشرةً من دون أي فجوات في الترتيب.

توفّر الدالة [rank](./rank.md) السلوك نفسه، ولكن مع وجود فجوات في الترتيب.

**البنية**

الاسم المستعار: `denseRank` (حساس لحالة الأحرف)

```sql
dense_rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

لمزيد من التفاصيل حول صياغة دوال النافذة، راجع: [دوال النافذة - البنية](./index.md/#syntax).

**القيمة المعادة**

* رقم للصف الحالي ضمن تقسيمه، من دون فجوات في الترتيب. [UInt64](../data-types/int-uint.md).

**مثال**

يستند المثال التالي إلى المثال الوارد في الفيديو التعليمي [Ranking window functions in ClickHouse](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA).

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
       dense_rank() OVER (ORDER BY salary DESC) AS dense_rank
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─dense_rank─┐
1. │ Gary Chen       │ 195000 │          1 │
2. │ Robert George   │ 195000 │          1 │
3. │ Charles Juarez  │ 190000 │          2 │
4. │ Michael Stanley │ 150000 │          3 │
5. │ Douglas Benson  │ 150000 │          3 │
6. │ Scott Harrison  │ 150000 │          3 │
7. │ James Henderson │ 140000 │          4 │
   └─────────────────┴────────┴────────────┘
```