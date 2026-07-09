---
description: 'توثيق دالة النافذة rank'
sidebar_label: 'rank'
sidebar_position: 6
slug: /sql-reference/window-functions/rank
title: 'rank'
doc_type: 'مرجع'
---

تُسنِد هذه الدالة رتبةً للصف الحالي داخل قسمه مع وجود فجوات. وبعبارة أخرى، إذا كانت قيمة صف ما مساوية لقيمة صف سابق، فسيحصل على الرتبة نفسها التي حصل عليها ذلك الصف السابق.
ثم تكون رتبة الصف التالي مساوية لرتبة الصف السابق مضافًا إليها فجوة تساوي عدد المرات التي مُنحت فيها الرتبة السابقة.

توفّر الدالة [dense&#95;rank](./dense_rank.md) السلوك نفسه ولكن من دون فجوات في الترتيب.

**الصياغة**

```sql
rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

لمزيد من التفاصيل حول صياغة الدوال النافذة، راجع: [الدوال النافذة - الصياغة](./index.md/#syntax).

**القيمة المُعادة**

* رقم للصف الحالي ضمن القسم الخاص به، مع احتساب الفجوات. [UInt64](../data-types/int-uint.md).

**مثال**

يعتمد المثال التالي على المثال الوارد في فيديو الشرح [ترتيب الدوال النافذة في ClickHouse](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA).

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
       rank() OVER (ORDER BY salary DESC) AS rank
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─rank─┐
1. │ Gary Chen       │ 195000 │    1 │
2. │ Robert George   │ 195000 │    1 │
3. │ Charles Juarez  │ 190000 │    3 │
4. │ Douglas Benson  │ 150000 │    4 │
5. │ Michael Stanley │ 150000 │    4 │
6. │ Scott Harrison  │ 150000 │    4 │
7. │ James Henderson │ 140000 │    7 │
   └─────────────────┴────────┴──────┘
```