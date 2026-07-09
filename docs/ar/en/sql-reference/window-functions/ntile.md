---
description: 'توثيق دالة النافذة ntile'
sidebar_label: 'ntile'
sidebar_position: 13
slug: /sql-reference/window-functions/ntile
title: 'ntile'
doc_type: 'reference'
---

تقسّم الصفوف المرتَّبة داخل قسم إلى عدد محدد من المجموعات بأحجام متقاربة قدر الإمكان، وتُرجع رقم المجموعة التي ينتمي إليها الصف الحالي. يبدأ ترقيم المجموعات من 1. وفي كل قسم، تُسنَد الصفوف إلى المجموعات بالترتيب: إذا كان عدد الصفوف غير قابل للقسمة على عدد المجموعات، تحصل المجموعات الأولى على صف إضافي واحد مقارنةً بالمجموعات اللاحقة.

**الصياغة**

```sql
ntile (buckets)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

يجب أن تكون الوسيطة `buckets` عددًا صحيحًا موجبًا ثابتًا.

تُعد عبارة `ORDER BY` مطلوبة. ويجب أن يكون إطار النافذة هو التقسيم بأكمله (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`)، وهو أيضًا الإطار الافتراضي المستخدم عند عدم تحديد أي إطار صراحةً.

لمزيد من التفاصيل حول صياغة دوال النوافذ، راجع: [Window Functions - Syntax](./index.md/#syntax).

**القيمة المُعادة**

* رقم الحاوية للصف الحالي ضمن التقسيم الخاص به. [UInt64](../data-types/int-uint.md).

**مثال**

يقسم المثال التالي اللاعبين إلى أربع حاويات مرتبة حسب الراتب ترتيبًا تنازليًا.

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
       ntile(4) OVER (ORDER BY salary DESC, player ASC) AS bucket
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─bucket─┐
1. │ Gary Chen       │ 195000 │      1 │
2. │ Robert George   │ 195000 │      1 │
3. │ Charles Juarez  │ 190000 │      2 │
4. │ Douglas Benson  │ 150000 │      2 │
5. │ Michael Stanley │ 150000 │      3 │
6. │ Scott Harrison  │ 150000 │      3 │
7. │ James Henderson │ 140000 │      4 │
   └─────────────────┴────────┴────────┘
```

يوجد هنا سبعة صفوف وأربع حاويات، لذا يضم كلٌّ من الحاويات الثلاث الأولى صفّين، بينما تضم الحاوية الأخيرة صفًا واحدًا.