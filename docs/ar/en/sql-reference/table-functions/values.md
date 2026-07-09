---
description: 'ينشئ تخزينًا مؤقتًا تُملأ أعمدته بالقيم.'
keywords: ['القيم', 'دالة الجدول']
sidebar_label: 'values'
sidebar_position: 210
slug: /sql-reference/table-functions/values
title: 'values'
doc_type: 'reference'
---

تتيح لك دالة الجدول `Values` إنشاء تخزين مؤقت تُملأ أعمدته بالقيم. وهي مفيدة للاختبارات السريعة أو لإنشاء بيانات نموذجية.

:::note
Values دالة لا تفرّق بين الأحرف الكبيرة والصغيرة. أي إن `VALUES` و`values` كلتيهما صالحتان.
:::

<div id="syntax">
  ## الصيغة
</div>

الصيغة الأساسية لدالة الجدول `VALUES` هي:

```sql
VALUES([structure,] values...)
```

ويُستخدم عادةً كما يلي:

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

<div id="arguments">
  ## الوسائط
</div>

* `column1_name Type1, ...` (اختياري). [String](/ar/sql-reference/data-types/string)
  يحدّد أسماء الأعمدة وأنواعها. وإذا أُهمل هذا الوسيط، فستُسمّى الأعمدة
  `c1` و`c2` وهكذا.
* `(value1_row1, value2_row1)`. [Tuples](/ar/sql-reference/data-types/tuple)
  تحتوي على قيم من أي نوع.

:::note
يمكن أيضًا استبدال الـ Tuples المفصولة بفواصل بقيم مفردة. في هذه الحالة،
تُعامل كل قيمة على أنها صف جديد. راجع قسم [الأمثلة](#examples) لمزيد من
التفاصيل.
:::

<div id="returned-value">
  ## القيمة المُعادة
</div>

* تُرجع جدولًا مؤقتًا يحتوي على القيم المُمرَّرة.

<div id="examples">
  ## أمثلة
</div>

```sql title="Query"
SELECT *
FROM VALUES(
    'person String, place String',
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─person───┬─place─────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

يمكن أيضًا استخدام `VALUES` مع قيم مفردة بدلًا من الـ tuples. على سبيل المثال:

```sql title="Query"
SELECT *
FROM VALUES(
    'person String',
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─person───┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

أو من دون تحديد صف (`'column1_name Type1, column2_name Type2, ...'`
في [الصيغة](#syntax))، وفي هذه الحالة تُسمّى الأعمدة تلقائيًا.

على سبيل المثال:

```sql title="Query"
-- tuples as values
SELECT *
FROM VALUES(
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─c1───────┬─c2────────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

```sql title="Query"
-- single values
SELECT *
FROM VALUES(
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─c1───────┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

<div id="sql-standard-values-clause">
  ## عبارة `VALUES` القياسية في SQL
</div>

اعتبارًا من الإصدار 26.3، يدعم ClickHouse أيضًا عبارة `VALUES` القياسية في SQL كتعبير جدولي
ضمن `FROM`، كما هو الحال في PostgreSQL وMySQL وDuckDB وSQL Server. وتُعاد كتابة هذه الصياغة
داخليًا لاستخدام دالة الجدول ‏`values` الموصوفة أعلاه.

```sql title="Query"
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

```response title="Response"
┌─id─┬─val─┐
│  1 │ a   │
│  2 │ b   │
│  3 │ c   │
└────┴─────┘
```

يمكن استخدامه ضمن تعبيرات الجدول الشائعة:

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

وفي عبارات JOIN:

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

:::note
تتبع الأسماء المستعارة للأعمدة بعد `AS t(col1, col2, ...)` بناء الجملة القياسي في SQL
لتسمية أعمدة الجداول المشتقة. وإذا لم تُذكر، تُسمّى الأعمدة `c1` و`c2` وما إلى ذلك.
:::

<div id="see-also">
  ## راجع أيضًا
</div>

* [صيغة Values](/ar/interfaces/formats/Values)