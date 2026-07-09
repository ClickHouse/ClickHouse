---
alias: []
description: 'توثيق تنسيق Buffers'
input_format: true
keywords: ['Buffers']
output_format: true
slug: /interfaces/formats/Buffers
title: 'Buffers'
doc_type: 'reference'
---

| الإدخال | المخرجات | الاسم المستعار |
| ------- | -------- | -------------- |
| ✔       | ✔        |                |

<div id="description">
  ## الوصف
</div>

يُعد `Buffers` تنسيقًا ثنائيًا بسيطًا جدًا لتبادل البيانات **المؤقتة**، حيث يكون كلٌّ من المستهلك والمنتِج على دراية مسبقة بالمخطط وترتيب الأعمدة.

وعلى خلاف [Native](./Native.md)، فهو **لا** يخزن أسماء الأعمدة أو أنواعها أو أي بيانات وصفية إضافية.

في هذا التنسيق، تُكتب البيانات وتُقرأ على شكل [كتل](/ar/development/architecture#block) بتنسيق ثنائي. ويستخدم Buffers التمثيل الثنائي نفسه لكل عمود كما في تنسيق [Native](./Native.md)، كما يلتزم بإعدادات تنسيق Native نفسها.

لكل كتلة، يُكتب التسلسل التالي:

1. عدد الأعمدة (UInt64، little-endian).
2. عدد الصفوف (UInt64، little-endian).
3. لكل عمود:

* الحجم الإجمالي بالبايت لبيانات العمود المُسلسلة (UInt64، little-endian).
* بايتات بيانات العمود المُسلسلة، تمامًا كما في تنسيق [Native](./Native.md).

<div id="example-usage">
  ## مثال على الاستخدام
</div>

للكتابة إلى ملف:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

اقرأ مرة أخرى باستخدام أنواع أعمدة محددة صراحةً:

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

إذا كان لديك جدول بأنواع الأعمدة نفسها، يمكنك ملؤه مباشرةً:

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

افحص الجدول:

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
