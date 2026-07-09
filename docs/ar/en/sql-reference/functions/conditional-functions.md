---
description: 'وثائق الدوال الشرطية'
sidebar_label: 'الشرطية'
slug: /sql-reference/functions/conditional-functions
title: 'الدوال الشرطية'
doc_type: 'reference'
---

<div id="overview">
  ## نظرة عامة
</div>

<div id="using-conditional-results-directly">
  ### استخدام النتائج الشرطية مباشرةً
</div>

تُرجِع التعبيرات الشرطية دائمًا `0` أو `1` أو `NULL`. لذا يمكنك استخدام النتائج الشرطية مباشرةً كما يلي:

```sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

<div id="null-values-in-conditionals">
  ### قيم `NULL` في التعبير الشرطي
</div>

عند استخدام قيم `NULL` في التعبير الشرطي، تكون النتيجة أيضًا `NULL`.

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

لذا ينبغي أن تُنشئ استعلاماتك بعناية إذا كانت الأنواع `Nullable`.

ويوضح المثال التالي ذلك بعدم إضافة شرط equals إلى `multiIf`.

```sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

<div id="case-statement">
  ### جملة CASE
</div>

يوفّر تعبير CASE في ClickHouse منطقًا شرطيًا مشابهًا لمعامل CASE في SQL. فهو يقيّم الشروط ويُرجع القيم وفقًا لأول شرط متطابق.

يدعم ClickHouse شكلين من CASE:

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br />
   يتيح هذا الشكل مرونة كاملة، ويُنفَّذ داخليًا باستخدام الدالة [multiIf](/ar/sql-reference/functions/conditional-functions#multiIf). ويُقيَّم كل شرط بشكل مستقل، ويمكن أن تتضمن التعبيرات قيمًا غير ثابتة.

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br />
   هذا الشكل الأكثر اختصارًا مُحسَّن لمطابقة القيم الثابتة، ويستخدم داخليًا `caseWithExpression()`.

على سبيل المثال، ما يلي صحيح:

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

هذا النمط أيضًا لا يشترط أن تكون تعبيرات الإرجاع ثوابت.

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

<div id="caveats">
  #### محاذير
</div>

يحدّد ClickHouse نوع نتيجة تعبير `CASE` (أو ما يعادله داخليًا، مثل `multiIf`) قبل تقييم أي شرط. وتبرز أهمية ذلك عندما تختلف تعبيرات الإرجاع في النوع، مثل اختلاف المناطق الزمنية أو الأنواع الرقمية.

* يُختار نوع النتيجة بناءً على أكبر نوع متوافق بين جميع الفروع.
* وبمجرد اختيار هذا النوع، تُحوَّل جميع الفروع الأخرى إليه ضمنيًا، حتى إن كان منطقها لن يُنفَّذ مطلقًا وقت التشغيل.
* بالنسبة إلى أنواع مثل DateTime64، حيث تكون المنطقة الزمنية جزءًا من تعريف النوع، فقد يؤدي ذلك إلى سلوك غير متوقع: إذ قد تُستخدم أول منطقة زمنية تتم مصادفتها في جميع الفروع، حتى عندما تحدد فروع أخرى مناطق زمنية مختلفة.

على سبيل المثال، في المثال أدناه تُرجِع جميع الصفوف الطابع الزمني وفق المنطقة الزمنية لأول فرع مطابق، أي `Asia/Kolkata`

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

هنا يتعرّف ClickHouse على عدة أنواع إرجاع `DateTime64(3, <timezone>)`. ويستنتج أن النوع المشترك هو `DateTime64(3, 'Asia/Kolkata'` لأنه أول نوع يصادفه، مع تحويل الفروع الأخرى ضمنيًا إلى هذا النوع.

يمكن معالجة ذلك بالتحويل إلى سلسلة نصية للحفاظ على تنسيق المنطقة الزمنية المطلوب:

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

{/* 
  يُستبدل المحتوى الداخلي للوسوم أدناه، في وقت build لإطار عمل التوثيق، بـ
  الوثائق المُولَّدة من system.functions. يُرجى عدم تعديل هذه الوسوم أو إزالتها.
  راجع: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }