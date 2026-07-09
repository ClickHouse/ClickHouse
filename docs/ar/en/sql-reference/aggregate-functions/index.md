---
description: 'توثيق الدوال التجميعية'
sidebar_label: 'الدوال التجميعية'
sidebar_position: 33
slug: /sql-reference/aggregate-functions/
title: 'الدوال التجميعية'
doc_type: 'reference'
---

تعمل الدوال التجميعية بالطريقة [الاعتيادية](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) كما يتوقعها خبراء قواعد البيانات.

كما يدعم ClickHouse ما يلي:

* [الدوال التجميعية المعلمية](/ar/sql-reference/aggregate-functions/parametric-functions)، التي تقبل معلمات أخرى بالإضافة إلى الأعمدة.
* [المُعدِّلات](/ar/sql-reference/aggregate-functions/combinators)، التي تغيّر سلوك الدوال التجميعية.

<div id="null-processing">
  ## معالجة NULL
</div>

أثناء التجميع، تُتخطّى جميع الوسيطات ذات القيمة `NULL`. وإذا كان للتجميع عدة وسيطات، فإنه يتجاهل أي صف تكون فيه واحدة أو أكثر منها `NULL`.

هناك استثناء لهذه القاعدة، ويتمثل في الدالتين [`first_value`](../../sql-reference/aggregate-functions/reference/first_value.md) و[`last_value`](../../sql-reference/aggregate-functions/reference/last_value.md) والأسماء البديلة لهما (`any` و`anyLast` على الترتيب) عند اتباعهما بالمُعدِّل `RESPECT NULLS`. على سبيل المثال: `FIRST_VALUE(b) RESPECT NULLS`.

**أمثلة:**

لنأخذ هذا الجدول:

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

لنفترض أنك تريد جمع القيم في العمود `y`:

```sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

الآن يمكنك استخدام الدالة `groupArray` لإنشاء مصفوفة من العمود `y`:

```sql
SELECT groupArray(y) FROM t_null_big
```

```text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

لا تتضمن `groupArray` القيمة `NULL` في المصفوفة الناتجة.

يمكنك استخدام [COALESCE](../../sql-reference/functions/functions-for-nulls.md#coalesce) لتحويل `NULL` إلى قيمة مناسبة لحالة الاستخدام لديك. على سبيل المثال: سيستخدم ‎`avg(COALESCE(column, 0))`‎ قيمة العمود في التجميع، أو الصفر إذا كانت `NULL`:

```sql
SELECT
    avg(y),
    avg(coalesce(y, 0))
FROM t_null_big
```

```text
┌─────────────avg(y)─┬─avg(coalesce(y, 0))─┐
│ 2.3333333333333335 │                 1.4 │
└────────────────────┴─────────────────────┘
```

يمكنك أيضًا استخدام [Tuple](/ar/sql-reference/data-types/tuple.md) لتجاوز سلوك تخطي NULL. فـ `Tuple` الذي يحتوي فقط على قيمة `NULL` لا يُعد `NULL`، لذلك لن تتخطى الدوال التجميعية ذلك الصف بسبب قيمة `NULL` هذه.

```sql
SELECT
    groupArray(y),
    groupArray(tuple(y)).1
FROM t_null_big;

┌─groupArray(y)─┬─tupleElement(groupArray(tuple(y)), 1)─┐
│ [2,2,3]       │ [2,NULL,2,3,NULL]                     │
└───────────────┴───────────────────────────────────────┘
```

لاحظ أن عمليات التجميع لا تُطبَّق عندما تُستخدم الأعمدة كوسائط لدالة تجميعية.  على سبيل المثال، فإن [`count`](../../sql-reference/aggregate-functions/reference/count.md) من دون معلمات (`count()`) أو مع معلمات ثابتة (`count(1)`) سيحسب جميع الصفوف في الكتلة (بغض النظر عن قيمة عمود GROUP BY لأنه ليس وسيطًا)، بينما `count(column)` لن يُرجع إلا عدد الصفوف التي لا تكون فيها قيمة `column` هي NULL.

```sql
SELECT
    v,
    count(1),
    count(v)
FROM
(
    SELECT if(number < 10, NULL, number % 3) AS v
    FROM numbers(15)
)
GROUP BY v

┌────v─┬─count()─┬─count(v)─┐
│ ᴺᵁᴸᴸ │      10 │        0 │
│    0 │       1 │        1 │
│    1 │       2 │        2 │
│    2 │       2 │        2 │
└──────┴─────────┴──────────┘
```

وهنا مثال على first&#95;value مع `RESPECT NULLS`، حيث نرى أن قيم NULL المُدخلة تُراعى، وستُرجِع أول قيمة تمت قراءتها، سواء كانت NULL أم لا:

```sql
SELECT
    col || '_' || ((col + 1) * 5 - 1) AS range,
    first_value(odd_or_null) AS first,
    first_value(odd_or_null) IGNORE NULLS as first_ignore_null,
    first_value(odd_or_null) RESPECT NULLS as first_respect_nulls
FROM
(
    SELECT
        intDiv(number, 5) AS col,
        if(number % 2 == 0, NULL, number) AS odd_or_null
    FROM numbers(15)
)
GROUP BY col
ORDER BY col

┌─range─┬─first─┬─first_ignore_null─┬─first_respect_nulls─┐
│ 0_4   │     1 │                 1 │                ᴺᵁᴸᴸ │
│ 1_9   │     5 │                 5 │                   5 │
│ 2_14  │    11 │                11 │                ᴺᵁᴸᴸ │
└───────┴───────┴───────────────────┴─────────────────────┘
```