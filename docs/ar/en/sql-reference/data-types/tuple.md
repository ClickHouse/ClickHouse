---
description: 'توثيق نوع البيانات Tuple في ClickHouse'
sidebar_label: 'Tuple(T1, T2, ...)'
sidebar_position: 34
slug: /sql-reference/data-types/tuple
title: 'Tuple(T1, T2, ...)'
doc_type: 'reference'
---

Tuple هو مجموعة من العناصر، لكل عنصر فيها [نوع](/ar/sql-reference/data-types) مستقل. ويجب أن يحتوي Tuple على عنصر واحد على الأقل.

تُستخدم Tuples لتجميع الأعمدة مؤقتًا. ويمكن تجميع الأعمدة عند استخدام تعبير IN في استعلام، وكذلك لتحديد بعض المعلمات الشكلية لدوال لامبدا. لمزيد من المعلومات، راجع القسمين [عوامل التشغيل IN](../../sql-reference/operators/in.md) و[الدوال عالية الرتبة](/ar/sql-reference/functions/overview#higher-order-functions).

يمكن أن تكون Tuples ناتج استعلام. في هذه الحالة، في تنسيقات النص بخلاف JSON، تُفصل القيم بفواصل داخل `()`. أما في تنسيقات JSON، فتُخرَج Tuples على شكل مصفوفات (داخل `[]`).

<div id="creating-tuples">
  ## إنشاء Tuples
</div>

يمكنك استخدام دالة لإنشاء Tuple:

```sql
tuple(T1, T2, ...)
```

مثال على إنشاء Tuple:

```sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

```text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

يمكن أن يتكوّن Tuple من عنصر واحد

مثال:

```sql
SELECT tuple('a') AS x;
```

```text
┌─x─────┐
│ ('a') │
└───────┘
```

يمكن استخدام البنية `(tuple_element1, tuple_element2)` لإنشاء tuple من عدة عناصر من دون استدعاء الدالة `tuple()`.

مثال:

```sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

```text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

<div id="data-type-detection">
  ## اكتشاف نوع البيانات
</div>

عند إنشاء قيم Tuple مباشرةً، يستنتج ClickHouse نوع وسيطات Tuple باعتباره أصغر الأنواع القادرة على احتواء قيمة الوسيطة المُقدَّمة. وإذا كانت القيمة هي [NULL](/ar/operations/settings/formats#input_format_null_as_default)، فسيكون النوع المستنتَج هو [Nullable](../../sql-reference/data-types/nullable.md).

مثال على الاكتشاف التلقائي لنوع البيانات:

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

```text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

<div id="referring-to-tuple-elements">
  ## الإشارة إلى عناصر Tuple
</div>

يمكن الإشارة إلى عناصر Tuple بالاسم أو برقم الفهرس:

```sql title="Query"
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- by name
SELECT a.2 FROM named_tuples; -- by index
```

```text title="Response"
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

<div id="comparison-operations-with-tuple">
  ## عمليات المقارنة باستخدام Tuple
</div>

تُقارَن قيمتا Tuple بمقارنة عناصرهما تسلسليًا من اليسار إلى اليمين. فإذا كان العنصر الأول في Tuple الأول أكبر (أصغر) من العنصر المناظر له في Tuple الثاني، عُدَّ Tuple الأول أكبر (أصغر) من الثاني. وإلا، إذا كان العنصران متساويين، فتُقارَن القيمة التالية.

مثال:

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

```text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

أمثلة واقعية:

```sql
CREATE TABLE test
(
    `year` Int16,
    `month` Int8,
    `day` Int8
)
ENGINE = Memory AS
SELECT *
FROM values((2022, 12, 31), (2000, 1, 1));

SELECT * FROM test;

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
│ 2000 │     1 │   1 │
└──────┴───────┴─────┘

SELECT *
FROM test
WHERE (year, month, day) > (2010, 1, 1);

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
└──────┴───────┴─────┘
CREATE TABLE test
(
    `key` Int64,
    `duration` UInt32,
    `value` Float64
)
ENGINE = Memory AS
SELECT *
FROM values((1, 42, 66.5), (1, 42, 70), (2, 1, 10), (2, 2, 0));

SELECT * FROM test;

┌─key─┬─duration─┬─value─┐
│   1 │       42 │  66.5 │
│   1 │       42 │    70 │
│   2 │        1 │    10 │
│   2 │        2 │     0 │
└─────┴──────────┴───────┘

-- Let's find a value for each key with the biggest duration, if durations are equal, select the biggest value

SELECT
    key,
    max(duration),
    argMax(value, (duration, value))
FROM test
GROUP BY key
ORDER BY key ASC;

┌─key─┬─max(duration)─┬─argMax(value, tuple(duration, value))─┐
│   1 │            42 │                                    70 │
│   2 │             2 │                                     0 │
└─────┴───────────────┴───────────────────────────────────────┘
```

<div id="nullable-tuple">
  ## Nullable(Tuple(T1, T2, ...))
</div>

:::note ميزة تجريبية
يتطلب `SET enable_nullable_tuple_type = 1`
هذه ميزة تجريبية.
:::

يسمح بأن تكون الـ Tuple بأكملها `NULL`، بخلاف `Tuple(Nullable(T1), Nullable(T2), ...)` حيث لا يمكن أن تكون `NULL` إلا العناصر الفردية.

| النوع                                      | يمكن أن تكون Tuple بقيمة NULL | يمكن أن تكون العناصر بقيمة NULL |
| ------------------------------------------ | ----------------------------- | ------------------------------- |
| `Nullable(Tuple(String, Int64))`           | ✅                             | ❌                               |
| `Tuple(Nullable(String), Nullable(Int64))` | ❌                             | ✅                               |

مثال:

```sql
SET enable_nullable_tuple_type = 1;

CREATE TABLE test (
    id UInt32,
    data Nullable(Tuple(String, Int64))
) ENGINE = Memory;

INSERT INTO test VALUES (1, ('hello', 42)), (2, NULL);

SELECT * FROM test WHERE data IS NULL;
```

```txt
 ┌─id─┬─data─┐
 │  2 │ ᴺᵁᴸᴸ │
 └────┴──────┘
```