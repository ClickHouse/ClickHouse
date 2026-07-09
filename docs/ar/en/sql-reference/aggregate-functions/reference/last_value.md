---
description: 'تُرجِع آخر قيمة تمت مصادفتها، على غرار `anyLast`، ولكنها تقبل
  NULL.'
slug: /sql-reference/aggregate-functions/reference/last_value
title: 'last_value'
doc_type: 'reference'
---

تُرجِع آخر قيمة تمت مصادفتها، على غرار `anyLast`، ولكنها تقبل NULL.
في معظم الحالات، ينبغي استخدامها مع [دوال النافذة](../../window-functions/index.md).
من دون دوال النافذة، ستكون النتيجة عشوائية إذا لم يكن تدفق البيانات المصدر مرتبًا.

<div id="examples">
  ## أمثلة
</div>

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null)
```

<div id="example1">
  ### المثال 1
</div>

تُتجاهَل القيمة NULL بشكل افتراضي.

```sql
SELECT last_value(b) FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example2">
  ### المثال 2
</div>

تُتجاهَل قيمة NULL.

```sql
SELECT last_value(b) ignore nulls FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example3">
  ### مثال 3
</div>

تُقبل القيمة NULL.

```sql
SELECT last_value(b) respect nulls FROM test_data
```

```text
┌─last_value_respect_nulls(b)─┐
│                        ᴺᵁᴸᴸ │
└─────────────────────────────┘
```

<div id="example4">
  ### المثال 4
</div>

الحصول على نتيجة مستقرة باستخدام الاستعلام الفرعي مع `ORDER BY`.

```sql
SELECT
    last_value_respect_nulls(b),
    last_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─last_value_respect_nulls(b)─┬─last_value(b)─┐
│                        ᴺᵁᴸᴸ │             5 │
└─────────────────────────────┴───────────────┘
```