---
description: 'هو اسم مستعار لـ any، لكنه أُضيف للتوافق مع
  دوال window، إذ يلزم أحيانًا معالجة قيم `NULL` (فجميع
  الدوال التجميعية في ClickHouse تتجاهل قيم NULL افتراضيًا).'
slug: /sql-reference/aggregate-functions/reference/first_value
title: 'first_value'
doc_type: 'reference'
---

هو اسم مستعار لـ [`any`](../../../sql-reference/aggregate-functions/reference/any.md)، لكنه أُضيف للتوافق مع [دوال window](../../window-functions/index.md)، إذ يلزم أحيانًا معالجة قيم `NULL` (فجميع الدوال التجميعية في ClickHouse تتجاهل قيم NULL افتراضيًا).

ويدعم تعريف مُعدِّل لمراعاة قيم NULL (`RESPECT NULLS`)، سواء ضمن [دوال window](../../window-functions/index.md) أو في عمليات التجميع العادية.

وكما هو الحال مع `any`، فبدون دوال window ستكون النتيجة عشوائية إذا لم يكن دفق الإدخال مرتبًا، ويطابق نوع الإرجاع
نوع الإدخال (ولا تُعاد القيمة Null إلا إذا كان الإدخال من النوع Nullable أو أُضيف المُركِّب ‎-OrNull).

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

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null);
```

<div id="example1">
  ### المثال 1
</div>

بشكل افتراضي، تُتجاهَل قيمة NULL.

```sql
SELECT first_value(b) FROM test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

<div id="example2">
  ### مثال 2
</div>

يتم تجاهل القيمة NULL.

```sql
SELECT first_value(b) ignore nulls FROM test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

<div id="example3">
  ### مثال 3
</div>

تُقبل قيمة NULL.

```sql
SELECT first_value(b) respect nulls FROM test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

<div id="example4">
  ### مثال 4
</div>

الحصول على نتيجة مستقرة باستخدام استعلام فرعي مع `ORDER BY`.

```sql
SELECT
    first_value_respect_nulls(b),
    first_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─any_respect_nulls(b)─┬─any(b)─┐
│                 ᴺᵁᴸᴸ │      3 │
└──────────────────────┴────────┘
```