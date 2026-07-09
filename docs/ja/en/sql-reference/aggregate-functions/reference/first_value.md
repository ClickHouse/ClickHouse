---
description: '`any` の別名ですが、[Window Functions](../../window-functions/index.md) との互換性のために導入されました。Window Functions では、`NULL` 値を処理する必要がある場合があるためです（デフォルトでは、ClickHouse のすべての集約関数は NULL 値を無視します）。'
slug: /sql-reference/aggregate-functions/reference/first_value
title: 'first_value'
doc_type: 'reference'
---

[`any`](../../../sql-reference/aggregate-functions/reference/any.md) の別名ですが、[Window Functions](../../window-functions/index.md) との互換性のために導入されました。Window Functions では、`NULL` 値を処理する必要がある場合があるためです (デフォルトでは、ClickHouse のすべての集約関数は NULL 値を無視します) 。

[Window Functions](../../window-functions/index.md) と通常の集計の両方で、NULL を尊重する修飾子 (`RESPECT NULLS`) を指定できます。

`any` と同様に、Window Functions を使用しない場合、入力ストリームが順序付けされていなければ結果はランダムになり、戻り値の型は入力型と一致します
 (`NULL` が返されるのは、入力が Nullable であるか、-OrNull combinator が追加されている場合のみです) 。

<div id="examples">
  ## 例
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
  ### 例 1
</div>

デフォルトでは、NULL 値は無視されます。

```sql
SELECT first_value(b) FROM test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

<div id="example2">
  ### 例 2
</div>

NULL 値は無視されます。

```sql
SELECT first_value(b) ignore nulls FROM test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

<div id="example3">
  ### 例 3
</div>

NULL 値が許容されます。

```sql
SELECT first_value(b) respect nulls FROM test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

<div id="example4">
  ### 例 4
</div>

`ORDER BY` を使用したサブクエリによる安定した結果。

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