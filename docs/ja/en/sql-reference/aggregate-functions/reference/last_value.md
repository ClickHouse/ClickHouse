---
description: '最後に現れた値を選択します。`anyLast` に似ていますが、NULL も受け入れます。'
slug: /sql-reference/aggregate-functions/reference/last_value
title: 'last_value'
doc_type: 'reference'
---

最後に現れた値を選択します。`anyLast` に似ていますが、NULL も受け入れます。
主に [ウィンドウ関数](../../window-functions/index.md) と組み合わせて使用します。
ウィンドウ関数を使用しない場合、元のストリームに順序がないと結果はランダムになります。

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

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null)
```

<div id="example1">
  ### 例 1
</div>

デフォルトでは、NULL 値は無視されます。

```sql
SELECT last_value(b) FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example2">
  ### 例 2
</div>

NULL 値は無視されます。

```sql
SELECT last_value(b) ignore nulls FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example3">
  ### 例 3
</div>

NULL 値を受け付けます。

```sql
SELECT last_value(b) respect nulls FROM test_data
```

```text
┌─last_value_respect_nulls(b)─┐
│                        ᴺᵁᴸᴸ │
└─────────────────────────────┘
```

<div id="example4">
  ### 例 4
</div>

サブクエリで `ORDER BY` を使用して結果を安定化する。

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