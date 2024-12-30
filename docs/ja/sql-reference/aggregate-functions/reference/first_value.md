---
slug: /ja/sql-reference/aggregate-functions/reference/first_value
sidebar_position: 137
---

# first_value

これは、[`any`](../../../sql-reference/aggregate-functions/reference/any.md)のエイリアスですが、[ウィンドウ関数](../../window-functions/index.md)との互換性のために導入されました。ここでは、`NULL`値を処理する必要がある場合があります（デフォルトでは、すべてのClickHouse集計関数はNULL値を無視します）。

ウィンドウ関数および通常の集計の両方でNULLを尊重する修飾子（`RESPECT NULLS`）を宣言することをサポートしています。

`any`と同様に、ウィンドウ関数なしではソースストリームが注文されていない場合、結果はランダムになり、戻りの型は入力の型と一致します（入力がNullableまたは-OrNullコンビネータが追加されている場合のみNullが返されます）。

## 例

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) Values (1,null), (2,3), (4, 5), (6,null);
```

### 例1
デフォルトでは、NULL値は無視されます。
```sql
select first_value(b) from test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

### 例2
NULL値は無視されます。
```sql
select first_value(b) ignore nulls from test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

### 例3
NULL値が受け入れられます。
```sql
select first_value(b) respect nulls from test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

### 例4
`ORDER BY`を使用したサブクエリで安定した結果。
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
