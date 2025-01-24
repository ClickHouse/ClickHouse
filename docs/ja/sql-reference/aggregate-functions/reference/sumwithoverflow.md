---
slug: /ja/sql-reference/aggregate-functions/reference/sumwithoverflow
sidebar_position: 200
---

# sumWithOverflow

数値の合計を計算し、結果には入力パラメータと同じデータ型を使用します。このデータ型の最大値を超える場合は、オーバーフロー計算を行います。

数値に対してのみ動作します。

**構文**

```sql
sumWithOverflow(num)
```

**パラメータ**
- `num`: 数値のカラム。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md)。

**返される値**

- 値の合計。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md)。

**例**

まず、`employees` というテーブルを作成し、いくつかの架空の従業員データを挿入します。この例では、合計がオーバーフローを引き起こす可能性のある `UInt16` 型の `salary` を選択します。

クエリ:

```sql
CREATE TABLE employees
(
    `id` UInt32,
    `name` String,
    `monthly_salary` UInt16
)
ENGINE = Log
```

```sql
SELECT
    sum(monthly_salary) AS no_overflow,
    sumWithOverflow(monthly_salary) AS overflow,
    toTypeName(no_overflow),
    toTypeName(overflow)
FROM employees
```

`sum` と `sumWithOverflow` 関数を使用して従業員の給料の合計をクエリし、`toTypeName` 関数を使用してその型を表示します。`sum` 関数では結果の型は `UInt64` となり、合計を収めるのに十分なサイズです。一方、`sumWithOverflow` の結果の型は `UInt16` のままです。

クエリ:

```sql
SELECT 
    sum(monthly_salary) AS no_overflow,
    sumWithOverflow(monthly_salary) AS overflow,
    toTypeName(no_overflow),
    toTypeName(overflow),    
FROM employees;
```

結果:

```response
   ┌─no_overflow─┬─overflow─┬─toTypeName(no_overflow)─┬─toTypeName(overflow)─┐
1. │      118700 │    53164 │ UInt64                  │ UInt16               │
   └─────────────┴──────────┴─────────────────────────┴──────────────────────┘
```
