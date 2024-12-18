---
slug: /ja/sql-reference/aggregate-functions/reference/sum
sidebar_position: 195
---

# sum

合計を計算します。数値に対してのみ機能します。

**構文**

```sql
sum(num)
```

**パラメータ**
- `num`: 数値のカラム。[(U)Int*](../../data-types/int-uint.md)、[Float*](../../data-types/float.md)、[Decimal*](../../data-types/decimal.md)。

**戻り値**

- 値の合計。[(U)Int*](../../data-types/int-uint.md)、[Float*](../../data-types/float.md)、[Decimal*](../../data-types/decimal.md)。

**例**

まず、`employees`というテーブルを作成し、いくつかの架空の従業員データを挿入します。

クエリ:

```sql
CREATE TABLE employees
(
    `id` UInt32,
    `name` String,
    `salary` UInt32
)
ENGINE = Log
```

```sql
INSERT INTO employees VALUES
    (87432, 'John Smith', 45680),
    (59018, 'Jane Smith', 72350),
    (20376, 'Ivan Ivanovich', 58900),
    (71245, 'Anastasia Ivanovna', 89210);
```

`sum`関数を使用して、従業員の給与の合計を求めます。

クエリ:

```sql
SELECT sum(salary) FROM employees;
```

結果:

```response
   ┌─sum(salary)─┐
1. │      266140 │
   └─────────────┘
```
