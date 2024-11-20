---
title: "varPop"
slug: "/ja/sql-reference/aggregate-functions/reference/varPop"
sidebar_position: 210
---

## varPop

母分散を計算します。

**構文**

```sql
varPop(x)
```

エイリアス: `VAR_POP`.

**パラメーター**

- `x`: 母分散を求める値の集まり。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md)。

**返される値**

- `x` の母分散を返します。[`Float64`](../../data-types/float.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x UInt8,
)
ENGINE = Memory;

INSERT INTO test_data VALUES (3), (3), (3), (4), (4), (5), (5), (7), (11), (15);

SELECT
    varPop(x) AS var_pop
FROM test_data;
```

結果:

```response
┌─var_pop─┐
│    14.4 │
└─────────┘
```
