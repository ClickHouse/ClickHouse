---
slug: /ja/sql-reference/aggregate-functions/reference/corrmatrix
sidebar_position: 118
---

# corrMatrix

N個の変数に対する相関行列を計算します。

**構文**

```sql
corrMatrix(x[, ...])
```

**引数**

- `x` — 可変個のパラメータ。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md)。

**返される値**

- 相関行列。 [Array](../../data-types/array.md)([Array](../../data-types/array.md)([Float64](../../data-types/float.md)))。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a UInt32,
    b Float64,
    c Float64,
    d Float64
)
ENGINE = Memory;
INSERT INTO test(a, b, c, d) VALUES (1, 5.6, -4.4, 2.6), (2, -9.6, 3, 3.3), (3, -1.3, -4, 1.2), (4, 5.3, 9.7, 2.3), (5, 4.4, 0.037, 1.222), (6, -8.6, -7.8, 2.1233), (7, 5.1, 9.3, 8.1222), (8, 7.9, -3.6, 9.837), (9, -8.2, 0.62, 8.43555), (10, -3, 7.3, 6.762);
```

```sql
SELECT arrayMap(x -> round(x, 3), arrayJoin(corrMatrix(a, b, c, d))) AS corrMatrix
FROM test;
```

結果:

```response
   ┌─corrMatrix─────────────┐
1. │ [1,-0.096,0.243,0.746] │
2. │ [-0.096,1,0.173,0.106] │
3. │ [0.243,0.173,1,0.258]  │
4. │ [0.746,0.106,0.258,1]  │
   └────────────────────────┘
```
