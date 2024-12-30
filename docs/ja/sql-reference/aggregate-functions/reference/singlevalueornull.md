---
slug: /ja/sql-reference/aggregate-functions/reference/singlevalueornull
sidebar_position: 184
---

# singleValueOrNull

集計関数 `singleValueOrNull` は、`x = ALL (SELECT ...)` のようなサブクエリオペレーターを実装するために使用されます。データ内に一意の非NULL値が唯一あるかどうかを確認します。
もし一意の値が一つだけある場合、その値を返します。ゼロまたは少なくとも二つ以上の異なる値がある場合、NULLを返します。

**構文**

``` sql
singleValueOrNull(x)
```

**パラメーター**

- `x` — 任意の[データ型](../../data-types/index.md)（[Map](../../data-types/map.md)、[Array](../../data-types/array.md)、または[Tuple](../../data-types/tuple)で[Nullable](../../data-types/nullable.md)型になれないものを除く）のカラム。

**返される値**

- 一意の非NULL値が一つだけある場合、その値。
- ゼロまたは少なくとも二つ以上の異なる値がある場合、`NULL`。

**例**

クエリ:

``` sql
CREATE TABLE test (x UInt8 NULL) ENGINE=Log;
INSERT INTO test (x) VALUES (NULL), (NULL), (5), (NULL), (NULL);
SELECT singleValueOrNull(x) FROM test;
```

結果:

```response
┌─singleValueOrNull(x)─┐
│                    5 │
└──────────────────────┘
```

クエリ:

```sql
INSERT INTO test (x) VALUES (10);
SELECT singleValueOrNull(x) FROM test;
```

結果:

```response
┌─singleValueOrNull(x)─┐
│                 ᴺᵁᴸᴸ │
└──────────────────────┘
```

