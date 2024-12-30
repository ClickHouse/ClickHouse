---
slug: /ja/sql-reference/aggregate-functions/reference/minmap
sidebar_position: 169
---

# minMap

`key` 配列で指定されたキーに基づいて、`value` 配列から最小値を計算します。

**構文**

```sql
`minMap(key, value)`
```
または
```sql
minMap(Tuple(key, value))
```

別名: `minMappedArrays`

:::note
- キーのタプルと値の配列を渡すことは、キーの配列と値の配列を渡すことと同じです。
- 合計される各行で、`key` と `value` の要素数は同じでなければなりません。
:::

**パラメータ**

- `key` — キーの配列。[Array](../../data-types/array.md)。
- `value` — 値の配列。[Array](../../data-types/array.md)。

**返される値**

- ソート順に並べられたキーと、それに対応するキーのために計算された値の2つの配列を持つタプルを返します。[Tuple](../../data-types/tuple.md)([Array](../../data-types/array.md), [Array](../../data-types/array.md))。

**例**

クエリ:

``` sql
SELECT minMap(a, b)
FROM values('a Array(Int32), b Array(Int64)', ([1, 2], [2, 2]), ([2, 3], [1, 1]))
```

結果:

``` text
┌─minMap(a, b)──────┐
│ ([1,2,3],[2,1,1]) │
└───────────────────┘
```
