---
slug: /ja/sql-reference/aggregate-functions/reference/maxmap
sidebar_position: 165
---

# maxMap

`key` 配列で指定されたキーに基づいて、`value` 配列からの最大値を計算します。

**構文**

```sql
maxMap(key, value)
```
または
```sql
maxMap(Tuple(key, value))
```

エイリアス: `maxMappedArrays`

:::note
- キーと値の配列のタプルを渡すことは、2つの配列（キーと値）を渡すことと同じです。
- `key` と `value` の要素数は、合計される各行で同じでなければなりません。
:::

**パラメータ**

- `key` — キーの配列。 [Array](../../data-types/array.md)。
- `value` — 値の配列。 [Array](../../data-types/array.md)。

**返される値**

- ソート順のキーの配列と、それに対応するキーに対する計算値の配列を持つタプルを返します。 [Tuple](../../data-types/tuple.md)([Array](../../data-types/array.md), [Array](../../data-types/array.md))。

**例**

クエリ:

``` sql
SELECT maxMap(a, b)
FROM values('a Array(Char), b Array(Int64)', (['x', 'y'], [2, 2]), (['y', 'z'], [3, 1]))
```

結果:

``` text
┌─maxMap(a, b)───────────┐
│ [['x','y','z'],[2,3,1]]│
└────────────────────────┘
```
