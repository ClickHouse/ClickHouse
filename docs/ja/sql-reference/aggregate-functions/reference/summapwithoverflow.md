---
slug: /ja/sql-reference/aggregate-functions/reference/summapwithoverflow
sidebar_position: 199
---

# sumMapWithOverflow

`key` 配列で指定されたキーに基づいて `value` 配列の合計を求めます。ソートされた順序でのキーと、それに対応するキーの合計値の二つの配列のタプルを返します。この関数は [sumMap](../reference/summap.md) 関数と異なり、オーバーフローによる加算を行います。つまり、引数のデータ型と同じデータ型で結果を返します。

**構文**

- `sumMapWithOverflow(key <Array>, value <Array>)` [Array 型](../../data-types/array.md)。
- `sumMapWithOverflow(Tuple(key <Array>, value <Array>))` [Tuple 型](../../data-types/tuple.md)。

**引数**

- `key`: [Array](../../data-types/array.md) 型のキー。
- `value`: [Array](../../data-types/array.md) 型の値。

キーと値の配列のタプルを渡すことは、キーの配列と値の配列を別々に渡すことと同義です。

:::note 
合計される各行において、`key` と `value` の要素数は同じでなければなりません。
:::

**戻り値**

- ソートされた順序でのキーと、それに対応するキーの合計値の二つの配列のタプルを返します。

**例**

まず、`sum_map` というテーブルを作成し、そこにデータを挿入します。キーと値の配列は [Nested](../../data-types/nested-data-structures/index.md) 型の `statusMap` というカラムと、[tuple](../../data-types/tuple.md) 型の `statusMapTuple` というカラムに分けて保存されます。この例では、上記のこの関数の二つの異なる構文の使用方法を示します。

クエリ:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt8,
        requests UInt8
    ),
    statusMapTuple Tuple(Array(Int8), Array(Int8))
) ENGINE = Log;
```
```sql
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));
```

`sumMap`、`sumMapWithOverflow` の配列型構文、および `toTypeName` 関数を使用してテーブルをクエリすると、`sumMapWithOverflow` 関数の場合、合計された値の配列のデータ型が引数の型と同じ `UInt8` であることがわかります（つまり、オーバーフローを伴う加算が行われました）。一方、`sumMap` では、合計された値の配列のデータ型が `UInt8` から `UInt64` に変更され、オーバーフローが発生しないようになっています。

クエリ:

``` sql
SELECT
    timeslot,
    toTypeName(sumMap(statusMap.status, statusMap.requests)),
    toTypeName(sumMapWithOverflow(statusMap.status, statusMap.requests)),
FROM sum_map
GROUP BY timeslot
```

同じ結果を得るために、タプル構文を使うこともできました。

``` sql
SELECT
    timeslot,
    toTypeName(sumMap(statusMapTuple)),
    toTypeName(sumMapWithOverflow(statusMapTuple)),
FROM sum_map
GROUP BY timeslot
```

結果:

``` text
   ┌────────────timeslot─┬─toTypeName(sumMap(statusMap.status, statusMap.requests))─┬─toTypeName(sumMapWithOverflow(statusMap.status, statusMap.requests))─┐
1. │ 2000-01-01 00:01:00 │ Tuple(Array(UInt8), Array(UInt64))                       │ Tuple(Array(UInt8), Array(UInt8))                                    │
2. │ 2000-01-01 00:00:00 │ Tuple(Array(UInt8), Array(UInt64))                       │ Tuple(Array(UInt8), Array(UInt8))                                    │
   └─────────────────────┴──────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────┘
```

**関連項目**
    
- [sumMap](../reference/summap.md)
