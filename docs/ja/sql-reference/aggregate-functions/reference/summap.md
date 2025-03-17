---
slug: /ja/sql-reference/aggregate-functions/reference/summap
sidebar_position: 198
---

# sumMap

`key` 配列で指定されたキーに従って `value` 配列を合計します。ソートされた順のキーと、対応するキーの合計値をオーバーフローせずに返す2つの配列のタプルを返します。

**構文**

- `sumMap(key <Array>, value <Array>)` [配列型](../../data-types/array.md)。
- `sumMap(Tuple(key <Array>, value <Array>))` [タプル型](../../data-types/tuple.md)。

エイリアス: `sumMappedArrays`。

**引数**

- `key`: キーの[配列](../../data-types/array.md)。
- `value`: 値の[配列](../../data-types/array.md)。

キーと値の配列のタプルを渡すことは、キーの配列と値の配列を個別に渡すことと同義です。

:::note
`key` と `value` の要素数は合計される各行で同じでなければなりません。
:::

**戻り値** 

- ソートされた順のキーと、対応するキーの合計値を含む2つの配列のタプルを返します。

**例**

まず、`sum_map` というテーブルを作成し、いくつかのデータを挿入します。キーの配列と値の配列は、[Nested](../../data-types/nested-data-structures/index.md) 型の `statusMap` というカラムに個別に保存され、この関数の2つの異なる構文の使用方法を説明するために、[タプル](../../data-types/tuple.md) 型の `statusMapTuple` というカラムに一緒に保存されています。

クエリ:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Log;
```
```sql
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));
```

次に、テーブルに対して `sumMap` 関数を使い、配列とタプル型の構文の両方を利用してクエリを行います:

クエリ:

``` sql
SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

結果:

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

**関連情報**

- [Map データ型のための Map コンビネーター](../combinators.md#-map)
- [sumMapWithOverflow](../reference/summapwithoverflow.md)
