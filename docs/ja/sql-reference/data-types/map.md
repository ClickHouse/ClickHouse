---
slug: /ja/sql-reference/data-types/map
sidebar_position: 36
sidebar_label: Map(K, V)
---

# Map(K, V)

データ型 `Map(K, V)` はキーと値のペアを格納します。

他のデータベースと異なり、ClickHouseのマップは一意ではありません。つまり、マップには同じキーを持つ2つの要素を含めることができます。
（その理由は、マップが内部的に `Array(Tuple(K, V))` として実装されているためです。）

マップ `m` でキー `k` の値を取得するには、構文 `m[k]` を使用できます。
また、`m[k]` はマップをスキャンするため、操作のランタイムはマップのサイズに対して線形です。

**パラメータ**

- `K` — マップキーの型。任意の型。ただし、[Nullable](../../sql-reference/data-types/nullable.md) および [LowCardinality](../../sql-reference/data-types/lowcardinality.md) が [Nullable](../../sql-reference/data-types/nullable.md) 型とネストされている場合を除く。
- `V` — マップ値の型。任意の型。

**例**

マップ型のカラムを持つテーブルを作成します：

``` sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

`key2` の値を選択します：

```sql
SELECT m['key2'] FROM tab;
```

結果：

```text
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

要求されたキー `k` がマップに含まれていない場合、`m[k]` は型のデフォルト値、たとえば整数型の場合は `0`、文字列型の場合は `''` を返します。
マップにキーが存在するかどうかを確認するには、関数 [mapContains](../../sql-reference/functions/tuple-map-functions#mapcontains) を使用できます。

```sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

結果：

```text
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

## Tuple を Map に変換する

`Tuple()` 型の値を [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) 関数を使用して `Map()` 型の値にキャストできます：

**例**

クエリ：

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

結果：

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

## マップのサブカラムを読み取る

マップ全体を読み取るのを避けるために、場合によってはサブカラム `keys` と `values` を使用できます。

**例**

クエリ：

``` sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   mapKeys(m) と同じ
SELECT m.values FROM tab; -- mapValues(m) と同じ
```

結果：

``` text
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

**関連項目**

- [map()](../../sql-reference/functions/tuple-map-functions.md#function-map) 関数
- [CAST()](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) 関数
- [-Map データ型用のコンビネータ](../aggregate-functions/combinators.md#-map)


## 関連コンテンツ

- ブログ: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

