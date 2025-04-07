---
slug: /ja/sql-reference/functions/tuple-map-functions
sidebar_position: 120
sidebar_label: マップ
---

## map

キーと値のペアから[Map(key, value)](../data-types/map.md)型の値を作成します。

**構文**

```sql
map(key1, value1[, key2, value2, ...])
```

**引数**

- `key_n` — マップエントリのキー。[Map](../data-types/map.md)のキー型としてサポートされている任意の型。
- `value_n` — マップエントリの値。[Map](../data-types/map.md)の値型としてサポートされている任意の型。

**返される値**

- `key:value`ペアを含むマップ。[Map(key, value)](../data-types/map.md)。

**例**

クエリ:

```sql
SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);
```

結果:

``` text
┌─map('key1', number, 'key2', multiply(number, 2))─┐
│ {'key1':0,'key2':0}                              │
│ {'key1':1,'key2':2}                              │
│ {'key1':2,'key2':4}                              │
└──────────────────────────────────────────────────┘
```

## mapFromArrays

キーの配列またはマップと値の配列またはマップからマップを作成します。

この関数は、構文 `CAST([...], 'Map(key_type, value_type)')` の便利な代替です。
例えば、以下のように書く代わりに
- `CAST((['aa', 'bb'], [4, 5]), 'Map(String, UInt32)')`、または
- `CAST([('aa',4), ('bb',5)], 'Map(String, UInt32)')`

`mapFromArrays(['aa', 'bb'], [4, 5])` と書くことができます。

**構文**

```sql
mapFromArrays(keys, values)
```

別名: `MAP_FROM_ARRAYS(keys, values)`

**引数**

- `keys` —  キーの配列またはマップ [Array](../data-types/array.md) または [Map](../data-types/map.md)。`keys`が配列の場合は、その型として `Array(Nullable(T))` または `Array(LowCardinality(Nullable(T)))` を許容しますが、NULL値は含めません。
- `values` — 値の配列またはマップ [Array](../data-types/array.md) または [Map](../data-types/map.md)。

**返される値**

- キー配列と値配列/マップから構成されたキーと値を持つマップ。

**例**

クエリ:

```sql
select mapFromArrays(['a', 'b', 'c'], [1, 2, 3])
```

結果:

```
┌─mapFromArrays(['a', 'b', 'c'], [1, 2, 3])─┐
│ {'a':1,'b':2,'c':3}                       │
└───────────────────────────────────────────┘
```

`mapFromArrays` は [Map](../data-types/map.md) 型の引数も受け入れます。これらは実行中にタプルの配列にキャストされます。

```sql
SELECT mapFromArrays([1, 2, 3], map('a', 1, 'b', 2, 'c', 3))
```

結果:

```
┌─mapFromArrays([1, 2, 3], map('a', 1, 'b', 2, 'c', 3))─┐
│ {1:('a',1),2:('b',2),3:('c',3)}                       │
└───────────────────────────────────────────────────────┘
```

```sql
SELECT mapFromArrays(map('a', 1, 'b', 2, 'c', 3), [1, 2, 3])
```

結果:

```
┌─mapFromArrays(map('a', 1, 'b', 2, 'c', 3), [1, 2, 3])─┐
│ {('a',1):1,('b',2):2,('c',3):3}                       │
└───────────────────────────────────────────────────────┘
```

## extractKeyValuePairs

キーと値のペアを含む文字列を[Map(String, String)](../data-types/map.md)に変換します。
解析はノイズ（例: ログファイル）に対して寛容です。
入力文字列のキーと値のペアは、キー、キーと値の区切り文字、値で構成されています。
キーと値のペアはペア区切り文字で区切られています。
キーと値は引用符で囲むことができます。

**構文**

``` sql
extractKeyValuePairs(data[, key_value_delimiter[, pair_delimiter[, quoting_character]]])
```

別名:
- `str_to_map`
- `mapFromString`

**引数**

- `data` - キーと値のペアを抽出するための文字列。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。
- `key_value_delimiter` - キーと値を区切る単一文字。デフォルトは `:`。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。
- `pair_delimiters` - ペアを区切る文字のセット。デフォルトは ` `, `,` および `;`。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。
- `quoting_character` - 引用符として使う単一文字。デフォルトは `"`。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- キーと値のペア。[Map(String, String)](../data-types/map.md)型

**例**

クエリ

``` sql
SELECT extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv
```

結果:

``` result:
┌─kv──────────────────────────────────────────────────────────────────────┐
│ {'name':'neymar','age':'31','team':'psg','nationality':'brazil'}        │
└─────────────────────────────────────────────────────────────────────────┘
```

単一引用符 `'` を引用符として使用する場合:

``` sql
SELECT extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') as kv
```

結果:

``` text
┌─kv───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {'name':'neymar','age':'31','team':'psg','nationality':'brazil','last_key':'last_value'}                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

エスケープシーケンスをサポートしない場合:

``` sql
SELECT extractKeyValuePairs('age:a\\x0A\\n\\0') AS kv
```

結果:

``` text
┌─kv─────────────────────┐
│ {'age':'a\\x0A\\n\\0'} │
└────────────────────────┘
```

`toString`でシリアライズされたマップ文字列のキーと値のペアを復元するには:

```sql
SELECT
    map('John', '33', 'Paula', '31') AS m,
    toString(m) as map_serialized,
    extractKeyValuePairs(map_serialized, ':', ',', '\'') AS map_restored
FORMAT Vertical;
```

結果:

```
Row 1:
──────
m:              {'John':'33','Paula':'31'}
map_serialized: {'John':'33','Paula':'31'}
map_restored:   {'John':'33','Paula':'31'}
```

## extractKeyValuePairsWithEscaping

`extractKeyValuePairs`と同じですが、エスケープをサポートしています。

サポートされているエスケープシーケンス: `\x`, `\N`, `\a`, `\b`, `\e`, `\f`, `\n`, `\r`, `\t`, `\v` および `\0`。
非標準のエスケープシーケンスは、そのまま（バックスラッシュを含む）返されますが、以下のいずれかを除きます：`\\`, `'`, `"`, `backtick`, `/`, `=` または ASCII制御文字（c <= 31）。

この関数は、プリエスケープやポストエスケープが適切でないケースを満たします。例えば、以下のような入力文字列があるとします：`a: "aaaa\"bbb"`。期待される出力は：`a: aaaa\"bbbb`。
- プリエスケープ: プリエスケープすると、`a: "aaaa"bbb"` となり、`extractKeyValuePairs` はその後 `a: aaaa` を出力します。
- ポストエスケープ: `extractKeyValuePairs` は `a: aaaa\` を出力し、ポストエスケープはそのまま保持します。

キーの前のエスケープシーケンスはスキップされ、値には無効とみなされます。

**例**

エスケープシーケンスに対するエスケープシーケンスサポートを有効にした場合:

``` sql
SELECT extractKeyValuePairsWithEscaping('age:a\\x0A\\n\\0') AS kv
```

結果:

``` result
┌─kv────────────────┐
│ {'age':'a\n\n\0'} │
└───────────────────┘
```

## mapAdd

すべてのキーを収集し、対応する値を合計します。

**構文**

```sql
mapAdd(arg1, arg2 [, ...])
```

**引数**

引数は2つの[配列](../data-types/array.md#data-type-array)の[タプル](../data-types/tuple.md#tuplet1-t2)または[マップ](../data-types/map.md)で、最初の配列のアイテムがキーを表し、2つ目の配列が各キーの値を含みます。すべてのキー配列は同じ型を持ち、すべての値配列は、1つの型 ([Int64](../data-types/int-uint.md#int-ranges)、[UInt64](../data-types/int-uint.md#uint-ranges) または [Float64](../data-types/float.md#float32-float64)) に昇格される項目を含む必要があります。共通の昇格型が結果配列の型として使用されます。

**返される値**

- 引数に応じて、1つの[マップ](../data-types/map.md)か、ソートされたキーを含む最初の配列と、そのキーに対応する値を含む2つ目の配列を持つ[タプル](../data-types/tuple.md#tuplet1-t2)が返されます。

**例**

`Map`型を使ったクエリ:

```sql
SELECT mapAdd(map(1,1), map(1,1));
```

結果:

```text
┌─mapAdd(map(1, 1), map(1, 1))─┐
│ {1:2}                        │
└──────────────────────────────┘
```

タプルを使ったクエリ:

```sql
SELECT mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res) as type;
```

結果:

```text
┌─res───────────┬─type───────────────────────────────┐
│ ([1,2],[2,2]) │ Tuple(Array(UInt8), Array(UInt64)) │
└───────────────┴────────────────────────────────────┘
```

## mapSubtract

すべてのキーを収集し、対応する値を減算します。

**構文**

```sql
mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**引数**

引数は2つの[配列](../data-types/array.md#data-type-array)の[タプル](../data-types/tuple.md#tuplet1-t2)または[マップ](../data-types/map.md)で、最初の配列のアイテムがキーを表し、2つ目の配列が各キーの値を含みます。すべてのキー配列は同じ型を持ち、すべての値配列は、1つの型 ([Int64](../data-types/int-uint.md#int-ranges)、[UInt64](../data-types/int-uint.md#uint-ranges) または [Float64](../data-types/float.md#float32-float64)) に昇格される項目を含む必要があります。共通の昇格型が結果配列の型として使用されます。

**返される値**

- 引数に応じて、1つの[マップ](../data-types/map.md)か、ソートされたキーを含む最初の配列と、そのキーに対応する値を含む2つ目の配列を持つ[タプル](../data-types/tuple.md#tuplet1-t2)が返されます。

**例**

`Map`型を使ったクエリ:

```sql
SELECT mapSubtract(map(1,1), map(1,1));
```

結果:

```text
┌─mapSubtract(map(1, 1), map(1, 1))─┐
│ {1:0}                             │
└───────────────────────────────────┘
```

タプルマップを使ったクエリ:

```sql
SELECT mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])) as res, toTypeName(res) as type;
```

結果:

```text
┌─res────────────┬─type──────────────────────────────┐
│ ([1,2],[-1,0]) │ Tuple(Array(UInt8), Array(Int64)) │
└────────────────┴───────────────────────────────────┘
```

## mapPopulateSeries

整数キーを持つマップの欠落しているキーと値のペアを埋めます。
最大キーを指定することで、キーを最大値を超えて拡張することをサポートします。
具体的には、この関数はマップのキーを、最小から最大までの値（または指定された `max` 引数）に1のステップサイズで形成し、対応する値を持つマップを返します。
値が指定されていないキーの場合は、デフォルト値が使用されます。
キーが重複する場合は、最初の値（出現順に）だけがそのキーに関連付けられます。

**構文**

```sql
mapPopulateSeries(map[, max])
mapPopulateSeries(keys, values[, max])
```

配列の引数については、`keys` と `values` の各行内の要素数は同じである必要があります。

**引数**

引数は、整数キーを持つ[マップ](../data-types/map.md)または2つの[配列](../data-types/array.md#data-type-array)で、最初と2番目の配列が各キーのキーと値を含みます。

マップされた配列:

- `map` — 整数キーを持つマップ。[Map](../data-types/map.md)。

または

- `keys` — キーの配列。[Array](../data-types/array.md#data-type-array)([Int](../data-types/int-uint.md#uint-ranges))。
- `values` — 値の配列。[Array](../data-types/array.md#data-type-array)([Int](../data-types/int-uint.md#uint-ranges))。
- `max` — 最大キー値。オプション。[Int8, Int16, Int32, Int64, Int128, Int256](../data-types/int-uint.md#int-ranges)。

**返される値**

- 引数に応じて、ソートされた順序にあるキーを含む[Map](../data-types/map.md)または2つの[配列](../data-types/array.md#data-type-array)を持つ[タプル](../data-types/tuple.md#tuplet1-t2): キーに対応する値を持つ。

**例**

`Map`型を使ったクエリ:

```sql
SELECT mapPopulateSeries(map(1, 10, 5, 20), 6);
```

結果:

```text
┌─mapPopulateSeries(map(1, 10, 5, 20), 6)─┐
│ {1:10,2:0,3:0,4:0,5:20,6:0}             │
└─────────────────────────────────────────┘
```

マップされた配列を使ったクエリ:

```sql
SELECT mapPopulateSeries([1,2,4], [11,22,44], 5) AS res, toTypeName(res) AS type;
```

結果:

```text
┌─res──────────────────────────┬─type──────────────────────────────┐
│ ([1,2,3,4,5],[11,22,0,44,0]) │ Tuple(Array(UInt8), Array(UInt8)) │
└──────────────────────────────┴───────────────────────────────────┘
```

## mapContains

指定されたキーが指定されたマップに含まれているかどうかを返します。

**構文**

```sql
mapContains(map, key)
```

**引数**

- `map` — マップ。[Map](../data-types/map.md)。
- `key` — キー。`map` のキー型と一致する型である必要があります。

**返される値**

- `map` が `key` を含む場合は `1`、含まない場合は `0`。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapContains(a, 'name') FROM tab;

```

結果:

```text
┌─mapContains(a, 'name')─┐
│                      1 │
│                      0 │
└────────────────────────┘
```

## mapKeys

指定されたマップのキーを返します。

この関数は、設定 [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) を有効にすることで最適化できます。
設定を有効にすると、関数はマップ全体の代わりに [keys](../data-types/map.md#map-subcolumns) サブカラムのみを読み取ります。
クエリ `SELECT mapKeys(m) FROM table` は `SELECT m.keys FROM table` に変換されます。

**構文**

```sql
mapKeys(map)
```

**引数**

- `map` — マップ。[Map](../data-types/map.md)。

**返される値**

- `map` からすべてのキーを含む配列。[Array](../data-types/array.md)。

**例**

クエリ:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapKeys(a) FROM tab;
```

結果:

```text
┌─mapKeys(a)────────────┐
│ ['name','age']        │
│ ['number','position'] │
└───────────────────────┘
```

## mapValues

指定されたマップの値を返します。

この関数は、設定 [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) を有効にすることで最適化できます。
設定を有効にすると、関数はマップ全体の代わりに [values](../data-types/map.md#map-subcolumns) サブカラムのみを読み取ります。
クエリ `SELECT mapValues(m) FROM table` は `SELECT m.values FROM table` に変換されます。

**構文**

```sql
mapValues(map)
```

**引数**

- `map` — マップ。[Map](../data-types/map.md)。

**返される値**

- `map` からすべての値を含む配列。[Array](../data-types/array.md)。

**例**

クエリ:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapValues(a) FROM tab;
```

結果:

```text
┌─mapValues(a)─────┐
│ ['eleven','11']  │
│ ['twelve','6.0'] │
└──────────────────┘
```

## mapContainsKeyLike

**構文**

```sql
mapContainsKeyLike(map, pattern)
```

**引数**
- `map` — マップ。[Map](../data-types/map.md)。
- `pattern`  - マッチングする文字列パターン。

**返される値**

- 指定したパターンにマッチする `key` が `map` に含まれている場合は `1`、そうでない場合は `0`。

**例**

クエリ:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapContainsKeyLike(a, 'a%') FROM tab;
```

結果:

```text
┌─mapContainsKeyLike(a, 'a%')─┐
│                           1 │
│                           0 │
└─────────────────────────────┘
```

## mapExtractKeyLike

文字列キーを持つマップとLIKEパターンを与えると、この関数はキーがパターンと一致する要素を持つマップを返します。

**構文**

```sql
mapExtractKeyLike(map, pattern)
```

**引数**

- `map` — マップ。[Map](../data-types/map.md)。
- `pattern`  - マッチングする文字列パターン。

**返される値**

- 指定したパターンに一致するキーを持つ要素を含むマップ。一致する要素がない場合は、空のマップが返されます。

**例**

クエリ:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapExtractKeyLike(a, 'a%') FROM tab;
```

結果:

```text
┌─mapExtractKeyLike(a, 'a%')─┐
│ {'abc':'abc'}              │
│ {}                         │
└────────────────────────────┘
```

## mapApply

マップの各要素に関数を適用します。

**構文**

```sql
mapApply(func, map)
```

**引数**

- `func` — [ラムダ関数](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function)。
- `map` — [マップ](../data-types/map.md)。

**返される値**

- 各要素に `func(map1[i], ..., mapN[i])` を適用して得られるオリジナルのマップからマップを返します。

**例**

クエリ:

```sql
SELECT mapApply((k, v) -> (k, v * 10), _map) AS r
FROM
(
    SELECT map('key1', number, 'key2', number * 2) AS _map
    FROM numbers(3)
)
```

結果:

```text
┌─r─────────────────────┐
│ {'key1':0,'key2':0}   │
│ {'key1':10,'key2':20} │
│ {'key1':20,'key2':40} │
└───────────────────────┘
```

## mapFilter

マップの各要素に関数を適用してフィルタリングします。

**構文**

```sql
mapFilter(func, map)
```

**引数**

- `func`  - [ラムダ関数](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function)。
- `map` — [マップ](../data-types/map.md)。

**返される値**

- `func(map1[i], ..., mapN[i])` が0以外のものを返す`map`内の要素のみを含むマップを返します。

**例**

クエリ:

```sql
SELECT mapFilter((k, v) -> ((v % 2) = 0), _map) AS r
FROM
(
    SELECT map('key1', number, 'key2', number * 2) AS _map
    FROM numbers(3)
)
```

結果:

```text
┌─r───────────────────┐
│ {'key1':0,'key2':0} │
│ {'key2':2}          │
│ {'key1':2,'key2':4} │
└─────────────────────┘
```

## mapUpdate

**構文**

```sql
mapUpdate(map1, map2)
```

**引数**

- `map1` [マップ](../data-types/map.md)。
- `map2` [マップ](../data-types/map.md)。

**返される値**

- map2の対応キーに対する値が更新されたmap1を返します。

**例**

クエリ:

```sql
SELECT mapUpdate(map('key1', 0, 'key3', 0), map('key1', 10, 'key2', 10)) AS map;
```

結果:

```text
┌─map────────────────────────────┐
│ {'key3':0,'key1':10,'key2':10} │
└────────────────────────────────┘
```

## mapConcat

キーの等価性に基づいて複数のマップを連結します。
同じキーを持つ要素が複数の入力マップに存在する場合、すべての要素が結果マップに追加されますが、演算子 `[]` を介してアクセスできるのは最初の要素だけです。

**構文**

```sql
mapConcat(maps)
```

**引数**

-   `maps` – 任意の数の[マップ](../data-types/map.md)。

**返される値**

- 引数として渡されたマップを連結したマップを返します。

**例**

クエリ:

```sql
SELECT mapConcat(map('key1', 1, 'key3', 3), map('key2', 2)) AS map;
```

結果:

```text
┌─map──────────────────────────┐
│ {'key1':1,'key3':3,'key2':2} │
└──────────────────────────────┘
```

クエリ:

```sql
SELECT mapConcat(map('key1', 1, 'key2', 2), map('key1', 3)) AS map, map['key1'];
```

結果:

```text
┌─map──────────────────────────┬─elem─┐
│ {'key1':1,'key2':2,'key1':3} │    1 │
└──────────────────────────────┴──────┘
```

## mapExists(\[func,\], map)

`map`の少なくとも1つのキー-値のペアが存在し、`func(key, value)` が0以外を返す場合に1を返します。それ以外の場合は0を返します。

:::note
`mapExists` は [高次関数](../../sql-reference/functions/index.md#higher-order-functions)です。
最初の引数としてラムダ関数を渡すことができます。
:::

**例**

クエリ:

```sql
SELECT mapExists((k, v) -> (v = 1), map('k1', 1, 'k2', 2)) AS res
```

結果:

```
┌─res─┐
│   1 │
└─────┘
```

## mapAll(\[func,\] map)

`map`のすべてのキー-値のペアに対して`func(key, value)`が0以外を返す場合に1を返します。それ以外の場合は0を返します。

:::note
`mapAll` は [高次関数](../../sql-reference/functions/index.md#higher-order-functions)です。
最初の引数としてラムダ関数を渡すことができます。
:::

**例**

クエリ:

```sql
SELECT mapAll((k, v) -> (v = 1), map('k1', 1, 'k2', 2)) AS res
```

結果:

```
┌─res─┐
│   0 │
└─────┘
```

## mapSort(\[func,\], map)

マップの要素を昇順にソートします。
`func` 関数が指定された場合、マップのキーと値に適用された`func`関数の結果によってソート順が決まります。

**例**

``` sql
SELECT mapSort(map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key1':3,'key2':2,'key3':1} │
└──────────────────────────────┘
```

``` sql
SELECT mapSort((k, v) -> v, map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key3':1,'key2':2,'key1':3} │
└──────────────────────────────┘
```

詳細は `arraySort` 関数の[リファレンス](../../sql-reference/functions/array-functions.md#array_functions-sort)を参照してください。

## mapPartialSort

部分的なソートを許可する `limit` 引数を追加して、昇順にマップの要素をソートします。
`func` 関数が指定された場合、マップのキーと値に適用された`func`関数の結果によってソート順が決まります。

**構文**

```sql
mapPartialSort([func,] limit, map)
```
**引数**

- `func` – マップのキーと値に適用するオプションの関数。[ラムダ関数](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function)。
- `limit` – ソートされる要素の範囲 [1..limit]。[(U)Int](../data-types/int-uint.md)。
- `map` – ソートするマップ。[Map](../data-types/map.md)。

**返される値**

- 部分的にソートされたマップ。[Map](../data-types/map.md)。

**例**

``` sql
SELECT mapPartialSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2));
```

``` text
┌─mapPartialSort(lambda(tuple(k, v), v), 2, map('k1', 3, 'k2', 1, 'k3', 2))─┐
│ {'k2':1,'k3':2,'k1':3}                                                    │
└───────────────────────────────────────────────────────────────────────────┘
```

## mapReverseSort(\[func,\], map)

マップの要素を降順にソートします。
`func` 関数が指定された場合、マップのキーと値に適用された`func`関数の結果によってソート順が決まります。

**例**

``` sql
SELECT mapReverseSort(map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key3':1,'key2':2,'key1':3} │
└──────────────────────────────┘
```

``` sql
SELECT mapReverseSort((k, v) -> v, map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key1':3,'key2':2,'key3':1} │
└──────────────────────────────┘
```

詳細は `arrayReverseSort` 関数の[リファレンス](../../sql-reference/functions/array-functions.md#array_functions-reverse-sort)を参照してください。

## mapPartialReverseSort

追加の`limit`引数を持つ部分的なソートを許可した上で、降順にマップの要素をソートします。
`func` 関数が指定された場合、マップのキーと値に適用された`func`関数の結果によってソート順が決まります。

**構文**

```sql
mapPartialReverseSort([func,] limit, map)
```
**引数**

- `func` – マップのキーと値に適用するオプションの関数。[ラムダ関数](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function)。
- `limit` – ソートされる要素の範囲 [1..limit]。[(U)Int](../data-types/int-uint.md)。
- `map` – ソートするマップ。[Map](../data-types/map.md)。

**返される値**

- 部分的にソートされたマップ。[Map](../data-types/map.md)。

**例**

``` sql
SELECT mapPartialReverseSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2));
```

``` text
┌─mapPartialReverseSort(lambda(tuple(k, v), v), 2, map('k1', 3, 'k2', 1, 'k3', 2))─┐
│ {'k1':3,'k3':2,'k2':1}                                                           │
└──────────────────────────────────────────────────────────────────────────────────┘
```
