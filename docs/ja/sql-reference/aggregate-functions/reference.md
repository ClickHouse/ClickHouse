---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "\u53C2\u7167"
---

# 集計関数の参照 {#aggregate-functions-reference}

## カウント {#agg_function-count}

行数またはnot-NULL値をカウントします。

ClickHouseは以下の構文をサポートしています `count`:
- `count(expr)` または `COUNT(DISTINCT expr)`.
- `count()` または `COUNT(*)`. その `count()` 構文はClickHouse固有です。

**パラメータ**

機能は取ることができます:

-   ゼロパラメータ。
-   ワン [式](../syntax.md#syntax-expressions).

**戻り値**

-   関数がパラメータなしで呼び出された場合、行の数がカウントされます。
-   もし [式](../syntax.md#syntax-expressions) 渡されると、関数はこの式がnot nullを返した回数をカウントします。 式がaを返す場合 [Null可能](../../sql-reference/data-types/nullable.md)-型の値、その後の結果 `count` 滞在しない `Nullable`. 式が返された場合、関数は0を返します `NULL` すべての行について。

どちらの場合も、戻り値の型は次のとおりです [UInt64](../../sql-reference/data-types/int-uint.md).

**詳細**

クリックハウスは `COUNT(DISTINCT ...)` 構文。 この構造の動作は、 [count_distinct_implementation](../../operations/settings/settings.md#settings-count_distinct_implementation) 設定。 それはのどれを定義します [uniq\*](#agg_function-uniq) 関数は、操作を実行するために使用されます。 デフォルトは [uniqExact](#agg_function-uniqexact) 機能。

その `SELECT count() FROM table` テーブル内のエントリの数が別々に格納されないため、クエリは最適化されません。 テーブルから小さな列を選択し、その中の値の数をカウントします。

**例**

例1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

例2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

この例では、 `count(DISTINCT num)` によって実行されます。 `uniqExact` に従う機能 `count_distinct_implementation` 設定値。

## 任意(x) {#agg_function-any}

最初に検出された値を選択します。
クエリは毎回異なる順序でも任意の順序で実行できるため、この関数の結果は不確定です。
決定的な結果を得るには、 ‘min’ または ‘max’ 関数の代わりに ‘any’.

場合によっては、実行の順序に依存することができます。 これは、SELECTがORDER BYを使用するサブクエリから来ている場合に適用されます。

ときa `SELECT` クエリには `GROUP BY` クラスタ内のすべての式は、クラスタ内のすべての式を必要とします。 `SELECT`, `HAVING`,and `ORDER BY` 句は、キーまたは集計関数から計算されます。 つまり、テーブルから選択された各列は、キーまたは集計関数内で使用する必要があります。 MySQLのような動作を得るには、他の列を `any` 集計関数。

## anyHeavy(x) {#anyheavyx}

を使用して頻繁に発生する値を選択します。 [ヘビーヒッター](http://www.cs.umd.edu/~samir/498/karp.pdf) アルゴリズム 各クエリの実行スレッドでケースの半分を超える値がある場合、この値が返されます。 通常、結果は非決定的です。

``` sql
anyHeavy(column)
```

**引数**

-   `column` – The column name.

**例**

を取る [オンタイム](../../getting-started/example-datasets/ontime.md) データ-セットはの頻繁に発生する値を選び、 `AirlineID` 列。

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```

## anyLast(x) {#anylastx}

最後に検出された値を選択します。
結果は同じように不確定です。 `any` 機能。

## グループビタンド {#groupbitand}

ビット単位で適用 `AND` 数のシリーズのために。

``` sql
groupBitAnd(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt*` タイプ。

**例**

テストデータ:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

クエリ:

``` sql
SELECT groupBitAnd(num) FROM t
```

どこに `num` テストデータを含む列です。

結果:

``` text
binary     decimal
00000100 = 4
```

## groupBitOr {#groupbitor}

ビット単位で適用 `OR` 数のシリーズのために。

``` sql
groupBitOr(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt*` タイプ。

**例**

テストデータ:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

クエリ:

``` sql
SELECT groupBitOr(num) FROM t
```

どこに `num` テストデータを含む列です。

結果:

``` text
binary     decimal
01111101 = 125
```

## groupBitXor {#groupbitxor}

ビット単位で適用 `XOR` 数のシリーズのために。

``` sql
groupBitXor(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt*` タイプ。

**例**

テストデータ:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

クエリ:

``` sql
SELECT groupBitXor(num) FROM t
```

どこに `num` テストデータを含む列です。

結果:

``` text
binary     decimal
01101000 = 104
```

## groupBitmap {#groupbitmap}

符号なし整数列からのビットマップまたは集計計算、uint64型の基数を返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例**

テストデータ:

``` text
UserID
1
1
2
3
```

クエリ:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

結果:

``` text
num
3
```

## min(x) {#agg_function-min}

最小値を計算します。

## max(x) {#agg_function-max}

最大値を計算します。

## アルグミン(arg,val) {#agg-function-argmin}

計算します ‘arg’ 最小値の値 ‘val’ 値。 のいくつかの異なる値がある場合 ‘arg’ の最小値に対して ‘val’、検出された最初のこれらの値が出力されます。

**例:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMin(user, salary) FROM salary
```

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```

## アルグマックス(arg,val) {#agg-function-argmax}

計算します ‘arg’ 最大値 ‘val’ 値。 のいくつかの異なる値がある場合 ‘arg’ の最大値の場合 ‘val’、検出された最初のこれらの値が出力されます。

## sum(x) {#agg_function-sum}

合計を計算します。
数字のみで動作します。

## sumWithOverflow(x) {#sumwithoverflowx}

結果には、入力パラメーターと同じデータ型を使用して、数値の合計を計算します。 合計がこのデータ型の最大値を超えると、関数はエラーを返します。

数字のみで動作します。

## sumMap(キー,値),sumMap(タプル(キー,値))) {#agg_functions-summap}

合計 ‘value’ に指定されたキーに従って配列 ‘key’ 配列
キーと値の配列のタプルを渡すことは、キーと値の二つの配列を渡すことと同義です。
要素の数 ‘key’ と ‘value’ 合計される行ごとに同じである必要があります。
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

例:

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
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

## skewPop {#skewpop}

を計算する [歪み](https://en.wikipedia.org/wiki/Skewness) シーケンスの。

``` sql
skewPop(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**例**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## skewSamp {#skewsamp}

を計算する [サンプル歪度](https://en.wikipedia.org/wiki/Skewness) シーケンスの。

渡された値がサンプルを形成する場合、確率変数の歪度の不偏推定値を表します。

``` sql
skewSamp(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). もし `n <= 1` (`n` はサンプルのサイズです）、その後、関数が返します `nan`.

**例**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## クルトポップ {#kurtpop}

を計算する [尖度](https://en.wikipedia.org/wiki/Kurtosis) シーケンスの。

``` sql
kurtPop(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**例**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## クルツァンプ {#kurtsamp}

を計算する [尖度のサンプル](https://en.wikipedia.org/wiki/Kurtosis) シーケンスの。

渡された値がその標本を形成する場合、確率変数の尖度の不偏推定値を表します。

``` sql
kurtSamp(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). もし `n <= 1` (`n` はサンプルのサイズです）、関数は `nan`.

**例**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## avg(x) {#agg_function-avg}

平均を計算します。
数字のみで動作します。
結果は常にFloat64です。

## avgWeighted {#avgweighted}

計算します [加重算術平均](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**構文**

``` sql
avgWeighted(x, weight)
```

**パラメータ**

-   `x` — Values. [整数](../data-types/int-uint.md) または [浮動小数点数](../data-types/float.md).
-   `weight` — Weights of the values. [整数](../data-types/int-uint.md) または [浮動小数点数](../data-types/float.md).

タイプの `x` と `weight` 同じである必要があります。

**戻り値**

-   加重平均。
-   `NaN`. すべての重みが0に等しい場合。

タイプ: [Float64](../data-types/float.md).

**例**

クエリ:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

結果:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

## uniq {#agg_function-uniq}

引数の異なる値のおおよその数を計算します。

``` sql
uniq(x[, ...])
```

**パラメータ**

この関数は、可変数のパラメータを取ります。 変数はあります `Tuple`, `Array`, `Date`, `DateTime`, `String` または数値型。

**戻り値**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ番号。

**実装の詳細**

関数:

-   集計内のすべてのパラメーターのハッシュを計算し、それを計算に使用します。

-   を使用して適応サンプリングアルゴリズムです。 計算状態では、関数は65536までの要素ハッシュ値のサンプルを使用します。

        This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

-   結果を確定的に提供します(クエリ処理順序に依存しません)。

使用をお勧めしますこの機能はほとんど全てのシナリオ.

**も参照。**

-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined {#agg_function-uniqcombined}

異なる引数値のおおよその数を計算します。

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

その `uniqCombined` 関数は、異なる値の数を計算するための良い選択です。

**パラメータ**

この関数は、可変数のパラメータを取ります。 変数はあります `Tuple`, `Array`, `Date`, `DateTime`, `String` または数値型。

`HLL_precision` は、セルの数の底2対数です。 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). オプションで、次のように関数を使用できます `uniqCombined(x[, ...])`. のデフォルト値 `HLL_precision` は17であり、これは実質的に96KiBの空間（2^17セル、各6ビット）である。

**戻り値**

-   番号 [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ番号。

**実装の詳細**

関数:

-   ハッシュ(64ビットのハッシュ `String` それ以外の場合は32ビット）集計内のすべてのパラメータについて、それを計算に使用します。

-   配列、ハッシュテーブル、HyperLogLogと誤り訂正表の組み合わせを使用します。

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   結果を確定的に提供します(クエリ処理順序に依存しません)。

!!! note "注"
    非32ビットハッシュを使用するので-`String` タイプは、結果よりも有意に大きい基数のための非常に高い誤差を有する `UINT_MAX` （エラーは数十億の異なる値の後にすぐに発生します）、この場合は以下を使用する必要があります [uniqCombined64](#agg_function-uniqcombined64)

と比較される [uniq](#agg_function-uniq) 関数は、 `uniqCombined`:

-   数倍少ないメモリを消費します。
-   数倍高い精度で計算します。
-   通常はやや性能が低い。 シナリオによっては, `uniqCombined` ではどのように絡んでいるのかを調べ `uniq` たとえば、ネットワーク経由で多数の集約状態を送信する分散クエリです。

**も参照。**

-   [uniq](#agg_function-uniq)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined64 {#agg_function-uniqcombined64}

同じ [uniqCombined](#agg_function-uniqcombined) しかし、すべてのデータ型に64ビットハッシュを使用します。

## uniqHLL12 {#agg_function-uniqhll12}

異なる引数値のおおよその数を計算します。 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) アルゴリズム

``` sql
uniqHLL12(x[, ...])
```

**パラメータ**

この関数は、可変数のパラメータを取ります。 変数はあります `Tuple`, `Array`, `Date`, `DateTime`, `String` または数値型。

**戻り値**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ番号。

**実装の詳細**

関数:

-   集計内のすべてのパラメーターのハッシュを計算し、それを計算に使用します。

-   HyperLogLogアルゴリズムを使用して、異なる引数値の数を近似します。

        2^12 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   決定的な結果を提供します(クエリ処理順序に依存しません)。

この機能の使用はお勧めしません。 ほとんどの場合、 [uniq](#agg_function-uniq) または [uniqCombined](#agg_function-uniqcombined) 機能。

**も参照。**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqExact](#agg_function-uniqexact)

## uniqExact {#agg_function-uniqexact}

異なる引数値の正確な数を計算します。

``` sql
uniqExact(x[, ...])
```

使用する `uniqExact` あなたが絶対に正確な結果が必要な場合は、関数。 それ以外の場合は、 [uniq](#agg_function-uniq) 機能。

その `uniqExact` 関数はより多くのメモリを使用 `uniq`、状態の大きさは、異なる値の数が増加するにつれて無限の成長を有するからである。

**パラメータ**

この関数は、可変数のパラメータを取ります。 変数はあります `Tuple`, `Array`, `Date`, `DateTime`, `String` または数値型。

**も参照。**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqHLL12](#agg_function-uniqhll12)

## groupArray(x),groupArray(max_size)(x) {#agg_function-grouparray}

引数値の配列を作成します。
値は、任意の（不確定な）順序で配列に追加できます。

第二のバージョン（と `max_size` パラメータ)結果の配列のサイズを以下に制限します `max_size` 要素。
例えば, `groupArray (1) (x)` に等しい。 `[any (x)]`.

場合によっては、実行順序に依存することもあります。 これは、次の場合に適用されます `SELECT` を使用するサブクエリから来ています `ORDER BY`.

## グループパラインセルタット {#grouparrayinsertat}

指定された位置に配列に値を挿入します。

**構文**

``` sql
groupArrayInsertAt(default_x, size)(x, pos);
```

あるクエリで複数の値が同じ位置に挿入された場合、関数は次のように動作します:

-   クエリが単一のスレッドで実行される場合、挿入された最初の値が使用されます。
-   クエリが複数のスレッドで実行される場合、結果の値は挿入された値のいずれかになります。

**パラメータ**

-   `x` — Value to be inserted. [式](../syntax.md#syntax-expressions) のいずれかになります [対応するデータ型](../../sql-reference/data-types/index.md).
-   `pos` — Position at which the specified element `x` 挿入されるべきです。 配列のインデックス番号はゼロから始まります。 [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x`— Default value for substituting in empty positions. Optional parameter. [式](../syntax.md#syntax-expressions) その結果、データ型は `x` パラメータ。 もし `default_x` は定義されていない。 [デフォルト値](../../sql-reference/statements/create.md#create-default-values) 使用されます。
-   `size`— Length of the resulting array. Optional parameter. When using this parameter, the default value `default_x` 指定する必要があります。 [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).

**戻り値**

-   値が挿入された配列。

タイプ: [配列](../../sql-reference/data-types/array.md#data-type-array).

**例**

クエリ:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

結果:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

結果:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

結果:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

一つの位置に要素のマルチスレッド挿入。

クエリ:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

このクエリの結果として、あなたはランダムな整数を取得します `[0,9]` 範囲 例えば:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```

## グループパレイモビングスム {#agg_function-grouparraymovingsum}

入力値の移動合計を計算します。

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

この機能できるウィンドウサイズとしてのパラメータとします。 指定されていない場合、関数は列の行数に等しいウィンドウサイズを取ります。

**パラメータ**

-   `numbers_for_summing` — [式](../syntax.md#syntax-expressions) 数値データ型の値になります。
-   `window_size` — Size of the calculation window.

**戻り値**

-   入力データと同じサイズと型の配列。

**例**

サンプルテーブル:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

クエリ:

``` sql
SELECT
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

``` sql
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

## groupArrayMovingAvg {#agg_function-grouparraymovingavg}

入力値の移動平均を計算します。

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

この機能できるウィンドウサイズとしてのパラメータとします。 指定されていない場合、関数は列の行数に等しいウィンドウサイズを取ります。

**パラメータ**

-   `numbers_for_summing` — [式](../syntax.md#syntax-expressions) 数値データ型の値になります。
-   `window_size` — Size of the calculation window.

**戻り値**

-   入力データと同じサイズと型の配列。

この関数は [ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). 結果のデータ型の小数点以下の桁数を切り捨てます。

**例**

サンプルテーブル `b`:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

クエリ:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```

## groupUniqArray(x),groupUniqArray(max_size)(x) {#groupuniqarrayx-groupuniqarraymax-sizex}

異なる引数値から配列を作成します。 メモリ消費量は `uniqExact` 機能。

第二のバージョン（と `max_size` パラメータ)結果の配列のサイズを以下に制限します `max_size` 要素。
例えば, `groupUniqArray(1)(x)` に等しい。 `[any(x)]`.

## 分位数 {#quantile}

近似を計算します [分位数](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンスの。

この関数が適用されます [貯蔵所の見本抽出](https://en.wikipedia.org/wiki/Reservoir_sampling) 8192までの貯蔵所のサイズおよび見本抽出のための乱数発生器を使って。 結果は非決定的です。 正確な分位値を取得するには、次の式を使用します [quantileExact](#quantileexact) 機能。

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantile(level)(expr)
```

別名: `median`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   指定されたレベルの近似分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型入力の場合。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例**

入力テーブル:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

クエリ:

``` sql
SELECT quantile(val) FROM t
```

結果:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileDeterministic {#quantiledeterministic}

近似を計算します [分位数](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンスの。

この関数が適用されます [貯蔵所の見本抽出](https://en.wikipedia.org/wiki/Reservoir_sampling) 貯蔵所のサイズ8192までおよび見本抽出の決定論的なアルゴリズムを使って。 結果は決定的です。 正確な分位値を取得するには、次の式を使用します [quantileExact](#quantileexact) 機能。

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileDeterministic(level)(expr, determinator)
```

別名: `medianDeterministic`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).
-   `determinator` — Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occures too often, the function works incorrectly.

**戻り値**

-   指定されたレベルの近似分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型入力の場合。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例**

入力テーブル:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

クエリ:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

結果:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileExact {#quantileexact}

正確に計算する [分位数](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンスの。

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` メモリ、どこ `n` 渡された値の数です。 しかし、少数の値の場合、この関数は非常に効果的です。

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileExact(level)(expr)
```

別名: `medianExact`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   指定されたレベルの分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型入力の場合。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例**

クエリ:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

結果:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileExactWeighted {#quantileexactweighted}

正確に計算する [分位数](https://en.wikipedia.org/wiki/Quantile) 各要素の重みを考慮して、数値データシーケンスの重みを指定します。

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [quantileExact](#quantileexact). この関数は、次の代わりに使用できます `quantileExact` そして、重み1を指定します。

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileExactWeighted(level)(expr, weight)
```

別名: `medianExactWeighted`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence members. Weight is a number of value occurrences.

**戻り値**

-   指定されたレベルの分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型入力の場合。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例**

入力テーブル:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

クエリ:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

結果:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## クオンタイミング {#quantiletiming}

決定された精度では、 [分位数](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンスの。

結果は決定的です（クエリ処理順序に依存しません）。 この機能を最適化と配列における分布のような積載ウェブページではバックエンド対応。

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTiming(level)(expr)
```

別名: `medianTiming`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).

-   `expr` — [式](../syntax.md#syntax-expressions) aを返す列の値に対して [フロート\*](../../sql-reference/data-types/float.md)-タイプ番号。

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**精度**

計算は次の場合に正確です:

-   値の総数は5670を超えません。
-   値の総数は5670を超えますが、ページの読み込み時間は1024ms未満です。

それ以外の場合、計算の結果は16msの最も近い倍数に丸められます。

!!! note "注"
    ページの読み込み時間の分位数を計算するために、この機能はより有効、正確です [分位数](#quantile).

**戻り値**

-   指定されたレベルの分位値。

タイプ: `Float32`.

!!! note "注"
    関数に値が渡されない場合（使用する場合 `quantileTimingIf`), [ナン](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 返されます。 この目的は、これらのケースをゼロになるケースと区別することです。 見る [ORDER BY句](../statements/select/order-by.md#select-order-by) ソートに関する注意事項 `NaN` 値。

**例**

入力テーブル:

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

クエリ:

``` sql
SELECT quantileTiming(response_time) FROM t
```

結果:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantitetimingweighted {#quantiletimingweighted}

決定された精度では、 [分位数](https://en.wikipedia.org/wiki/Quantile) 各シーケンスメンバの重みに応じた数値データシーケンスの。

結果は決定的です（クエリ処理順序に依存しません）。 この機能を最適化と配列における分布のような積載ウェブページではバックエンド対応。

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

別名: `medianTimingWeighted`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).

-   `expr` — [式](../syntax.md#syntax-expressions) aを返す列の値に対して [フロート\*](../../sql-reference/data-types/float.md)-タイプ番号。

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**精度**

計算は次の場合に正確です:

-   値の総数は5670を超えません。
-   値の総数は5670を超えますが、ページの読み込み時間は1024ms未満です。

それ以外の場合、計算の結果は16msの最も近い倍数に丸められます。

!!! note "注"
    ページの読み込み時間の分位数を計算するために、この機能はより有効、正確です [分位数](#quantile).

**戻り値**

-   指定されたレベルの分位値。

タイプ: `Float32`.

!!! note "注"
    関数に値が渡されない場合（使用する場合 `quantileTimingIf`), [ナン](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 返されます。 この目的は、これらのケースをゼロになるケースと区別することです。 見る [ORDER BY句](../statements/select/order-by.md#select-order-by) ソートに関する注意事項 `NaN` 値。

**例**

入力テーブル:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

クエリ:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

結果:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileTDigest {#quantiletdigest}

近似を計算します [分位数](https://en.wikipedia.org/wiki/Quantile) を用いた数値データシーケンスの [t-ダイジェスト](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) アルゴリズム

最大誤差は1%です。 メモリ消費は `log(n)`,ここで `n` 値の数です。 結果は、クエリの実行順序に依存し、非決定的です。

機能の性能はの性能より低いです [分位数](#quantile) または [クオンタイミング](#quantiletiming). 状態サイズと精度の比に関しては、この関数は `quantile`.

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTDigest(level)(expr)
```

別名: `medianTDigest`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   指定されたレベルの近似分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型入力の場合。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例**

クエリ:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

結果:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

近似を計算します [分位数](https://en.wikipedia.org/wiki/Quantile) を用いた数値データシーケンスの [t-ダイジェスト](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) アルゴリズム この関数は、各シーケンスメンバーの重みを考慮に入れます。 最大誤差は1%です。 メモリ消費は `log(n)`,ここで `n` 値の数です。

機能の性能はの性能より低いです [分位数](#quantile) または [クオンタイミング](#quantiletiming). 状態サイズと精度の比に関しては、この関数は `quantile`.

結果は、クエリの実行順序に依存し、非決定的です。

複数を使用する場合 `quantile*` クエリ内の異なるレベルを持つ関数では、内部状態は結合されません(つまり、クエリの動作ができるほど効率的ではありません)。 この場合、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTDigest(level)(expr)
```

別名: `medianTDigest`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲の値 `[0.01, 0.99]`. デフォルト値は0.5です。 で `level=0.5` この関数は [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**戻り値**

-   指定されたレベルの近似分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型入力の場合。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例**

クエリ:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

結果:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**も参照。**

-   [中央値](#median)
-   [分位数](#quantiles)

## 中央値 {#median}

その `median*` 関数は、対応する関数のエイリアスです `quantile*` 機能。 数値データサンプルの中央値を計算します。

関数:

-   `median` — Alias for [分位数](#quantile).
-   `medianDeterministic` — Alias for [quantileDeterministic](#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](#quantileexact).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](#quantileexactweighted).
-   `medianTiming` — Alias for [クオンタイミング](#quantiletiming).
-   `medianTimingWeighted` — Alias for [quantitetimingweighted](#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantileTDigest](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](#quantiletdigestweighted).

**例**

入力テーブル:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

クエリ:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

結果:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## quantiles(level1, level2, …)(x) {#quantiles}

すべての分位関数にも対応する分位関数があります: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. これらの関数は、リストされたレベルのすべての分位数を一つのパスで計算し、結果の値の配列を返します。

## varSamp(x) {#varsampx}

金額を計算します `Σ((x - x̅)^2) / (n - 1)`,ここで `n` はサンプルサイズであり、 `x̅`の平均値です `x`.

渡された値がその標本を形成する場合、確率変数の分散の不偏推定値を表します。

ﾂづｩﾂ。 `Float64`. とき `n <= 1`,戻り値 `+∞`.

!!! note "注"
    この機能の利用を数値的に不安定なアルゴリズムです。 必要とすれば [数値安定性](https://en.wikipedia.org/wiki/Numerical_stability) 計算では、 `varSampStable` 機能。 それは遅く動作しますが、計算誤差は低くなります。

## varPop(x) {#varpopx}

金額を計算します `Σ((x - x̅)^2) / n`,ここで `n` はサンプルサイズであり、 `x̅`の平均値です `x`.

言い換えれば、値の集合に対する分散。 ﾂづｩﾂ。 `Float64`.

!!! note "注"
    この機能の利用を数値的に不安定なアルゴリズムです。 必要とすれば [数値安定性](https://en.wikipedia.org/wiki/Numerical_stability) 計算では、 `varPopStable` 機能。 それは遅く動作しますが、計算誤差は低くなります。

## stddevSamp(x) {#stddevsampx}

結果はの平方根に等しくなります `varSamp(x)`.

!!! note "注"
    この機能の利用を数値的に不安定なアルゴリズムです。 必要とすれば [数値安定性](https://en.wikipedia.org/wiki/Numerical_stability) 計算では、 `stddevSampStable` 機能。 それは遅く動作しますが、計算誤差は低くなります。

## stddevPop(x) {#stddevpopx}

結果はの平方根に等しくなります `varPop(x)`.

!!! note "注"
    この機能の利用を数値的に不安定なアルゴリズムです。 必要とすれば [数値安定性](https://en.wikipedia.org/wiki/Numerical_stability) 計算では、 `stddevPopStable` 機能。 それは遅く動作しますが、計算誤差は低くなります。

## topK(N)(x) {#topknx}

指定された列の中で最も頻度の高い値の配列を返します。 結果の配列は、値の近似頻度の降順でソートされます（値自体ではありません）。

を実装する。 [ろ過されたスペース節約](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf) TopKを解析するためのアルゴリズム。 [並列スペース節約](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

この関数は保証された結果を提供しません。 特定の状況では、エラーが発生し、最も頻繁な値ではない頻繁な値が返されることがあります。

私達は使用を推薦します `N < 10` 価値;性能は大きいと減ります `N` 値。 の最大値 `N = 65536`.

**パラメータ**

-   ‘N’ 返す要素の数です。

このパラメーターを省略すると、既定値10が使用されます。

**引数**

-   ' x ' – The value to calculate frequency.

**例**

を取る [オンタイム](../../getting-started/example-datasets/ontime.md) データセットで最も頻繁に発生する三つの値を選択します。 `AirlineID` 列。

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

## トップウェイト {#topkweighted}

に類似した `topK` しかし、整数型の追加引数を取ります - `weight`. すべての値が考慮されます `weight` 周波数計算のための時間。

**構文**

``` sql
topKWeighted(N)(x, weight)
```

**パラメータ**

-   `N` — The number of elements to return.

**引数**

-   `x` – The value.
-   `weight` — The weight. [UInt8](../../sql-reference/data-types/int-uint.md).

**戻り値**

おおよその重みの合計が最大の値の配列を返します。

**例**

クエリ:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

結果:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

## コバルサンプ(x,y) {#covarsampx-y}

の値を計算します `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Float64を返します。 とき `n <= 1`, returns +∞.

!!! note "注"
    この機能の利用を数値的に不安定なアルゴリズムです。 必要とすれば [数値安定性](https://en.wikipedia.org/wiki/Numerical_stability) 計算では、 `covarSampStable` 機能。 それは遅く動作しますが、計算誤差は低くなります。

## コバルポップ(x,y) {#covarpopx-y}

の値を計算します `Σ((x - x̅)(y - y̅)) / n`.

!!! note "注"
    この機能の利用を数値的に不安定なアルゴリズムです。 必要とすれば [数値安定性](https://en.wikipedia.org/wiki/Numerical_stability) 計算では、 `covarPopStable` 機能。 それは遅く動作しますが、計算誤差は低くなります。

## corr(x,y) {#corrx-y}

ピアソン相関係数を計算します: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

!!! note "注"
    この機能の利用を数値的に不安定なアルゴリズムです。 必要とすれば [数値安定性](https://en.wikipedia.org/wiki/Numerical_stability) 計算では、 `corrStable` 機能。 それは遅く動作しますが、計算誤差は低くなります。

## categoricalInformationValue {#categoricalinformationvalue}

の値を計算します `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` カテゴリごとに。

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

結果は、離散(カテゴリカル)フィーチャの方法を示します `[category1, category2, ...]` の値を予測する学習モデルに貢献する `tag`.

## simpleLinearRegression {#simplelinearregression}

単純(一次元)線形回帰を実行します。

``` sql
simpleLinearRegression(x, y)
```

パラメータ:

-   `x` — Column with dependent variable values.
-   `y` — Column with explanatory variable values.

戻り値:

定数 `(a, b)` 結果の行の `y = a*x + b`.

**例**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

## stochasticLinearRegression {#agg_functions-stochasticlinearregression}

この関数は、確率的線形回帰を実装します。 学習率、L2正則化係数、ミニバッチサイズのカスタムパラメータをサポートし、重みを更新する方法はほとんどありません ([アダム](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) （デフォルトで使用), [シンプルSGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [勢い](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [ネステロフ](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### パラメータ {#agg_functions-stochasticlinearregression-parameters}

が4カスタマイズ可能パラメータ。 デフォルト値が使用されますが、良いモデルではパラメータの調整が必要です。

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` 勾配降下ステップを実行したときのステップ長の係数です。 大きすぎる学習率の原因となり無限の量のモデルです。 デフォルトは `0.00001`.
2.  `l2 regularization coefficient` 過適合を防ぐのに役立つかもしれません。 デフォルトは `0.1`.
3.  `mini-batch size` グラデーションが計算され、グラデーション降下のステップを実行するために合計される要素の数を設定します。 純粋な確率降下は一つの要素を使用しますが、小さなバッチ（約10要素）を持つと勾配ステップがより安定します。 デフォルトは `15`.
4.  `method for updating weights`、彼らは: `Adam` （デフォルトでは), `SGD`, `Momentum`, `Nesterov`. `Momentum` と `Nesterov` もう少し多くの計算とメモリを必要とするが、収束の速度と確率勾配法の安定性の点で有用であることが起こる。

### 使用法 {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` モデルの近似と新しいデータの予測です。 モデルを適合させ、後で使用するためにその状態を保存するために、 `-State` 基本的に状態（モデルの重みなど）を保存するcombinator。
予測するには関数を使います [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod) は、状態を引数として取り、予測する機能も備えています。

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** フィット

このようなクエリが使用され得る。

``` sql
CREATE TABLE IF NOT EXISTS train_data
(
    param1 Float64,
    param2 Float64,
    target Float64
) ENGINE = Memory;

CREATE TABLE your_model ENGINE = Memory AS SELECT
stochasticLinearRegressionState(0.1, 0.0, 5, 'SGD')(target, param1, param2)
AS state FROM train_data;
```

ここでは、データを挿入する必要もあります `train_data` テーブル。 パラメータの数は固定されておらず、渡された引数の数にのみ依存します `linearRegressionState`. すべて数値でなければなりません。
ターゲット値を持つ列（予測することを学びたい）が最初の引数として挿入されることに注意してください。

**2.** 予測

テーブルに状態を保存した後、予測に複数回使用したり、他の状態とマージして新しいより良いモデルを作成したりすることもできます。

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

クエリは予測値の列を返します。 その最初の引数に注意してください `evalMLMethod` は `AggregateFunctionState` オブジェクト、次はフィーチャの列です。

`test_data` のようなテーブルです `train_data` が含まれないことがあります。

### ノート {#agg_functions-stochasticlinearregression-notes}

1.  統合モデルにはユーザーの作成などのクエリ:
    `sql  SELECT state1 + state2 FROM your_models`
    どこに `your_models` テーブルの両方のモデルです。 このクエリはnewを返します `AggregateFunctionState` オブジェクト

2.  ユーザーは、作成されたモデルのウェイトを独自の目的で取得することができます。 `-State` combinatorが使用されます。
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    このようなクエリはモデルに適合し、その重みを返します-最初はモデルのパラメータに対応する重みであり、最後はバイアスです。 したがって、上記の例では、クエリは3つの値を持つ列を返します。

**も参照。**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [線形回帰とロジスティック回帰の違い](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

この関数は、確率的ロジスティック回帰を実装します。 これは、バイナリ分類問題に使用することができ、stochasticLinearRegressionと同じカスタムパラメータをサポートし、同じように動作します。

### パラメータ {#agg_functions-stochasticlogisticregression-parameters}

パラメーターはstochasticLinearRegressionとまったく同じです:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
詳細については、 [パラメータ](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  フィット

<!-- -->

    See the `Fitting` section in the [stochasticLinearRegression](#stochasticlinearregression-usage-fitting) description.

    Predicted labels have to be in \[-1, 1\].

1.  予測

<!-- -->

    Using saved state we can predict probability of object having label `1`.

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

    The query will return a column of probabilities. Note that first argument of `evalMLMethod` is `AggregateFunctionState` object, next are columns of features.

    We can also set a bound of probability, which assigns elements to different labels.

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

    Then the result will be labels.

    `test_data` is a table like `train_data` but may not contain target value.

**も参照。**

-   [stochasticLinearRegression](#agg_functions-stochasticlinearregression)
-   [線形回帰とロジスティック回帰の違い。](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## groupBitmapAnd {#groupbitmapand}

ビットマップ列のANDを計算し、uint64型の基数を返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**パラメータ**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapAnd(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapAnd(z)─┐
│               3   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapAndState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapAndState(z)))─┐
│ [6,8,10]                                         │
└──────────────────────────────────────────────────┘
```

## groupBitmapOr {#groupbitmapor}

ビットマップ列のORを計算し、uint64型の基数を返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md). これは `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**パラメータ**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapOr(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapOr(z)─┐
│             15   │
└──────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapOrState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapOrState(z)))─┐
│ [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]           │
└─────────────────────────────────────────────────┘
```

## groupBitmapXor {#groupbitmapxor}

ビットマップ列のXORを計算し、uint64型の基数を返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**パラメータ**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapXor(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapXor(z)─┐
│              10   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapXorState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapXorState(z)))─┐
│ [1,3,5,6,8,10,11,13,14,15]                       │
└──────────────────────────────────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
