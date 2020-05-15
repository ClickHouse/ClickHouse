---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 36
toc_title: "\u53C2\u7167"
---

# 関数リファレンス {#function-reference}

## カウント {#agg_function-count}

行数またはnull以外の値をカウントします。

ClickHouseは以下の構文をサポートしています `count`:
- `count(expr)` または `COUNT(DISTINCT expr)`.
- `count()` または `COUNT(*)`. その `count()` 構文はClickHouse固有です。

**パラメータ**

機能は取ることができます:

-   ゼロ変数。
-   ワン [式](../syntax.md#syntax-expressions).

**戻り値**

-   関数がパラメータなしで呼び出されると、行数がカウントされます。
-   この [式](../syntax.md#syntax-expressions) が渡されると、この関数は、この式がnullではなく返された回数をカウントします。 式がaを返す場合 [Nullable](../../sql-reference/data-types/nullable.md)-タイプ値、そして結果の `count` 滞在しない `Nullable`. 式が返された場合、関数は0を返します `NULL` すべての行について。

どちらの場合も、戻り値の型は次のようになります [UInt64](../../sql-reference/data-types/int-uint.md).

**詳細**

クリックハウスは `COUNT(DISTINCT ...)` 構文。 この構成の動作は、 [count\_distinct\_implementation](../../operations/settings/settings.md#settings-count_distinct_implementation) 設定。 それはどれをの定義します [uniq\*](#agg_function-uniq) 関数は、操作を実行するために使用されます。 デフォルトは [ユニキャック](#agg_function-uniqexact) 機能。

その `SELECT count() FROM table` テーブル内のエントリの数が別々に格納されていないため、クエリは最適化されません。 テーブルから小さな列を選択し、その中の値の数を数えます。

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

この例では、 `count(DISTINCT num)` によって実行される。 `uniqExact` に従う機能 `count_distinct_implementation` 設定値。

## 任意(x) {#agg_function-any}

最初に検出された値を選択します。
クエリは、毎回異なる順序で実行することができるため、この関数の結果は不確定です。
確定的な結果を得るには、 ‘min’ または ‘max’ 関数の代わりに ‘any’.

場合によっては、実行順序に頼ることができます。 これは、order byを使用するサブクエリからのselectの場合に適用されます。

とき `SELECT` クエリには `GROUP BY` 句または少なくとも一つの集計関数、ClickHouse（MySQLとは対照的に）内のすべての式ということが必要です `SELECT`, `HAVING`、と `ORDER BY` 句は、キーまたは集計関数から計算されます。 つまり、テーブルから選択された各列は、キーまたは集計関数内で使用する必要があります。 MySQLのような動作を得るには、他の列を `any` 集計関数。

## anyHeavy(x) {#anyheavyx}

頻繁に発生する値を選択します。 [ヘビーヒッターズ](http://www.cs.umd.edu/~samir/498/karp.pdf) アルゴリズムだ 各クエリの実行スレッドのケースの半分を超える値がある場合は、この値が返されます。 通常、結果は非決定的です。

``` sql
anyHeavy(column)
```

**引数**

-   `column` – The column name.

**例えば**

を取る [オンタイム](../../getting-started/example-datasets/ontime.md) データセットと選択頻繁に発生する値で `AirlineID` コラム

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```

## anyllast(x) {#anylastx}

最後に検出された値を選択します。
結果は、次の場合と同様に不確定です `any` 機能。

## groupBitAnd {#groupbitand}

ビットごとの適用 `AND` 一連の数字のために。

``` sql
groupBitAnd(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt*` タイプ。

**例えば**

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

どこに `num` テストデータの列です。

結果:

``` text
binary     decimal
00000100 = 4
```

## groupBitOr {#groupbitor}

ビットごとの適用 `OR` 一連の数字のために。

``` sql
groupBitOr(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt*` タイプ。

**例えば**

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

どこに `num` テストデータの列です。

結果:

``` text
binary     decimal
01111101 = 125
```

## groupBitXor {#groupbitxor}

ビットごとの適用 `XOR` 一連の数字のために。

``` sql
groupBitXor(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt*` タイプ。

**例えば**

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

どこに `num` テストデータの列です。

結果:

``` text
binary     decimal
01101000 = 104
```

## groupBitmap {#groupbitmap}

符号なし整数列からのビットマップ計算または集計計算を行い、uint64型のカーディナリティを返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**パラメータ**

`expr` – An expression that results in `UInt*` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例えば**

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

## 最小(x) {#agg_function-min}

最小値を計算します。

## 最大(x) {#agg_function-max}

最大値を計算します。

## argMin(arg,val) {#agg-function-argmin}

を計算し ‘arg’ 最小値の値 ‘val’ 値。 いくつかの異なる値がある場合 ‘arg’ のための最小値 ‘val’ これらの値のうち、最初に検出された値が出力されます。

**例えば:**

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

## argMax(arg,val) {#agg-function-argmax}

を計算し ‘arg’ 最大値の値 ‘val’ 値。 いくつかの異なる値がある場合 ‘arg’ の最大値 ‘val’ これらの値のうち、最初に検出された値が出力されます。

## sum(x) {#agg_function-sum}

合計を計算します。
数字のためにのみ動作します。

## sumWithOverflow(x) {#sumwithoverflowx}

入力パラメーターの結果と同じデータ型を使用して、数値の合計を計算します。 合計がこのデータ型の最大値を超えると、関数はエラーを返します。

数字のためにのみ動作します。

## sumMap(キー,値) {#agg_functions-summap}

合計 ‘value’ 配列に指定されたキーに応じて ‘key’ 配列だ
の要素の数 ‘key’ と ‘value’ 合計される行ごとに同じでなければなりません。
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

例えば:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    )
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests)
FROM sum_map
GROUP BY timeslot
```

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │
└─────────────────────┴──────────────────────────────────────────────┘
```

## skewPop {#skewpop}

を計算します [歪み](https://en.wikipedia.org/wiki/Skewness) シーケンスの。

``` sql
skewPop(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**例えば**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## 串焼き {#skewsamp}

を計算します [サンプルの歪度](https://en.wikipedia.org/wiki/Skewness) シーケンスの。

これは、渡された値がそのサンプルを形成する場合、確率変数の歪度の不偏推定値を表します。

``` sql
skewSamp(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). もし `n <= 1` (`n` はサンプルのサイズです）、関数は次の値を返します `nan`.

**例えば**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## kurtPop {#kurtpop}

を計算します [尖度](https://en.wikipedia.org/wiki/Kurtosis) シーケンスの。

``` sql
kurtPop(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**例えば**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## kurtSamp {#kurtsamp}

を計算します [サンプル尖度](https://en.wikipedia.org/wiki/Kurtosis) のシーケンスです。

これは、渡された値がサンプルを形成する場合、確率変数の尖度の不偏推定値を表します。

``` sql
kurtSamp(expr)
```

**パラメータ**

`expr` — [式](../syntax.md#syntax-expressions) 番号を返す。

**戻り値**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). もし `n <= 1` (`n` はサンプルのサイズです）、関数は次の値を返します `nan`.

**例えば**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## timeSeriesGroupSum(uid,タイムスタンプ,値) {#agg-function-timeseriesgroupsum}

`timeSeriesGroupSum` 総異なる時系列のサンプルのタイムスタンプなアライメントを実施します。
これは、二つのサンプルタイムスタンプ間の線形補間を使用して、一緒に時系列を合計します。

-   `uid` タイムシリーズの一意のidです, `UInt64`.
-   `timestamp` ミリ秒またはマイクロ秒をサポートするためにInt64型です。
-   `value` メトリックです。

この関数は、以下のタプルの配列を返します `(timestamp, aggregated_value)` のペアになっています。

この機能を使用する前に確認 `timestamp` は昇順です。

例えば:

``` text
┌─uid─┬─timestamp─┬─value─┐
│ 1   │     2     │   0.2 │
│ 1   │     7     │   0.7 │
│ 1   │    12     │   1.2 │
│ 1   │    17     │   1.7 │
│ 1   │    25     │   2.5 │
│ 2   │     3     │   0.6 │
│ 2   │     8     │   1.6 │
│ 2   │    12     │   2.4 │
│ 2   │    18     │   3.6 │
│ 2   │    24     │   4.8 │
└─────┴───────────┴───────┘
```

``` sql
CREATE TABLE time_series(
    uid       UInt64,
    timestamp Int64,
    value     Float64
) ENGINE = Memory;
INSERT INTO time_series VALUES
    (1,2,0.2),(1,7,0.7),(1,12,1.2),(1,17,1.7),(1,25,2.5),
    (2,3,0.6),(2,8,1.6),(2,12,2.4),(2,18,3.6),(2,24,4.8);

SELECT timeSeriesGroupSum(uid, timestamp, value)
FROM (
    SELECT * FROM time_series order by timestamp ASC
);
```

結果は次のようになります:

``` text
[(2,0.2),(3,0.9),(7,2.1),(8,2.4),(12,3.6),(17,5.1),(18,5.4),(24,7.2),(25,2.5)]
```

## timeSeriesGroupRateSum(uid,ts,val) {#agg-function-timeseriesgroupratesum}

同様にtimeseriesgroupratesum、timeseriesgroupratesumは、時系列のレートを計算し、その後、一緒にレートを合計します。
また、この関数を使用する前にタイムスタンプが昇順になるはずです。

この関数を使用すると、上記の結果は次のようになります:

``` text
[(2,0),(3,0.1),(7,0.3),(8,0.3),(12,0.3),(17,0.3),(18,0.3),(24,0.3),(25,0.1)]
```

## 平均(x) {#agg_function-avg}

平均を計算します。
数字のためにのみ動作します。
結果は常にfloat64です。

## uniq {#agg_function-uniq}

引数の異なる値のおおよその数を計算します。

``` sql
uniq(x[, ...])
```

**パラメータ**

この関数は、可変個のパラメータを受け取ります。 変数は `Tuple`, `Array`, `Date`, `DateTime`, `String`、または数値型。

**戻り値**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ番号。

**実装の詳細**

機能:

-   集計内のすべてのパラメータのハッシュを計算し、それを計算に使用します。

-   を使用して適応サンプリングアルゴリズムです。 計算状態の場合、関数は65536までの要素ハッシュ値のサンプルを使用します。

        This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

-   結果を確定的に提供します（クエリ処理の順序に依存しません）。

使用をお勧めしますこの機能はほとんど全てのシナリオ.

**また見なさい**

-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [unihll12](#agg_function-uniqhll12)
-   [ユニキャック](#agg_function-uniqexact)

## uniqCombined {#agg_function-uniqcombined}

異なる引数値のおおよその数を計算します。

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

その `uniqCombined` 関数は、異なる値の数を計算するのに適しています。

**パラメータ**

この関数は、可変個のパラメータを受け取ります。 変数は `Tuple`, `Array`, `Date`, `DateTime`, `String`、または数値型。

`HLL_precision` は、2のセル数の底の対数です [ハイパーログ](https://en.wikipedia.org/wiki/HyperLogLog). オプションで、次のように関数を使用できます `uniqCombined(x[, ...])`. のデフォルト値 `HLL_precision` は17で、これは効果的に96KiBのスペース（2^17セル、6ビットそれぞれ）です。

**戻り値**

-   を番号 [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ番号。

**実装の詳細**

機能:

-   ハッシュを計算します(64ビットのハッシュ `String` それ以外の場合は32ビット）は、集計内のすべてのパラメータに対して、それを計算に使用します。

-   配列、ハッシュテーブル、およびhyperloglogとエラー修正テーブルの組み合わせを使用します。

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   結果を確定的に提供します（クエリ処理の順序に依存しません）。

!!! note "メモ"
    それは32ビットハッシュを使用しているので-`String` タイプすると、結果はカーディナリティのエラーが非常に大きくなります `UINT_MAX` （エラーは数十億の異なる値の後にすぐに発生します）、この場合は次のようにしてください [uniqCombined64](#agg_function-uniqcombined64)

に比べて [uniq](#agg_function-uniq) 機能、を `uniqCombined`:

-   数回少ないメモリを消費します。
-   数倍高い精度で計算します。
-   通常は若干低い性能を持っています。 一部のシナリオでは, `uniqCombined` より良い実行できる `uniq` たとえば、ネットワークを介して多数の集約状態を送信する分散クエリを使用します。

**また見なさい**

-   [uniq](#agg_function-uniq)
-   [uniqCombined64](#agg_function-uniqcombined64)
-   [unihll12](#agg_function-uniqhll12)
-   [ユニキャック](#agg_function-uniqexact)

## uniqCombined64 {#agg_function-uniqcombined64}

と同じ [uniqCombined](#agg_function-uniqcombined) ただし、すべてのデータ型に64ビットハッシュを使用します。

## unihll12 {#agg_function-uniqhll12}

を使用して、異なる引数値のおおよその数を計算します [ハイパーログ](https://en.wikipedia.org/wiki/HyperLogLog) アルゴリズムだ

``` sql
uniqHLL12(x[, ...])
```

**パラメータ**

この関数は、可変個のパラメータを受け取ります。 変数は `Tuple`, `Array`, `Date`, `DateTime`, `String`、または数値型。

**戻り値**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ番号。

**実装の詳細**

機能:

-   集計内のすべてのパラメータのハッシュを計算し、それを計算に使用します。

-   HyperLogLogアルゴリズムを使用して、異なる引数値の数を近似します。

        212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   確定的な結果を提供します（クエリ処理の順序に依存しません）。

この機能を使用することはお勧めしません。 ほとんどの場合、 [uniq](#agg_function-uniq) または [uniqCombined](#agg_function-uniqcombined) 機能。

**また見なさい**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [ユニキャック](#agg_function-uniqexact)

## ユニキャック {#agg_function-uniqexact}

異なる引数値の正確な数を計算します。

``` sql
uniqExact(x[, ...])
```

を使用 `uniqExact` 機能あなたは絶対に正確な結果が必要な場合。 それ以外の場合は、 [uniq](#agg_function-uniq) 機能。

その `uniqExact` 機能の使用ます。 `uniq`、状態のサイズは、異なる値の数が増加するにつれて無制限の成長を有するからである。

**パラメータ**

この関数は、可変個のパラメータを受け取ります。 変数は `Tuple`, `Array`, `Date`, `DateTime`, `String`、または数値型。

**また見なさい**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [unihll12](#agg_function-uniqhll12)

## groupArray(x),groupArray(max\_size)(x) {#agg_function-grouparray}

引数の値の配列を作成します。
値は、任意の（不確定な）順序で配列に追加できます。

第二のバージョン（ `max_size` パラメータ）結果の配列のサイズを次のように制限します `max_size` 要素。
例えば, `groupArray (1) (x)` に相当します `[any (x)]`.

場合によっては、実行の順序に依拠することもできます。 これは、次の場合に適用されます `SELECT` 使用するサブクエリーから取得されます `ORDER BY`.

## groupArrayInsertAt(値、位置) {#grouparrayinsertatvalue-position}

指定した位置の配列に値を挿入します。

!!! note "メモ"
    この関数はゼロベースの位置を使用します。

Accepts the value and position as input. If several values ​​are inserted into the same position, any of them might end up in the resulting array (the first one will be used in the case of single-threaded execution). If no value is inserted into a position, the position is assigned the default value.

任意変数:

-   空の位置に置き換えるためのデフォルト値。
-   結果の配列の長さ。 これにより、すべての集約キーで同じサイズの配列を受け取ることができます。 このパラメーターを使用する場合は、既定値を指定する必要があります。

## グルーパーレイモビングサムcity in new mexico usa {#agg_function-grouparraymovingsum}

入力値の移動和を計算します。

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

この機能できるウィンドウサイズとしてのパラメータとします。 指定しない場合、この関数は、列の行数と同じウィンドウサイズをとります。

**パラメータ**

-   `numbers_for_summing` — [式](../syntax.md#syntax-expressions) その結果、数値データ型の値が返されます。
-   `window_size` — Size of the calculation window.

**戻り値**

-   入力データと同じサイズおよびタイプの配列。

**例えば**

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

## groupparraymovingavg {#agg_function-grouparraymovingavg}

入力値の移動平均を計算します。

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

この機能できるウィンドウサイズとしてのパラメータとします。 指定しない場合、この関数は、列の行数と同じウィンドウサイズをとります。

**パラメータ**

-   `numbers_for_summing` — [式](../syntax.md#syntax-expressions) その結果、数値データ型の値が返されます。
-   `window_size` — Size of the calculation window.

**戻り値**

-   入力データと同じサイズおよびタイプの配列。

この関数は [ゼロに向かって丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). 結果のデータ型の小数点以下の桁を切り捨てます。

**例えば**

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

## groupUniqArray(x),groupUniqArray(max\_size)(x) {#groupuniqarrayx-groupuniqarraymax-sizex}

異なる引数値から配列を作成します。 メモリ消費量は、 `uniqExact` 機能。

第二のバージョン（ `max_size` パラメータ）結果の配列のサイズを次のように制限します `max_size` 要素。
例えば, `groupUniqArray(1)(x)` に相当します `[any(x)]`.

## 分位値 {#quantile}

近似値を計算します [分位値](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンス。

この関数が適用されます [貯蔵所の見本抽出](https://en.wikipedia.org/wiki/Reservoir_sampling) 8192までの貯蔵所のサイズおよび見本抽出のための乱数発電機を使って。 結果は非決定的です。 正確な分位値を取得するには、以下を使用します [quantileExact](#quantileexact) 機能。

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantile(level)(expr)
```

エイリアス: `median`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   指定したレベルの概算値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型の入力。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantiedeterministic {#quantiledeterministic}

近似値を計算します [分位値](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンス。

この関数が適用されます [貯蔵所の見本抽出](https://en.wikipedia.org/wiki/Reservoir_sampling) 8192までの貯蔵所のサイズおよび見本抽出の決定論のアルゴリズムを使って。 結果は決定的です。 正確な分位値を取得するには、以下を使用します [quantileExact](#quantileexact) 機能。

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileDeterministic(level)(expr, determinator)
```

エイリアス: `medianDeterministic`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).
-   `determinator` — Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occures too often, the function works incorrectly.

**戻り値**

-   指定したレベルの概算値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型の入力。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileExact {#quantileexact}

正確に計算する [分位値](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンス。

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` メモリ、どこ `n` 渡された値の数です。 しかし、少数の値の場合、関数は非常に効果的です。

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileExact(level)(expr)
```

エイリアス: `medianExact`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   指定されたレベルの分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型の入力。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileExactWeighted {#quantileexactweighted}

正確に計算する [分位値](https://en.wikipedia.org/wiki/Quantile) 各要素の重みを考慮した数値データシーケンス。

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [quantileExact](#quantileexact). この関数は、次の代わりに使用できます `quantileExact` そして、重み1を指定します。

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileExactWeighted(level)(expr, weight)
```

エイリアス: `medianExactWeighted`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence members. Weight is a number of value occurrences.

**戻り値**

-   指定されたレベルの分位値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型の入力。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## クオンタイミング {#quantiletiming}

決定された精度では、 [分位値](https://en.wikipedia.org/wiki/Quantile) 数値データシーケンス。

結果は決定的です（クエリ処理の順序に依存しません）。 この機能を最適化と配列における分布のような積載ウェブページではバックエンド対応。

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTiming(level)(expr)
```

エイリアス: `medianTiming`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).

-   `expr` — [式](../syntax.md#syntax-expressions) aを返す列の値を超える [フロート\*](../../sql-reference/data-types/float.md)-タイプ番号。

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**精度**

計算は次の場合に正確です:

-   値の総数は5670を超えません。
-   値の総数は5670を超えていますが、ページの読み込み時間は1024ms未満です。

それ以外の場合、計算の結果は16msの最も近い倍数に丸められます。

!!! note "メモ"
    ページの読み込み時間の分位数を計算するために、この関数はより効果的で正確です [分位値](#quantile).

**戻り値**

-   指定されたレベルの分位値。

タイプ: `Float32`.

!!! note "メモ"
    関数に値が渡されない場合（以下を使用する場合 `quantileTimingIf`), [ナン](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 返されます。 この目的は、これらのケースをゼロになるケースと区別することです。 見る [ORDER BY句](../statements/select.md#select-order-by) ソートに関する注意事項 `NaN` 値。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantitimingweighted {#quantiletimingweighted}

決定された精度では、 [分位値](https://en.wikipedia.org/wiki/Quantile) 各シーケンスメンバの重みに応じた数値データシーケンス。

結果は決定的です（クエリ処理の順序に依存しません）。 この機能を最適化と配列における分布のような積載ウェブページではバックエンド対応。

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

エイリアス: `medianTimingWeighted`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).

-   `expr` — [式](../syntax.md#syntax-expressions) aを返す列の値を超える [フロート\*](../../sql-reference/data-types/float.md)-タイプ番号。

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**精度**

計算は次の場合に正確です:

-   値の総数は5670を超えません。
-   値の総数は5670を超えていますが、ページの読み込み時間は1024ms未満です。

それ以外の場合、計算の結果は16msの最も近い倍数に丸められます。

!!! note "メモ"
    ページの読み込み時間の分位数を計算するために、この関数はより効果的で正確です [分位値](#quantile).

**戻り値**

-   指定されたレベルの分位値。

タイプ: `Float32`.

!!! note "メモ"
    関数に値が渡されない場合（以下を使用する場合 `quantileTimingIf`), [ナン](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 返されます。 この目的は、これらのケースをゼロになるケースと区別することです。 見る [ORDER BY句](../statements/select.md#select-order-by) ソートに関する注意事項 `NaN` 値。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantiletdigestcomment {#quantiletdigest}

近似値を計算します [分位値](https://en.wikipedia.org/wiki/Quantile) を使用する数値データシーケンスの [t-ダイジェスト](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) アルゴリズムだ

最大誤差は1%です。 メモリ消費量は `log(n)`、どこ `n` 値の数です。 結果は、クエリの実行順序によって異なり、非決定的です。

機能の性能は性能より低いですの [分位値](#quantile) または [クオンタイミング](#quantiletiming). 状態サイズと精度の比に関しては、この関数はよりもはるかに優れています `quantile`.

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTDigest(level)(expr)
```

エイリアス: `medianTDigest`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   指定したレベルの概算値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型の入力。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

近似値を計算します [分位値](https://en.wikipedia.org/wiki/Quantile) を使用する数値データシーケンスの [t-ダイジェスト](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) アルゴリズムだ この関数は、各シーケンスメンバーの重みを考慮に入れます。 最大誤差は1%です。 メモリ消費量は `log(n)`、どこ `n` 値の数です。

機能の性能は性能より低いですの [分位値](#quantile) または [クオンタイミング](#quantiletiming). 状態サイズと精度の比に関しては、この関数はよりもはるかに優れています `quantile`.

結果は、クエリの実行順序によって異なり、非決定的です。

複数を使用する場合 `quantile*` クエリの異なるレベルを持つ関数は、内部状態が結合されていません（つまり、クエリはそれほど効率的ではありません）。 この場合は、 [分位数](#quantiles) 機能。

**構文**

``` sql
quantileTDigest(level)(expr)
```

エイリアス: `medianTDigest`.

**パラメータ**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` の範囲内の値 `[0.01, 0.99]`. デフォルト値:0.5. で `level=0.5` 機能は計算する [中央値](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [データ型](../../sql-reference/data-types/index.md#data_types), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**戻り値**

-   指定したレベルの概算値。

タイプ:

-   [Float64](../../sql-reference/data-types/float.md) 数値データ型の入力。
-   [日付](../../sql-reference/data-types/date.md) 入力値が `Date` タイプ。
-   [DateTime](../../sql-reference/data-types/datetime.md) 入力値が `DateTime` タイプ。

**例えば**

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

**また見なさい**

-   [中央値](#median)
-   [分位数](#quantiles)

## 中央値 {#median}

その `median*` 関数は、対応する関数のエイリアスです `quantile*` 機能。 数値データサンプルの中央値を計算します。

機能:

-   `median` — Alias for [分位値](#quantile).
-   `medianDeterministic` — Alias for [quantiedeterministic](#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](#quantileexact).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](#quantileexactweighted).
-   `medianTiming` — Alias for [クオンタイミング](#quantiletiming).
-   `medianTimingWeighted` — Alias for [quantitimingweighted](#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantiletdigestcomment](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](#quantiletdigestweighted).

**例えば**

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

すべての分位数関数には、対応する分位数関数もあります: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. これらの関数は、あるパスでリストされたレベルのすべての分位数を計算し、結果の値の配列を返します。

## varSamp(x) {#varsampx}

金額を計算します `Σ((x - x̅)^2) / (n - 1)`、どこ `n` サンプルサイズは `x̅`の平均値です `x`.

これは、渡された値がそのサンプルを形成する場合、確率変数の分散の不偏推定値を表します。

を返します `Float64`. とき `n <= 1`、戻り値 `+∞`.

## varPop(x) {#varpopx}

金額を計算します `Σ((x - x̅)^2) / n`、どこ `n` サンプルサイズは `x̅`の平均値です `x`.

つまり、値のセットの分散。 を返します `Float64`.

## stddevSamp(x) {#stddevsampx}

結果はの平方根に等しい `varSamp(x)`.

## stddevPop(x) {#stddevpopx}

結果はの平方根に等しい `varPop(x)`.

## topK(N)(x) {#topknx}

指定された列のほぼ最も頻繁に使用される値の配列を返します。 結果の配列は、値のおおよその頻度の降順でソートされます（値そのものではありません）。

実装する [ろ過されたスペース節約](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf) からのreduce-and-combineアルゴリズムに基づいてTopKを分析するアルゴリズム [パラレル省スペース](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

この関数は保証された結果を提供しません。 特定の状況では、エラーが発生し、最も頻度の高い値ではない頻繁な値が返されることがあります。

私達は使用を推薦します `N < 10` 価値;性能は大きいと減ります `N` 値。 の最大値 `N = 65536`.

**パラメータ**

-   ‘N’ 返す要素の数です。

パラメーターを省略すると、既定値10が使用されます。

**引数**

-   ’ x ’ – The value to calculate frequency.

**例えば**

を取る [オンタイム](../../getting-started/example-datasets/ontime.md) データセットを選択し、最も頻繁に発生する三つの値を選択します。 `AirlineID` コラム

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

## トップクイット {#topkweighted}

に似て `topK` しかし、整数型の一つの追加の引数を取ります - `weight`. あらゆる価値は説明されます `weight` 頻度計算のための時間。

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

最大およその重みの合計を持つ値の配列を返します。

**例えば**

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

## covarSamp(x,y) {#covarsampx-y}

の値を計算します `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Float64を返します。 とき `n <= 1`, returns +∞.

## covarPop(x,y) {#covarpopx-y}

の値を計算します `Σ((x - x̅)(y - y̅)) / n`.

## corr(x,y) {#corrx-y}

ピアソン相関係数を計算します: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

## categoricalInformationValue {#categoricalinformationvalue}

の値を計算します `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` 各カテゴリの。

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

結果は、離散(カテゴリカル)フィーチャがどのようにして `[category1, category2, ...]` の値を予測する学習モデルに貢献する `tag`.

## simplelearregression {#simplelinearregression}

単純な(一次元的な)線形回帰を実行します。

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

この関数は、確率的線形回帰を実装します。 それは率、l2正則化係数、ミニバッチサイズを学ぶための注文変数を支え、重量を更新するための少数の方法を有する ([アダム](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) （デフォルトで使用), [シンプルSGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [勢い](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [ネステロフ](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### パラメータ {#agg_functions-stochasticlinearregression-parameters}

が4カスタマイズ可能パラメータ。 それらは関数に順番に渡されますが、すべての四つのデフォルト値を渡す必要はありませんが、良いモデルにはいくつかのパラメータ調整が必要で

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` 勾配降下ステップが実行されるときのステップ長の係数です。 大きすぎる学習率の原因となり無限の量のモデルです。 デフォルトは `0.00001`.
2.  `l2 regularization coefficient` これは過食防止に役立つ可能性があります。 デフォルトは `0.1`.
3.  `mini-batch size` グラデーションディセントのステップを実行するために、グラデーションを計算して合計する要素の数を設定します。 純粋な確率降下は一つの要素を使用しますが、小さなバッチ（約10の要素）を持つことで勾配ステップがより安定します。 デフォルトは `15`.
4.  `method for updating weights`、彼らは: `Adam` （デフォルトでは), `SGD`, `Momentum`, `Nesterov`. `Momentum` と `Nesterov` もう少し計算とメモリが必要ですが、確率勾配法の収束と安定性の点で有用です。

### 使い方 {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` モデルのフィッティングと新しいデータの予測です。 モデルを適合させ、後で使用するためにその状態を保存するために、 `-State` 基本的に状態（モデルの重みなど）を保存するcombinator。
予測するには、関数を使用します [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod) これは、状態を予測する機能と同様に引数として取ります。

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** 継手

このようなクエリを使用できます。

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

ここでは、データを挿入する必要もあります `train_data` テーブル。 パラメータの数は固定されていません。 `linearRegressionState`. 彼らはすべての必要数値です。
ターゲット値（予測することを学びたい）を持つ列が最初の引数として挿入されることに注意してください。

**2.** 予測

状態をテーブルに保存した後、予測に複数回使用したり、他の状態とマージして新しいモデルを作成したりすることもできます。

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

クエリは予測値の列を返します。 その最初の引数に注意してください `evalMLMethod` は `AggregateFunctionState` オブジェクト、次はフィーチャの列です。

`test_data` のようなテーブルです `train_data` が含まれないことがあります。

### 備考 {#agg_functions-stochasticlinearregression-notes}

1.  統合モデルにはユーザーの作成などのクエリ:
    `sql  SELECT state1 + state2 FROM your_models`
    どこに `your_models` テーブルの両方のモデルです。 このクエリはnewを返します `AggregateFunctionState` オブジェクト。

2.  ユーザーのフェッチのウエイトを作成したモデルとして独自の目的で保存しないモデルについていない場合 `-State` combinatorが使用されます。
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    そのようなクエリはモデルに適合し、その重みを返します-最初はモデルのパラメータに対応する重みです。 したがって、上記の例では、クエリは3つの値を持つ列を返します。

**また見なさい**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [線形およびロジスティック回帰の違い](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

この関数は、確率論的ロジスティック回帰を実装します。 これは、バイナリ分類問題に使用することができ、stochasticlinearregressionと同じカスタムパラメータをサポートし、同じ方法で動作します。

### パラメータ {#agg_functions-stochasticlogisticregression-parameters}

パラメーターは、stochasticlinearregressionとまったく同じです:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
詳細については、 [パラメータ](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  継手

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

**また見なさい**

-   [stochasticLinearRegression](#agg_functions-stochasticlinearregression)
-   [線形およびロジスティック回帰の違い。](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## groupbitmapandgenericname {#groupbitmapand}

ビットマップ列のandを計算し、型uint64のカーディナリティを返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**パラメータ**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例えば**

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

ビットマップ列のorを計算し、型uint64のカーディナリティを返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md). これは `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**パラメータ**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例えば**

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

ビットマップ列のxorを計算し、uint64型のカーディナリティを返します。 [ビットマップ](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**パラメータ**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` タイプ。

**戻り値**

の値 `UInt64` タイプ。

**例えば**

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
