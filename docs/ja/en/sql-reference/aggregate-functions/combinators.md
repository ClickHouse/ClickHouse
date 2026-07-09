---
description: '集約関数コンビネータのドキュメント'
sidebar_label: '集約関数コンビネータ'
sidebar_position: 37
slug: /sql-reference/aggregate-functions/combinators
title: '集約関数コンビネータ'
doc_type: 'reference'
---

集約関数の名前には接尾辞を追加できます。これにより、集約関数の動作が変わります。

<div id="-if">
  ## -If
</div>

接尾辞 `-If` は、任意の集約関数の名前に追加できます。この場合、集約関数は追加の引数、つまり条件 (Uint8 型) を受け取ります。集約関数は、その条件を満たす行だけを処理します。条件が一度も満たされなかった場合は、デフォルト値 (通常は 0 または空文字列) を返します。

例: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` など。

条件付き集約関数を使うと、サブクエリや `JOIN` を使わずに、複数の条件に対する集約を一度に計算できます。たとえば、条件付き集約関数はセグメント比較機能の実装に使用できます。

<div id="-array">
  ## -Array
</div>

-Array 接尾辞は、任意の 集約関数 に追加できます。この場合、集約関数 は &#39;T&#39; 型の argument ではなく、&#39;Array(T)&#39; 型 (配列) の argument を受け取ります。集約関数 が複数の argument を受け取る場合、それらは長さが等しい配列でなければなりません。配列を処理する際、集約関数 は元の 集約関数 と同様に、すべての配列要素に対して動作します。

例 1: `sumArray(arr)` - すべての &#39;arr&#39; 配列の全要素を合計します。この例は、より簡潔に `sum(arraySum(arr))` と書くこともできます。

例 2: `uniqArray(arr)` – すべての &#39;arr&#39; 配列に含まれる一意の要素数を数えます。これは `uniq(arrayJoin(arr))` とすることで、より簡単に実現することもできますが、クエリに &#39;arrayJoin&#39; を追加できるとは限りません。

-If と -Array は組み合わせて使用できます。ただし、&#39;Array&#39; を先に、その後に &#39;If&#39; を付ける必要があります。例: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`。この順序のため、&#39;cond&#39; argument は配列にはなりません.

<div id="-map">
  ## -Map
</div>

接尾辞 -Map は、任意の集約関数に付加できます。これにより、引数として Map 型を受け取り、指定した集約関数を使ってマップの各キーの値を個別に集約する集約関数が作成されます。結果も Map 型になります。

**例**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = MergeTree
ORDER BY ();

INSERT INTO map_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', (['a', 'b', 'c'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', (['c', 'd', 'e'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['d', 'e', 'f'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['f', 'g', 'g'], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(status),
    avgMap(status),
    minMap(status)
FROM map_map
GROUP BY timeslot;

┌────────────timeslot─┬─sumMap(status)───────────────────────┬─avgMap(status)───────────────────────┬─minMap(status)───────────────────────┐
│ 2000-01-01 00:00:00 │ {'a':10,'b':10,'c':20,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │
│ 2000-01-01 00:01:00 │ {'d':10,'e':10,'f':20,'g':20}        │ {'d':10,'e':10,'f':10,'g':10}        │ {'d':10,'e':10,'f':10,'g':10}        │
└─────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
```

<div id="-simplestate">
  ## -SimpleState
</div>

この コンビネータ を適用すると、集約関数 は同じ値を返しますが、型は異なります。これは [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md) で、[AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) テーブルで使用するためにテーブルへ格納できます。

**構文**

```sql
<aggFunction>SimpleState(x)
```

**引数**

* `x` — 集約関数のパラメータ。

**戻り値**

型が `SimpleAggregateFunction(...)` の集約関数の値。

**例**

```sql title="Query"
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

```text title="Response"
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

<div id="-state">
  ## -State
</div>

この コンビネータ を適用すると、集約関数 は結果の値 (たとえば [uniq](/ja/sql-reference/aggregate-functions/reference/uniq) 関数における一意な値の数) を返さず、集約の中間状態を返します (`uniq` の場合、これは一意な値の数を計算するための hash table です) 。これは `AggregateFunction(...)` で、後続の処理に使用したり、後で集約を完了するために table に保存したりできます。

:::note
-MapState は、同じ data に対して不変ではないことに注意してください。これは、中間状態における data の順序が変わる可能性があるためですが、この data の取り込みには影響しません。
:::

これらの状態を扱うには、次を使用します。

* [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) table engine。
* [finalizeAggregation](/ja/sql-reference/functions/other-functions#finalizeAggregation) function。
* [runningAccumulate](../../sql-reference/functions/other-functions.md#runningAccumulate) function。
* [-Merge](#-merge) コンビネータ。
* [-MergeState](#-mergestate) コンビネータ。

<div id="-merge">
  ## -Merge
</div>

このコンビネータを適用すると、集約関数は中間の集約状態を引数に取り、それらの状態を結合して集約を完了し、結果の値を返します。

<div id="-mergestate">
  ## -MergeState
</div>

`-Merge` コンビネータ と同じ方法で中間の集約状態をマージします。ただし、結果の値は返さず、`-State` コンビネータ と同様に中間の集約状態を返します。

<div id="-foreach">
  ## -ForEach
</div>

table用の集約関数を、対応する配列要素を集約し、結果を配列として返す配列用の集約関数に変換します。たとえば、配列 `[1, 2]`、`[3, 4, 5]`、`[6, 7]` に対する `sumForEach` は、対応する配列要素を加算し、その結果として `[10, 13, 5]` を返します。

<div id="-tuple">
  ## -Tuple
</div>

`-Tuple` 接尾辞は、任意の集約関数に追加できます。結合後の関数は、元の集約関数の各argumentに対して `Tuple` 型のargumentを 1 つ受け取ります。すべてのタプルは同じ数の要素を持っている必要があります。集約は各要素の位置ごとに独立して適用され、各 `Tuple` から対応する要素を受け取り、結果として `Tuple` を返します。

最初の入力 `Tuple` に明示的な要素名がある場合、それらは結果でも保持されます。

`NULL` 値を自前で処理する集約関数 (`anyRespectNulls`、`anyLastRespectNulls`、`RESPECT NULLS` modifier) は、argumentとして `Nullable(Tuple(...))` 型をサポートしません。代わりに、要素に `Nullable` を使用してください。

**Syntax**

```sql
<aggFunction>Tuple(tuple1[, tuple2, ...])
```

**引数**

* `tuple1[, tuple2, ...]` — `Tuple` 型のカラムです。基になる集約関数の各引数に対応するカラムを 1 つずつ指定します。すべて同じ要素数である必要があります。各要素は、その引数位置で基になる集約関数がサポートする型でなければなりません。

**戻り値**

* 各要素に集約関数を個別に適用した結果を含む `Tuple`。

型: `Tuple(aggFunction(element1), aggFunction(element2), ...)`.

**例**

クエリ:

```sql
SELECT sumTuple(t) FROM
(
    SELECT tuple(toInt64(1), toFloat64(2.5)) AS t
    UNION ALL
    SELECT tuple(toInt64(3), toFloat64(4.5))
    UNION ALL
    SELECT tuple(toInt64(5), toFloat64(6.5))
);
```

結果:

```text
┌─sumTuple(t)─┐
│ (9,13.5)    │
└─────────────┘
```

`GROUP BY` と併用する場合：

```sql
SELECT
    k,
    avgTuple(t)
FROM
(
    SELECT
        number % 2 AS k,
        tuple(toInt64(number), toFloat64(number) * 1.5) AS t
    FROM numbers(6)
)
GROUP BY k
ORDER BY k;
```

```text
┌─k─┬─avgTuple(t)─┐
│ 0 │ (2,3)       │
│ 1 │ (3,4.5)     │
└───┴─────────────┘
```

多引数の 集約関数 と組み合わせて使用する場合、各 `Tuple` 引数は基底となる関数の 1 つの引数に対応し、要素は位置ごとに対応付けられます:

```text
corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))
```

```sql
SELECT corrTuple((a1, a2), (b1, b2))
FROM
(
    SELECT
        toFloat64(number) AS a1,
        toFloat64(number * 2) AS a2,
        toFloat64(100 - number) AS b1,
        toFloat64(number * 3) AS b2
    FROM numbers(10)
);
```

```text
┌─corrTuple((a1, a2), (b1, b2))─┐
│ (-1,1)                        │
└───────────────────────────────┘
```

`a1` と `b1` は逆相関にあり、`a2` と `b2` は比例関係にあるため、結果は `(-1, 1)` になります。

`-Tuple` は `-If` などの他の集約関数コンビネータと組み合わせることができます。たとえば、`sumTupleIf(tuple_column, cond)` のように使用できます。

<div id="-distinct">
  ## -Distinct
</div>

引数の一意な組み合わせは、それぞれ1回だけ集計されます。重複する値は無視されます。
例: `sum(DISTINCT x)` (または `sumDistinct(x)`) 、`groupArray(DISTINCT x)` (または `groupArrayDistinct(x)`) 、`corrStable(DISTINCT x, y)` (または `corrStableDistinct(x, y)`) など。

<div id="-ordefault">
  ## -OrDefault
</div>

集約関数の動作を変更します。

集約関数に入力値がない場合、このコンビネータを使うと、戻り値のデータ型に対応するデフォルト値を返します。空の入力データを受け取れる集約関数に適用できます。

`-OrDefault` は他のコンビネータと組み合わせて使用できます。

**構文**

```sql
<aggFunction>OrDefault(x)
```

**引数**

* `x` — 集約関数のパラメータ。

**戻り値**

集約する対象がない場合、集約関数の戻り値の型のデフォルト値を返します。

型は、使用する集約関数によって異なります。

**例**

```sql title="Query"
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

```text title="Response"
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

また、`-OrDefault` は他の集約関数コンビネータと組み合わせて使うこともできます。これは、集約関数が空の入力を受け付けない場合に便利です。

```sql title="Query"
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

<div id="-ornull">
  ## -OrNull
</div>

集約関数の動作を変更します。

この集約関数コンビネータは、集約関数の結果を [Nullable](../../sql-reference/data-types/nullable.md) データ型に変換します。集約関数に計算対象の値がない場合は、[NULL](/ja/operations/settings/formats#input_format_null_as_default) を返します。

`-OrNull` は、他の集約関数コンビネータと組み合わせて使用できます。

**構文**

```sql
<aggFunction>OrNull(x)
```

**引数**

* `x` — 集約関数のパラメーター。

**戻り値**

* 集約関数の結果を `Nullable` データ型に変換した値。
* 集約する値がない場合は `NULL`。

型: `Nullable(aggregate function return type)`.

**例**

集約関数の末尾に `-orNull` を追加します。

```sql title="Query"
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

```text title="Response"
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

また、`-OrNull` はほかの集約関数コンビネータと組み合わせて使うこともできます。これは、集約関数が空の入力を受け付けない場合に便利です。

```sql title="Query"
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

<div id="-resample">
  ## -Resample
</div>

データをグループに分け、各グループ内のデータを個別に集約できます。グループは、1 つのカラムの値をインターバルごとに分割して作成されます。

```sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**引数**

* `start` — `resampling_key` の値に対して必要な区間全体の開始値。
* `stop` — `resampling_key` の値に対して必要な区間全体の終了値。区間全体には `stop` の値は含まれません `[start, stop)`。
* `step` — 区間全体を部分区間に分割する間隔。`aggFunction` は各部分区間ごとに独立して実行されます。
* `resampling_key` — その値を使ってデータを区間に分割するカラム。
* `aggFunction_params` — `aggFunction` のパラメーター。

**戻り値**

* 各部分区間に対する `aggFunction` の結果を格納した Array。

**例**

次のデータを持つ `people` テーブルについて考えます。

```text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

年齢が `[30,60)` および `[60,75)` のインターバルに含まれる人の名前を取得してみましょう。年齢は整数で表しているため、実際に対象となる年齢範囲は `[30, 59]` および `[60,74]` のインターバルになります。

名前を配列に集約するには、[groupArray](/ja/sql-reference/aggregate-functions/reference/grouparray) 集約関数を使用します。これは 1 つの引数を取ります。この場合は `name` カラムです。`groupArrayResample` 関数では、年齢ごとに名前を集約するために `age` カラムを使用します。必要なインターバルを定義するには、`groupArrayResample` 関数に `30, 75, 30` という引数を渡します。

```sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

```text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

結果を確認してください。

`John` は若すぎるため、サンプルから除外されます。その他の人は、指定された年齢区間に応じて分布します。

では、指定された年齢区間ごとの人数の合計と平均賃金を集計してみましょう。

```sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

```text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

<div id="-argmin">
  ## -ArgMin
</div>

接尾辞 -ArgMin は、任意の集約関数名の末尾に追加できます。この場合、集約関数は追加の引数を受け取り、その引数には比較可能な任意の式を指定する必要があります。集約関数は、指定した追加の式に対して最小値を持つ行だけを処理します。

例: `sumArgMin(column, expr)`, `countArgMin(expr)`, `avgArgMin(x, expr)` などです。

<div id="-argmax">
  ## -ArgMax
</div>

接尾辞 -ArgMin と同様ですが、指定された追加の式で最大値となる行のみを処理します。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseでArray、Map、stateに集約関数コンビネータを使う](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)