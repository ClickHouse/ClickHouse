---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 37
toc_title: "\u96C6\u8A08\u95A2\u6570\u306E\u30B3\u30F3\u30D3\u30CD\u30FC\u30BF"
---

# 集計関数のコンビネータ {#aggregate_functions_combinators}

集計関数の名前には、それに接尾辞を付けることができます。 これにより、集計関数の動作方法が変更されます。

## -もし {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

例: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` というように。

条件付集計関数を使用すると、サブクエリを使用せずに複数の条件の集計を一度に計算できます。 `JOIN`例えば、Yandexの中。Metrica、条件付き集約関数は、セグメント比較機能を実装するために使用されます。

## -配列 {#agg-functions-combinator-array}

-arrayサフィックスは、任意の集計関数に追加できます。 この場合、aggregate関数は次の引数を取ります ‘Array(T)’ 代わりにタイプ（配列） ‘T’ 型引数。 集計関数が複数の引数を受け入れる場合、これは同じ長さの配列でなければなりません。 配列を処理する場合、aggregate関数は、すべての配列要素にわたって元の集計関数と同様に機能します。

例1: `sumArray(arr)` -すべてのすべての要素を合計します ‘arr’ 配列だ この例では、より簡単に書かれている可能性があります: `sum(arraySum(arr))`.

例2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ 配列だ これは簡単な方法で行うことができます: `uniq(arrayJoin(arr))` しかし、それは常に追加することはできません ‘arrayJoin’ クエリに。

-Ifと-配列を組み合わせることができます。 しかし, ‘Array’ 第一だから、その ‘If’. 例: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. この順序のために、 ‘cond’ 引数は配列ではありません。

## -状態 {#agg-functions-combinator-state}

このコンビネーターを適用すると、集計関数は結果の値を返しません(たとえば、このコンビネーターの一意の値の数など)。 [uniq](reference.md#agg_function-uniq) の中間状態である。 `uniq`、これは一意の値の数を計算するためのハッシュテーブルです）。 これは `AggregateFunction(...)` これをさらなる処理に使用したり、テーブルに格納して後で集計を完了することができます。

これらの国は、利用:

-   [ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹ](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) テーブルエンジン。
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) 機能。
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#function-runningaccumulate) 機能。
-   [-マージ](#aggregate_functions_combinators-merge) コンビネータ
-   [-MergeState](#aggregate_functions_combinators-mergestate) コンビネータ

## -マージ {#aggregate_functions_combinators-merge}

このコンビネーターを適用すると、aggregate関数は中間の集約状態を引数として受け取り、状態を結合して集計を終了し、結果の値を返します。

## -MergeState {#aggregate_functions_combinators-mergestate}

-mergeコンビネータと同じ方法で中間の集約状態をマージします。 しかし、結果の値を返すのではなく、-stateコンビネータに似た中間の集約状態を返します。

## -ForEach {#agg-functions-combinator-foreach}

テーブルの集計関数を、対応する配列項目を集約して結果の配列を返す配列の集計関数に変換します。 例えば, `sumForEach` 配列の場合 `[1, 2]`, `[3, 4, 5]`と`[6, 7]`結果を返します `[10, 13, 5]` 対応する配列項目を一緒に追加した後。

## -オルデフォルト {#agg-functions-combinator-ordefault}

集約する値が何もない場合は、集計関数の戻り値のデフォルト値を設定します。

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

## -オルヌル {#agg-functions-combinator-ornull}

塗りつぶし `null` 集計するものがない場合。 戻り列はnull可能になります。

``` sql
SELECT avg(number), avgOrNull(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrNull(number)─┐
│         nan │              ᴺᵁᴸᴸ │
└─────────────┴───────────────────┘
```

-OrDefaultと-OrNullは他のコンビネータと組み合わせることができます。 これは、集計関数が空の入力を受け入れない場合に便利です。

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## -リサンプル {#agg-functions-combinator-resample}

データをグループに分割し、それらのグループのデータを個別に集計できます。 グループは、ある列の値を間隔に分割することによって作成されます。

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**パラメータ**

-   `start` — Starting value of the whole required interval for `resampling_key` 値。
-   `stop` — Ending value of the whole required interval for `resampling_key` 値。 全体の間隔は含まれていません `stop` 値 `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` 実行されるそれぞれのsubintervals。
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` パラメータ。

**戻り値**

-   の配列 `aggFunction` 各サブインターバルの結果。

**例えば**

考慮する `people` テーブルのデータ:

``` text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

のは、その年齢の間隔にある人の名前を取得してみましょう `[30,60)` と `[60,75)`. 私たちは年齢の整数表現を使用しているので、私たちはで年齢を取得します `[30, 59]` と `[60,74]` 間隔。

配列内の名前を集約するには、次のものを使用します [グルーパー](reference.md#agg_function-grouparray) 集計関数。 それは一つの議論を取る。 私たちの場合、それは `name` コラム その `groupArrayResample` 関数は `age` 年齢別に名前を集計する列。 必要な間隔を定義するために、 `30, 75, 30` への引数 `groupArrayResample` 機能。

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

結果を考慮する。

`Jonh` 彼は若すぎるので、サンプルの外です。 他の人は、指定された年齢区間に従って配布されます。

プラグインのインス数の合計人数とその平均賃金には、指定された年齢の間隔とします。

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/agg_functions/combinators/) <!--hide-->
