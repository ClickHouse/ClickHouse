---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "\u30B3\u30F3\u30D3\u30CD\u30FC\u30BF"
---

# 集計関数コンビネータ {#aggregate_functions_combinators}

集計関数の名前には、接尾辞を付加することができます。 これにより、集計関数の動作方法が変更されます。

## -もし {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

例: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` など。

条件集計関数を使用すると、サブクエリとサブクエリを使用せずに、複数の条件の集計を一度に計算できます。 `JOIN`例えば、Yandexの中。Metricaは、条件付き集計関数を使用してセグメント比較機能を実装します。

## -配列 {#agg-functions-combinator-array}

-Arrayサフィックスは、任意の集計関数に追加できます。 この場合、集計関数は ‘Array(T)’ 型(配列)の代わりに ‘T’ 引数を入力します。 集計関数が複数の引数を受け入れる場合、これは同じ長さの配列でなければなりません。 配列を処理する場合、集計関数はすべての配列要素にわたって元の集計関数と同様に機能します。

例1: `sumArray(arr)` -すべてのすべての要素を合計します ‘arr’ 配列。 この例では、より簡単に書くことができます: `sum(arraySum(arr))`.

例2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ 配列。 これは簡単な方法で行うことができます: `uniq(arrayJoin(arr))` しかし、常に追加することはできません ‘arrayJoin’ クエリに。

-Ifと-Arrayを組み合わせることができます。 しかし, ‘Array’ 最初に来なければならない ‘If’. 例: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. この順序が原因で、 ‘cond’ 引数は配列ではありません。

## -状態 {#agg-functions-combinator-state}

このコンビネータを適用すると、集計関数は結果の値（例えば、コンビネータの一意の値の数など）を返しません。 [uniq](reference.md#agg_function-uniq) の中間状態である。 `uniq`、これは一意の値の数を計算するためのハッシュテーブルです）。 これは `AggregateFunction(...)` 利用できるため、さらなる処理や保存のテーブルに仕上げを集計します。

これらの国は、利用:

-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) テーブルエンジン。
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) 機能。
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#function-runningaccumulate) 機能。
-   [-マージ](#aggregate_functions_combinators-merge) コンビネーター
-   [-MergeState](#aggregate_functions_combinators-mergestate) コンビネーター

## -マージ {#aggregate_functions_combinators-merge}

このコンビネータを適用すると、aggregate関数は中間集計状態を引数として受け取り、状態を結合して集計を終了し、結果の値を返します。

## -MergeState {#aggregate_functions_combinators-mergestate}

-Mergeコンビネータと同じ方法で中間集計状態をマージします。 ただし、結果の値を返すのではなく、-Stateコンビネータに似た中間集計状態を返します。

## -ForEach {#agg-functions-combinator-foreach}

テーブルの集計関数を、対応する配列項目を集計し、結果の配列を返す配列の集計関数に変換します。 例えば, `sumForEach` 配列の場合 `[1, 2]`, `[3, 4, 5]`と`[6, 7]`結果を返します `[10, 13, 5]` 対応する配列項目を一緒に追加した後。

## -オルデフォルト {#agg-functions-combinator-ordefault}

集計関数の動作を変更します。

集計関数に入力値がない場合、このコンビネータを使用すると、戻り値のデータ型のデフォルト値が返されます。 空の入力データを取ることができる集計関数に適用されます。

`-OrDefault` 他の組合せ器と使用することができる。

**構文**

``` sql
<aggFunction>OrDefault(x)
```

**パラメータ**

-   `x` — Aggregate function parameters.

**戻り値**

集計するものがない場合は、集計関数の戻り値の型の既定値を返します。

型は、使用される集計関数に依存します。

**例**

クエリ:

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

結果:

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

また `-OrDefault` 他のコンビネータと併用できます。 集計関数が空の入力を受け入れない場合に便利です。

クエリ:

``` sql
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

結果:

``` text
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

## -オルヌル {#agg-functions-combinator-ornull}

集計関数の動作を変更します。

このコンビネータは、集計関数の結果を [Null可能](../data-types/nullable.md) データ型。 集計関数に計算する値がない場合は、次の値が返されます [NULL](../syntax.md#null-literal).

`-OrNull` 他の組合せ器と使用することができる。

**構文**

``` sql
<aggFunction>OrNull(x)
```

**パラメータ**

-   `x` — Aggregate function parameters.

**戻り値**

-   集計関数の結果は、次のように変換されます。 `Nullable` データ型。
-   `NULL`、集約するものがない場合。

タイプ: `Nullable(aggregate function return type)`.

**例**

追加 `-orNull` 集計関数の最後に。

クエリ:

``` sql
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

結果:

``` text
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

また `-OrNull` 他のコンビネータと併用できます。 集計関数が空の入力を受け入れない場合に便利です。

クエリ:

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

結果:

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## -リサンプリング {#agg-functions-combinator-resample}

データをグループに分割し、それらのグループ内のデータを個別に集計できます。 グループは、ある列の値を間隔に分割することによって作成されます。

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**パラメータ**

-   `start` — Starting value of the whole required interval for `resampling_key` 値。
-   `stop` — Ending value of the whole required interval for `resampling_key` 値。 全体の区間は含まれていません `stop` 値 `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` これらの部分区間のそれぞれに対して独立に実行されます。
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` 変数。

**戻り値**

-   の配列 `aggFunction` 各サブインターバルの結果。

**例**

を考える `people` 次のデータを含むテーブル:

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

のは、その年齢の間隔にある人の名前を取得してみましょう `[30,60)` と `[60,75)`. 年齢に整数表現を使用するので、年齢を取得します `[30, 59]` と `[60,74]` 間隔。

配列内の名前を集計するには、 [グルーパレイ](reference.md#agg_function-grouparray) 集計関数。 それは一つの引数を取ります。 私たちの場合、それは `name` 列。 その `groupArrayResample` 関数は、 `age` 年齢別に名前を集計する列。 必要な間隔を定義するために、 `30, 75, 30` の引数 `groupArrayResample` 機能。

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

結果を考えてみましょう。

`Jonh` 彼は若すぎるので、サンプルの外にあります。 他の人は、指定された年齢間隔に従って分配されます。

今度は、指定された年齢間隔での人々の総数とその平均賃金を数えましょう。

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
