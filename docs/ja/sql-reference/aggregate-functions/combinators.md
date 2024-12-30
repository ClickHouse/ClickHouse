---
slug: /ja/sql-reference/aggregate-functions/combinators
sidebar_position: 37
sidebar_label: Combinators
---

# 集約関数コンビネータ

集約関数の名前にはサフィックスを付けることができます。これにより集約関数の動作が変更されます。

## -If

サフィックス -If は任意の集約関数の名前に付けることができます。この場合、集約関数は追加の引数として条件（Uint8 型）を受け取ります。集約関数は条件を満たした行のみを処理します。条件が一度も満たされなかった場合、デフォルト値（通常はゼロまたは空の文字列）を返します。

例: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` など。

条件付き集約関数を使用することで、サブクエリや `JOIN` を使用せずに複数の条件に対して集約を計算することができます。例えば、条件付き集約関数を使用してセグメント比較機能を実装できます。

## -Array

-Array サフィックスは、任意の集約関数の名前に付けることができます。この場合、集約関数は ‘T’ 型の引数ではなく、‘Array(T)’ 型（配列）の引数を取ります。集約関数が複数の引数を受け取る場合、これらは同じ長さの配列でなければなりません。配列を処理する際、集約関数は元の集約関数が各配列要素に対して動作するように機能します。

例1: `sumArray(arr)` - すべての ‘arr’ 配列の要素を合計します。この例では、より簡単に `sum(arraySum(arr))` と書くことができます。

例2: `uniqArray(arr)` – すべての ‘arr’ 配列内のユニークな要素の数を数えます。これは `uniq(arrayJoin(arr))` という方法でも行うことができますが、クエリに ‘arrayJoin’ を追加できない場合があります。

-If と -Array は組み合わせ可能です。しかしながら、‘Array’ は最初に、次に‘If’の順序でなければなりません。例: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`。この順序のため、‘cond’ 引数は配列ではありません。

## -Map

-Map サフィックスは、任意の集約関数に付けることができます。これにより、集約関数は引数として Map 型を取得し、マップの各キーの値を指定された集約関数を使用して個別に集約します。結果も Map 型となります。

**例**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = Log;

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

## -SimpleState

このコンビネータを適用すると、集約関数は同じ値を異なる型で返します。これは、[SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md) で、[AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) テーブルで使用できるようにテーブルに保存できます。

**構文**

``` sql
<aggFunction>SimpleState(x)
```

**引数**

- `x` — 集約関数のパラメータ。

**返される値**

`SimpleAggregateFunction(...)` 型の集約関数の値。

**例**

クエリ:

``` sql
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

結果:

``` text
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

## -State

このコンビネータを適用すると、集約関数は最終的な値（例えば、[uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) 関数のユニークな値の数）ではなく、中間の集約状態を返します（`uniq` の場合、ユニークな値の数を計算するためのハッシュテーブルです）。これは、後処理に使用したり、テーブルに保存して後で集約を完了するために使用できる `AggregateFunction(...)` です。

:::note
-MapState は、同じデータに対する不変性を保証しません。これは中間状態でのデータ順序が変わる可能性があるためですが、データの取り込みには影響を与えません。
:::

これらの状態を操作するには、次を使用します:

- [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) テーブルエンジン。
- [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) 関数。
- [runningAccumulate](../../sql-reference/functions/other-functions.md#runningaccumulate) 関数。
- [-Merge](#-merge) コンビネータ。
- [-MergeState](#-mergestate) コンビネータ。

## -Merge

このコンビネータを適用すると、集約関数は中間の集約状態を引数として受け取り、状態を結合して集計を完了し、結果の値を返します。

## -MergeState

-Merge コンビネータと同じ方法で中間集計状態をマージします。ただし、結果の値を返さず、-State コンビネータと同様に中間集約状態を返します。

## -ForEach

テーブルの集約関数を配列の集約関数に変換し、対応する配列のアイテムを集約して結果の配列を返します。例えば、配列 `[1, 2]`, `[3, 4, 5]`, `[6, 7]` に対して `sumForEach` を使うと、対応する配列のアイテムを加算して `[10, 13, 5]` という結果を返します。

## -Distinct

あらゆる引数のユニークな組み合わせを一度だけ集約します。繰り返しの値は無視されます。例: `sum(DISTINCT x)` (または `sumDistinct(x)`), `groupArray(DISTINCT x)` (または `groupArrayDistinct(x)`) , `corrStable(DISTINCT x, y)` (または `corrStableDistinct(x, y)`) など。

## -OrDefault

集約関数の動作を変更します。

集約関数が入力値を持たない場合、このコンビネータを使うと、集約関数の戻りデータ型に対するデフォルト値を返します。空の入力データを受け取ることができる集約関数に適用されます。

`-OrDefault` は他のコンビネータと一緒に使用することができます。

**構文**

``` sql
<aggFunction>OrDefault(x)
```

**引数**

- `x` — 集約関数のパラメータ。

**返される値**

集約するものがない場合、集約関数の戻り型のデフォルト値を返します。

型は使用する集約関数に依存します。

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

`-OrDefault` は他のコンビネータと一緒に使用することもできます。これは集約関数が空の入力を受け付けない場合に便利です。

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

## -OrNull

集約関数の動作を変更します。

このコンビネータは集約関数の結果を [Nullable](../../sql-reference/data-types/nullable.md) データ型に変換します。集約関数に値がない場合は、[NULL](../../sql-reference/syntax.md#null-literal) を返します。

`-OrNull` は他のコンビネータと一緒に使用することができます。

**構文**

``` sql
<aggFunction>OrNull(x)
```

**引数**

- `x` — 集約関数のパラメータ。

**返される値**

- 集約関数の結果を `Nullable` データ型に変換されたもの。
- 集約するものがない場合は `NULL`。

型: `Nullable(集約関数の戻り型)`。

**例**

集約関数の末尾に `-orNull` を追加します。

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

また `-OrNull` は他のコンビネータと一緒に使用できます。これは集約関数が空の入力を受け付けない場合に便利です。

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

## -Resample

データをグループに分け、そのグループごとにデータを個別に集約できます。グループは、ひとつのカラムの値を間隔に分割することによって作られます。

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**引数**

- `start` — `resampling_key` 値の全体の必要な間隔の開始値。
- `stop` — `resampling_key` 値の全体の必要な間隔の終了値。全体の間隔には終了値 `stop` が含まれません `[start, stop)`。
- `step` — 全体の間隔をサブ間隔に分ける間隔。`aggFunction` はこれらのサブ間隔ごとに独立して実行されます。
- `resampling_key` — 間隔にデータを分けるために使用されるカラムの値。
- `aggFunction_params` — `aggFunction` のパラメータ。

**返される値**

- 各サブ間隔の `aggFunction` の結果の配列。

**例**

以下のデータを持つ `people` テーブルを考えます:

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

年齢が `[30,60)` と `[60,75)` の範囲にある人々の名前を取得します。年齢を整数表現で使用するため、`[30, 59]` と `[60,74]` の範囲になります。

配列に名前を集約するために、[groupArray](../../sql-reference/aggregate-functions/reference/grouparray.md#agg_function-grouparray) 集約関数を使用します。この関数は1つの引数を取ります。この場合、`name` カラムです。`groupArrayResample` 関数は `age` カラムを使用して年齢ごとに名前を集約する必要があります。必要な間隔を定義するために、`30, 75, 30` の引数を `groupArrayResample` 関数に渡します。

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

結果を考えます。

`John` は若すぎるためサンプルから除外されます。その他の人々は指定された年齢の間隔に従って分配されています。

次に、指定された年齢範囲における総人口数と平均賃金を数えます。

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴──────────────────────────┘
```

## -ArgMin

サフィックス -ArgMin は任意の集約関数の名前に付けることができます。この場合、集約関数は追加の引数として、任意の比較可能な式を受け取ります。集約関数は特定の追加式の最小値を持つ行のみを処理します。

例: `sumArgMin(column, expr)`, `countArgMin(expr)`, `avgArgMin(x, expr)` など。

## -ArgMax

サフィックス -ArgMin と似ていますが、特定の追加式で最大値を持つ行のみを処理します。

## 関連コンテンツ

- ブログ: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
