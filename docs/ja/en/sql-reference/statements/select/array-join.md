---
description: 'ARRAY JOIN 句のドキュメント'
sidebar_label: 'ARRAY JOIN'
slug: /sql-reference/statements/select/array-join
title: 'ARRAY JOIN 句'
doc_type: 'reference'
---

配列カラムを含むテーブルでは、元のカラム内の各配列要素ごとに1行を持つ新しいテーブルを生成し、他のカラムの値は複製する、という操作が一般的です。これは `ARRAY JOIN` clause の基本的な動作です。

この名前は、配列またはネストされたデータ構造に対して `JOIN` を実行するものと見なせることに由来しています。意図としては [arrayJoin](/ja/sql-reference/functions/array-join) 関数に似ていますが、clause の機能はより広範です。

構文:

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

サポートされている `ARRAY JOIN` の種類を以下に示します。

* `ARRAY JOIN` - 基本形では、空の配列は `JOIN` の結果に含まれません。
* `LEFT ARRAY JOIN` - `JOIN` の結果には、空の配列を持つ行も含まれます。空の配列の値には、その配列の要素型のデフォルト値 (通常は 0、空文字列、または NULL) が設定されます。

<div id="basic-array-join-examples">
  ## ARRAY JOIN の基本的な例
</div>

<div id="array-join-left-array-join-examples">
  ### ARRAY JOIN と LEFT ARRAY JOIN
</div>

以下の例では、`ARRAY JOIN` 句と `LEFT ARRAY JOIN` 句の使い方を示します。[Array](../../../sql-reference/data-types/array.md) 型のカラムを持つテーブルを作成し、そこに値を挿入してみましょう。

```sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

```response
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

以下の例では、`ARRAY JOIN` 句を使用します。

```sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

```response
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

次の例では、`LEFT ARRAY JOIN` 句を使用します。

```sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

```response
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

<div id="array-join-arrayEnumerate">
  ### ARRAY JOIN と arrayEnumerate 関数
</div>

この関数は通常、`ARRAY JOIN` と組み合わせて使用します。これにより、`ARRAY JOIN` を適用した後でも、各配列ごとに対象を 1 回だけカウントできます。例:

```sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

この例では、Reaches はコンバージョン数 (`ARRAY JOIN` を適用した後の文字列数) 、Hits はページビュー数 (`ARRAY JOIN` を適用する前の文字列数) を表します。このケースでは、同じ結果をもっと簡単に得ることもできます。

```sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

<div id="array_join_arrayEnumerateUniq">
  ### ARRAY JOIN と arrayEnumerateUniq
</div>

この関数は、`ARRAY JOIN` を使用して配列要素を集計する際に役立ちます。

この例では、各目標 ID について、コンバージョン数 (ネストされた Goals データ構造の各要素は到達した目標を表し、これをコンバージョンと呼びます) とセッション数を計算しています。`ARRAY JOIN` がなければ、セッション数は `sum(Sign)` として数えます。しかしこのケースでは、ネストされた Goals 構造によって行が増えているため、その後で各セッションを 1 回だけ数えるには、`arrayEnumerateUniq(Goals.ID)` 関数の値に条件を適用します。

```sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

```text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

<div id="using-aliases">
  ## 別名の使用
</div>

`ARRAY JOIN` 句では、配列に別名を指定できます。この場合、配列の要素にはその別名でアクセスできますが、配列自体には元の名前でアクセスします。例:

```sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

```response
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

別名を使うと、外部の配列に対して `ARRAY JOIN` を実行できます。たとえば:

```sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

```response
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

複数の配列は、`ARRAY JOIN` 句内でカンマ区切りで指定できます。この場合、それらに対して `JOIN` が同時に実行されます (デカルト積ではなく直和です) 。なお、デフォルトでは、すべての配列のサイズが同じである必要があります。例:

```sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

以下の例では、[arrayEnumerate](/ja/sql-reference/functions/array-functions#arrayEnumerate) 関数を使用します：

```sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

サイズの異なる複数のArrayは、`SETTINGS enable_unaligned_array_join = 1` を使用すると結合できます。例:

```sql
SELECT s, arr, a, b
FROM arrays_test ARRAY JOIN arr AS a, [['a','b'],['c']] AS b
SETTINGS enable_unaligned_array_join = 1;
```

```response
┌─s───────┬─arr─────┬─a─┬─b─────────┐
│ Hello   │ [1,2]   │ 1 │ ['a','b'] │
│ Hello   │ [1,2]   │ 2 │ ['c']     │
│ World   │ [3,4,5] │ 3 │ ['a','b'] │
│ World   │ [3,4,5] │ 4 │ ['c']     │
│ World   │ [3,4,5] │ 5 │ []        │
│ Goodbye │ []      │ 0 │ ['a','b'] │
│ Goodbye │ []      │ 0 │ ['c']     │
└─────────┴─────────┴───┴───────────┘
```

<div id="array-join-with-nested-data-structure">
  ## ネストされたデータ構造での ARRAY JOIN
</div>

`ARRAY JOIN` は、[ネストされたデータ構造](../../../sql-reference/data-types/nested-data-structures/index.md)でも使用できます。

```sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

```response
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

`ARRAY JOIN` でネストされたデータ構造の名前を指定した場合、その意味は、それを構成するすべての配列要素に対して `ARRAY JOIN` を指定した場合と同じです。以下に例を示します。

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

このような書き方でも問題ありません：

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

ネストされたデータ構造では、`JOIN` の結果とソース配列のどちらを選択するかを指定するために、別名を使用できます。例:

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

[arrayEnumerate](/ja/sql-reference/functions/array-functions#arrayEnumerate) 関数の使用例：

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

<div id="implementation-details">
  ## 実装の詳細
</div>

`ARRAY JOIN` の実行時には、クエリの実行順序が最適化されます。クエリ内では `ARRAY JOIN` を常に [WHERE](../../../sql-reference/statements/select/where.md)/[PREWHERE](../../../sql-reference/statements/select/prewhere.md) 句より前に指定する必要がありますが、`ARRAY JOIN` の結果がフィルタリングに使用されない限り、技術的にはこれらはどの順序でも実行できます。処理順序はクエリオプティマイザによって制御されます。

<div id="incompatibility-with-short-circuit-function-evaluation">
  ### 短絡関数評価との非互換性
</div>

[短絡関数評価](/ja/operations/settings/settings#short_circuit_function_evaluation) は、`if`、`multiIf`、`and`、`or` などの特定の関数における複雑な式の実行を最適化する機能です。これにより、これらの関数の実行中にゼロ除算のような潜在的な例外が発生するのを防げます。

`arrayJoin` は常に実行され、短絡関数評価はサポートされていません。これは、`arrayJoin` がクエリ分析および実行時に他のすべての関数とは別個に処理される特殊な関数であり、短絡関数実行とは両立しない追加ロジックを必要とするためです。その理由は、結果に含まれる行数が `arrayJoin` の結果に依存しており、`arrayJoin` の遅延実行を実装するのは複雑すぎるうえにコストも高いためです。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouse で時系列データを扱う](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)