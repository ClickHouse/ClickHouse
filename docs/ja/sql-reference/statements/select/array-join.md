---
slug: /ja/sql-reference/statements/select/array-join
sidebar_label: ARRAY JOIN
---

# ARRAY JOIN 句

ARRAY JOIN句は、配列カラムを含むテーブルに対して、そのカラムの各配列要素を持つ新しいテーブルを生成し、他のカラムの値を複製する一般的な操作です。これは、`ARRAY JOIN`句が行う基本的な内容です。

名前の由来は、配列またはネストされたデータ構造との`JOIN`の実行として見ることができることにあります。この目的は、[arrayJoin](../../../sql-reference/functions/array-join.md#functions_arrayjoin) 関数と似ていますが、句の機能はより広範です。

構文:

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

`SELECT` クエリでは、`ARRAY JOIN` 句を1つだけ指定できます。

サポートされている`ARRAY JOIN`のタイプは以下の通りです:

- `ARRAY JOIN` - 基本の場合、空の配列は `JOIN` の結果に含まれません。
- `LEFT ARRAY JOIN` - `JOIN` の結果に空の配列を持つ行が含まれます。空の配列の値は、配列要素タイプのデフォルト値（通常は0、空文字列、または NULL）に設定されます。

## 基本的な ARRAY JOIN の例

以下の例では、`ARRAY JOIN` および `LEFT ARRAY JOIN` の使用法を示します。[Array](../../../sql-reference/data-types/array.md) タイプのカラムを持つテーブルを作成し、値を挿入します:

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

以下の例は、`ARRAY JOIN` 句を使用します:

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

次の例は、`LEFT ARRAY JOIN` 句を使用します:

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

## エイリアスの使用

`ARRAY JOIN` 句で配列のエイリアスを指定できます。この場合、配列項目にはこのエイリアスでアクセスできますが、配列自体には元の名前でアクセスします。例:

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

エイリアスを使用して、外部配列との`ARRAY JOIN`を実行できます。例:

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

複数の配列は、`ARRAY JOIN`句でカンマで区切ることができます。この場合、`JOIN`は同時に実行されます（直和であり、直積ではありません）。デフォルトでは、すべての配列は同じサイズである必要があります。例:

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

以下の例では、[arrayEnumerate](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate)関数を使用しています:

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

異なるサイズの複数の配列は次のようにして結合できます: `SETTINGS enable_unaligned_array_join = 1`。例:

```sql
SELECT s, arr, a, b
FROM arrays_test ARRAY JOIN arr as a, [['a','b'],['c']] as b
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

## ネストされたデータ構造との ARRAY JOIN

`ARRAY JOIN` は[ネストされたデータ構造](../../../sql-reference/data-types/nested-data-structures/index.md)にも対応しています:

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

`ARRAY JOIN` でネストされたデータ構造の名前を指定する場合、その意味はそれを構成するすべての配列要素に対する `ARRAY JOIN` と同じです。以下に例を示します:

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

以下のバリエーションも意味があります:

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

エイリアスをネストされたデータ構造に使用して、`JOIN`結果またはソース配列を選択することもできます。例:

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

[arrayEnumerate](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate)関数を使用する例:

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

## 実装の詳細

クエリ実行の順序は、`ARRAY JOIN` 実行時に最適化されます。`ARRAY JOIN` はクエリ内で常に [WHERE](../../../sql-reference/statements/select/where.md)/[PREWHERE](../../../sql-reference/statements/select/prewhere.md) 句の前に指定する必要がありますが、`ARRAY JOIN` の結果がフィルタリングに使用されない限り、技術的には任意の順序で実行できます。処理の順序はクエリオプティマイザによって制御されます。

### 短絡評価関数との互換性がない

[短絡評価関数](../../../operations/settings/index.md#short-circuit-function-evaluation) は、`if`、`multiIf`、`and`、`or` などの特定の関数で複雑な式の実行を最適化する機能で、ゼロ除算などの潜在的な例外がこれらの関数の実行中に発生するのを防ぎます。

`arrayJoin` は常に実行され、短絡評価関数には対応していません。これは、すべての他の関数とは異なり、クエリ分析および実行中に別途処理され、`arrayJoin` の結果に依存して結果の行数が決まるためです。そのため、`arrayJoin` の遅延評価の実装は複雑で高コストとなります。

## 関連コンテンツ

- ブログ: [ClickHouseでの時系列データ処理](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
