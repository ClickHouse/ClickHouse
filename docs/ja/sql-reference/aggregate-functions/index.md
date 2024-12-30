---
slug: /ja/sql-reference/aggregate-functions/
sidebar_label: 集約関数
sidebar_position: 33
---

# 集約関数

集約関数は、データベースの専門家が期待する[通常](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial)の方法で動作します。

ClickHouseは以下もサポートしています:

- カラムに加えて他のパラメータを受け入れる[パラメトリック集約関数](../../sql-reference/aggregate-functions/parametric-functions.md#aggregate_functions_parametric)。
- 集約関数の動作を変える[コンビネータ](../../sql-reference/aggregate-functions/combinators.md#aggregate_functions_combinators)。

## NULL の処理

集約中に、すべての `NULL` 引数はスキップされます。集約に複数の引数がある場合、いずれかが `NULL` の行は無視されます。

この規則には例外があり、それが[`first_value`](../../sql-reference/aggregate-functions/reference/first_value.md)、[`last_value`](../../sql-reference/aggregate-functions/reference/last_value.md)およびそのエイリアス（それぞれ `any` と `anyLast`）で、修飾子 `RESPECT NULLS` が続く場合です。例として、`FIRST_VALUE(b) RESPECT NULLS` があります。

**例:**

次のテーブルを考えてみましょう:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

`y` カラムの値を合計する必要があるとしましょう:

``` sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

次に、`y` カラムから配列を作成するために `groupArray` 関数を使用できます:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` は結果の配列に `NULL` を含めません。

[COALESCE](../../sql-reference/functions/functions-for-nulls.md#coalesce) を使用して `NULL` をユースケースに合った値に変更できます。例として、`avg(COALESCE(column, 0))` を使用すると、カラムの値が集約に使われ、`NULL` の場合はゼロが使われます:

``` sql
SELECT
    avg(y),
    avg(coalesce(y, 0))
FROM t_null_big
```

``` text
┌─────────────avg(y)─┬─avg(coalesce(y, 0))─┐
│ 2.3333333333333335 │                 1.4 │
└────────────────────┴─────────────────────┘
```

また、`Tuple` を使用して `NULL` スキップ動作を回避することもできます。`NULL` 値のみを含む `Tuple` は `NULL` ではないため、その`NULL` 値のために行がスキップされません。

```sql
SELECT
    groupArray(y),
    groupArray(tuple(y)).1
FROM t_null_big;

┌─groupArray(y)─┬─tupleElement(groupArray(tuple(y)), 1)─┐
│ [2,2,3]       │ [2,NULL,2,3,NULL]                     │
└───────────────┴───────────────────────────────────────┘
```

列が集約関数の引数として使用される場合、集約はスキップされます。例えば、パラメータなしの `count()` や定数のもの (`count(1)`) はブロック内のすべての行をカウントします（これは `GROUP BY` カラムの値に依存しないため、引数ではない）が、`count(column)` は列が `NULL` でない行の数のみを返します。

```sql
SELECT
    v,
    count(1),
    count(v)
FROM
(
    SELECT if(number < 10, NULL, number % 3) AS v
    FROM numbers(15)
)
GROUP BY v

┌────v─┬─count()─┬─count(v)─┐
│ ᴺᵁᴸᴸ │      10 │        0 │
│    0 │       1 │        1 │
│    1 │       2 │        2 │
│    2 │       2 │        2 │
└──────┴─────────┴──────────┘
```

`RESPECT NULLS` を伴う `first_value` の例として、`NULL` 入力が尊重され、`NULL` かどうかにかかわらず最初に読み取られた値が返されることを確認できます:

```sql
SELECT
    col || '_' || ((col + 1) * 5 - 1) as range,
    first_value(odd_or_null) as first,
    first_value(odd_or_null) IGNORE NULLS as first_ignore_null,
    first_value(odd_or_null) RESPECT NULLS as first_respect_nulls
FROM
(
    SELECT
        intDiv(number, 5) AS col,
        if(number % 2 == 0, NULL, number) as odd_or_null
    FROM numbers(15)
)
GROUP BY col
ORDER BY col

┌─range─┬─first─┬─first_ignore_null─┬─first_respect_nulls─┐
│ 0_4   │     1 │                 1 │                ᴺᵁᴸᴸ │
│ 1_9   │     5 │                 5 │                   5 │
│ 2_14  │    11 │                11 │                ᴺᵁᴸᴸ │
└───────┴───────┴───────────────────┴─────────────────────┘
```
