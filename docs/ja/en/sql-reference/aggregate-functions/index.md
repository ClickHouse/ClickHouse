---
description: '集約関数のドキュメント'
sidebar_label: '集約関数'
sidebar_position: 33
slug: /sql-reference/aggregate-functions/
title: '集約関数'
doc_type: 'reference'
---

集約関数は、データベースの専門家にとって想定どおりの[通常](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial)の方法で動作します。

ClickHouse は、以下もサポートしています。

* [パラメトリック集約関数](/ja/sql-reference/aggregate-functions/parametric-functions): カラムに加えて、ほかのパラメータも受け取ります。
* [集約関数コンビネータ](/ja/sql-reference/aggregate-functions/combinators): 集約関数の動作を変更します。

<div id="null-processing">
  ## NULL の処理
</div>

集約では、`NULL` の引数はすべてスキップされます。引数が複数ある場合は、そのうち 1 つ以上が NULL の行は無視されます。

この規則には例外があります。修飾子 `RESPECT NULLS` が続く場合の関数 [`first_value`](../../sql-reference/aggregate-functions/reference/first_value.md)、[`last_value`](../../sql-reference/aggregate-functions/reference/last_value.md) およびそれぞれの別名 (`any` と `anyLast`) です。たとえば、`FIRST_VALUE(b) RESPECT NULLS` のようになります。

**例:**

次のテーブルを考えます。

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

たとえば、`y`カラムの値を合計したいとします：

```sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

これで、`groupArray` 関数を使って `y` カラムから配列を作成できます。

```sql
SELECT groupArray(y) FROM t_null_big
```

```text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` は、結果の配列に `NULL` を含めません。

[COALESCE](../../sql-reference/functions/functions-for-nulls.md#coalesce) を使うと、NULL をユースケースに応じた適切な値に置き換えられます。たとえば、`avg(COALESCE(column, 0))` は、集約時にカラムの値を使用し、`NULL` の場合は 0 を使用します。

```sql
SELECT
    avg(y),
    avg(coalesce(y, 0))
FROM t_null_big
```

```text
┌─────────────avg(y)─┬─avg(coalesce(y, 0))─┐
│ 2.3333333333333335 │                 1.4 │
└────────────────────┴─────────────────────┘
```

また、NULL のスキップ動作を回避するために [Tuple](/ja/sql-reference/data-types/tuple.md) を使うこともできます。`NULL` 値だけを含む `Tuple` は `NULL` ではないため、集約関数はその `NULL` 値を理由にその行をスキップしません。

```sql
SELECT
    groupArray(y),
    groupArray(tuple(y)).1
FROM t_null_big;

┌─groupArray(y)─┬─tupleElement(groupArray(tuple(y)), 1)─┐
│ [2,2,3]       │ [2,NULL,2,3,NULL]                     │
└───────────────┴───────────────────────────────────────┘
```

カラムが集計関数の引数として使われる場合、集計はスキップされることに注意してください。たとえば、[`count`](../../sql-reference/aggregate-functions/reference/count.md) にパラメータがない場合 (`count()`) や定数を指定した場合 (`count(1)`) は、ブロック内のすべての行がカウントされます (GROUP BY のカラムは引数ではないため、その値には依存しません) 。一方、`count(column)` は、column が NULL ではない行の数だけを返します。

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

そして、`RESPECT NULLS` を指定した first&#95;value の例を次に示します。この例では、NULL 入力が尊重され、NULL かどうかにかかわらず、最初に読み取られた値が返されることがわかります。

```sql
SELECT
    col || '_' || ((col + 1) * 5 - 1) AS range,
    first_value(odd_or_null) AS first,
    first_value(odd_or_null) IGNORE NULLS as first_ignore_null,
    first_value(odd_or_null) RESPECT NULLS as first_respect_nulls
FROM
(
    SELECT
        intDiv(number, 5) AS col,
        if(number % 2 == 0, NULL, number) AS odd_or_null
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