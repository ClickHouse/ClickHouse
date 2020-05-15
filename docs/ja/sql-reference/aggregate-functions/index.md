---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Aggregate Functions
toc_priority: 33
toc_title: "\u5C0E\u5165"
---

# 集計関数 {#aggregate-functions}

集計関数は、 [通常の](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) 方法として期待されデータベースの専門家です。

ClickHouseはまた支えます:

-   [パラメトリックに集計機能](parametric-functions.md#aggregate_functions_parametric) 列に加えて他のパラメータを受け入れる。
-   [Combinators](combinators.md#aggregate_functions_combinators)、集計関数の動作を変更します。

## NULLの場合の処理 {#null-processing}

集計中、すべて `NULL`sはスキップされます。

**例:**

この表を考慮する:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

の値を合計する必要があるとしましょう `y` 列:

``` sql
SELECT sum(y) FROM t_null_big
```

    ┌─sum(y)─┐
    │      7 │
    └────────┘

その `sum` 関数の解釈 `NULL` として `0`. 特に、これは、関数がすべての値がある選択の入力を受け取った場合 `NULL` その後、結果は次のようになります `0`、ない `NULL`.

今すぐ使用できます `groupArray` から配列を作成する関数 `y` 列:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` 含まれていません `NULL` 結果の配列です。

[元の記事](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
