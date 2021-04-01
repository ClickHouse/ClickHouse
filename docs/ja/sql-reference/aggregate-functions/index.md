---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u96C6\u8A08\u95A2\u6570"
toc_priority: 33
toc_title: "\u306F\u3058\u3081\u306B"
---

# 集計関数 {#aggregate-functions}

集計関数は、 [標準](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) データベースの専門家が予想通りの方法。

ClickHouseはまた支えます:

-   [パラメトリック集計関数](parametric-functions.md#aggregate_functions_parametric) 列に加えて他のパラメータを受け入れます。
-   [コンビネータ](combinators.md#aggregate_functions_combinators)、集計関数の動作を変更します。

## ヌル処理 {#null-processing}

集計中、すべて `NULL`sはスキップされます。

**例:**

次の表を考えます:

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

その `sum` 関数は解釈します `NULL` として `0`. 特に、これは、関数がすべての値が次のような選択範囲の入力を受け取った場合 `NULL` 結果は次のようになります `0` ない `NULL`.

今すぐ使用することができます `groupArray` から配列を作成する関数 `y` 列:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` 含まない `NULL` 結果の配列で。

[元の記事](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
