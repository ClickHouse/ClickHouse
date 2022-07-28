---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 53
toc_title: Tuple(T1,T2,...)
---

# Tuple(t1, T2, …) {#tuplet1-t2}

要素のタプル、各個人を持つ [タイプ](index.md#data_types).

組は、一時的な列のグループ化に使用されます。 列は、in式がクエリで使用され、lambda関数の特定の仮パラメータを指定するときにグループ化できます。 詳細については [演算子で](../../sql-reference/operators/in.md) と [高次関数](../../sql-reference/functions/higher-order-functions.md).

タプルは、クエリの結果になります。 この場合、JSON以外のテキスト形式の場合、値は角かっこでカンマ区切られます。 JSON形式では、タプルは配列として出力されます（角括弧で囲みます）。

## タプルの作成 {#creating-a-tuple}

関数を使用してタプルを作成できます:

``` sql
tuple(T1, T2, ...)
```

タプルの作成例:

``` sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## データ型の操作 {#working-with-data-types}

オンザフライでタプルを作成するとき、ClickHouseは自動的に各引数の型を引数値を格納できる型の最小値として検出します。 引数が [NULL](../../sql-reference/syntax.md#null-literal) タプル要素の型は [Null可能](nullable.md).

自動データ型検出の例:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/data_types/tuple/) <!--hide-->
