---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 53
toc_title: "\u30BF\u30D7\u30EB(T1,T2,...)"
---

# Tuple(t1, T2, …) {#tuplet1-t2}

要素のタプル。 [タイプ](index.md#data_types).

タプルは、一時列のグループ化に使用されます。 列は、in式がクエリで使用されている場合、およびラムダ関数の特定の仮パラメータを指定するためにグループ化できます。 詳細については、以下を参照してください [演算子の場合](../../sql-reference/statements/select.md) と [高階関数](../../sql-reference/functions/higher-order-functions.md).

タプルは、クエリの結果になります。 この場合、json以外のテキスト形式の場合、値は角かっこでカンマ区切りになります。 json形式では、タプルは配列として出力されます（角括弧内）。

## タプルの作成 {#creating-a-tuple}

関数を使用してタプルを作成することができます:

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

タプルをオンザフライで作成するとき、clickhouseは引数の値を格納できる型の最小値として各引数の型を自動的に検出します。 引数が [NULL](../../sql-reference/syntax.md#null-literal) タプル要素の型は次のとおりです [Nullable](nullable.md).

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
