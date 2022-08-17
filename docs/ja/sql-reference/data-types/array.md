---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "\u914D\u5217(T)"
---

# 配列(t) {#data-type-array}

の配列 `T`-タイプ項目。 `T` 配列を含む任意のデータ型を指定できます。

## 配列の作成 {#creating-an-array}

関数を使用して配列を作成できます:

``` sql
array(T)
```

角括弧を使用することもできます。

``` sql
[]
```

配列の作成例:

``` sql
SELECT array(1, 2) AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

``` sql
SELECT [1, 2] AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

## データ型の操作 {#working-with-data-types}

オンザフライで配列を作成するとき、ClickHouseは、リストされたすべての引数を格納できる最も狭いデータ型として引数型を自動的に定義します。 いずれかがある場合 [Null可能](nullable.md#data_type-nullable) またはリテラル [NULL](../../sql-reference/syntax.md#null-literal) 値を指定すると、配列要素の型も次のようになります [Null可能](nullable.md).

ClickHouseがデータ型を判別できなかった場合、例外が生成されます。 たとえば、これは文字列と数値を同時に配列を作成しようとするときに発生します (`SELECT array(1, 'a')`).

自動データ型検出の例:

``` sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

``` text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

互換性のないデータ型の配列を作成しようとすると、ClickHouseは例外をスローします:

``` sql
SELECT array(1, 'a')
```

``` text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

[元の記事](https://clickhouse.tech/docs/en/data_types/array/) <!--hide-->
