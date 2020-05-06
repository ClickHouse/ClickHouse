---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 63
toc_title: "\u30CC\u30EB\u53EF\u80FD\u306A\u5F15\u6570\u306E\u64CD\u4F5C"
---

# Null可能な集計を操作するための関数 {#functions-for-working-with-nullable-aggregates}

## isNull {#isnull}

引数が [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNull(x)
```

**パラメータ**

-   `x` — A value with a non-compound data type.

**戻り値**

-   `1` もし `x` は `NULL`.
-   `0` もし `x` はない `NULL`.

**例えば**

入力テーブル

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリ

``` sql
SELECT x FROM t_null WHERE isNull(y)
```

``` text
┌─x─┐
│ 1 │
└───┘
```

## isNotNull {#isnotnull}

引数が [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNotNull(x)
```

**パラメータ:**

-   `x` — A value with a non-compound data type.

**戻り値**

-   `0` もし `x` は `NULL`.
-   `1` もし `x` はない `NULL`.

**例えば**

入力テーブル

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリ

``` sql
SELECT x FROM t_null WHERE isNotNull(y)
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## 合体 {#coalesce}

左から右にチェックするかどうか `NULL` 引数が渡され、最初の非を返します-`NULL` 引数。

``` sql
coalesce(x,...)
```

**パラメータ:**

-   非化合物タイプの任意の数のパラメーター。 すべてのパラメータに対応していることが必要となるデータ型になります。

**戻り値**

-   最初の非-`NULL` 引数。
-   `NULL` すべての引数が `NULL`.

**例えば**

顧客に連絡する複数の方法を指定する可能性のある連絡先のリストを考えてみましょう。

``` text
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

その `mail` と `phone` フィールドの型はStringですが、 `icq` フィールドは `UInt32`、それはに変換する必要があります `String`.

コンタクトリストから顧客の最初の利用可能なコンタクトメソッドを取得する:

``` sql
SELECT coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM aBook
```

``` text
┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘
```

## ifNull {#ifnull}

メイン引数がある場合は、代替値を返します `NULL`.

``` sql
ifNull(x,alt)
```

**パラメータ:**

-   `x` — The value to check for `NULL`.
-   `alt` — The value that the function returns if `x` は `NULL`.

**戻り値**

-   を値 `x`,もし `x` はない `NULL`.
-   を値 `alt`,もし `x` は `NULL`.

**例えば**

``` sql
SELECT ifNull('a', 'b')
```

``` text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

``` sql
SELECT ifNull(NULL, 'b')
```

``` text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullifname {#nullif}

を返します `NULL` 引数が等しい場合。

``` sql
nullIf(x, y)
```

**パラメータ:**

`x`, `y` — Values for comparison. They must be compatible types, or ClickHouse will generate an exception.

**戻り値**

-   `NULL` 引数が等しい場合。
-   その `x` 引数が等しくない場合の値。

**例えば**

``` sql
SELECT nullIf(1, 1)
```

``` text
┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

``` sql
SELECT nullIf(1, 2)
```

``` text
┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull {#assumenotnull}

結果はtypeの値になります [Nullable](../../sql-reference/data-types/nullable.md) 非のために- `Nullable` 値がない場合、 `NULL`.

``` sql
assumeNotNull(x)
```

**パラメータ:**

-   `x` — The original value.

**戻り値**

-   元の値から非-`Nullable` そうでない場合はタイプします `NULL`.
-   のデフォルト値。-`Nullable` 元の値が `NULL`.

**例えば**

考慮する `t_null` テーブル。

``` sql
SHOW CREATE TABLE t_null
```

``` text
┌─statement─────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_null ( x Int8,  y Nullable(Int8)) ENGINE = TinyLog │
└───────────────────────────────────────────────────────────────────────────┘
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

を適用 `assumeNotNull` に機能 `y` コラム

``` sql
SELECT assumeNotNull(y) FROM t_null
```

``` text
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

``` sql
SELECT toTypeName(assumeNotNull(y)) FROM t_null
```

``` text
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable {#tonullable}

引数の型を次のように変換します `Nullable`.

``` sql
toNullable(x)
```

**パラメータ:**

-   `x` — The value of any non-compound type.

**戻り値**

-   Aの入力値 `Nullable` タイプ。

**例えば**

``` sql
SELECT toTypeName(10)
```

``` text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

``` sql
SELECT toTypeName(toNullable(10))
```

``` text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/functions_for_nulls/) <!--hide-->
