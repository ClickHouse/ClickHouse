---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "Null\u8A31\u5BB9\u5F15\u6570\u306E\u64CD\u4F5C"
---

# Null許容集計を操作するための関数 {#functions-for-working-with-nullable-aggregates}

## isNull {#isnull}

引数が [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNull(x)
```

**パラメータ**

-   `x` — A value with a non-compound data type.

**戻り値**

-   `1` もし `x` は `NULL`.
-   `0` もし `x` ではない `NULL`.

**例**

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
-   `1` もし `x` ではない `NULL`.

**例**

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

左から右にチェックするかどうか `NULL` 引数が渡され、最初のnonを返します-`NULL` 引数。

``` sql
coalesce(x,...)
```

**パラメータ:**

-   非複合型の任意の数のパラメーター。 すべてのパラメータに対応していることが必要となるデータ型になります。

**戻り値**

-   最初の非-`NULL` 引数。
-   `NULL` すべての引数が `NULL`.

**例**

顧客に連絡する複数の方法を指定できる連絡先のリストを検討してください。

``` text
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

その `mail` と `phone` フィールドはString型ですが、 `icq` フィールドは `UInt32` したがって、変換する必要があります `String`.

連絡先リストから顧客に対して最初に使用可能な連絡先方法を取得する:

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

Main引数が次の場合、代替値を返します `NULL`.

``` sql
ifNull(x,alt)
```

**パラメータ:**

-   `x` — The value to check for `NULL`.
-   `alt` — The value that the function returns if `x` は `NULL`.

**戻り値**

-   値 `x`,if `x` ではない `NULL`.
-   値 `alt`,if `x` は `NULL`.

**例**

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

## ヌリフ {#nullif}

ﾂづｩﾂ。 `NULL` 引数が等しい場合。

``` sql
nullIf(x, y)
```

**パラメータ:**

`x`, `y` — Values for comparison. They must be compatible types, or ClickHouse will generate an exception.

**戻り値**

-   `NULL`、引数が等しい場合。
-   その `x` 引数が等しくない場合の値。

**例**

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

結果はtypeの値になります [Null可能](../../sql-reference/data-types/nullable.md) 非のため- `Nullable` 値が `NULL`.

``` sql
assumeNotNull(x)
```

**パラメータ:**

-   `x` — The original value.

**戻り値**

-   非からの元の値-`Nullable` そうでない場合は、タイプ `NULL`.
-   非のデフォルト値-`Nullable` 元の値が `NULL`.

**例**

を考える `t_null` テーブル。

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

を適用する `assumeNotNull` に対する関数 `y` 列。

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

-   Aを持つ入力値 `Nullable` タイプ。

**例**

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
