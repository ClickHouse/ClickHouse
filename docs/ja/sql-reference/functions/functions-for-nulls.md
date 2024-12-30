---
slug: /ja/sql-reference/functions/functions-for-nulls
sidebar_position: 135
sidebar_label: Nullable
---

# Nullable 値を扱う関数

## isNull

引数が [NULL](../../sql-reference/syntax.md#null) かどうかを返します。

演算子 [`IS NULL`](../operators/index.md#is_null) も参照してください。

**構文**

``` sql
isNull(x)
```

別名: `ISNULL`.

**引数**

- `x` — 非複合データ型の値。

**返される値**

- `x` が `NULL` の場合は `1`。
- `x` が `NULL` でない場合は `0`。

**例**

テーブル:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリ:

``` sql
SELECT x FROM t_null WHERE isNull(y);
```

結果:

``` text
┌─x─┐
│ 1 │
└───┘
```

## isNullable

カラムが [Nullable](../data-types/nullable.md) かどうかを返します。Nullable は `NULL` 値を許可することを意味します。

**構文**

``` sql
isNullable(x)
```

**引数**

- `x` — カラム。

**返される値**

- `x` が `NULL` 値を許可する場合は `1`。 [UInt8](../data-types/int-uint.md).
- `x` が `NULL` 値を許可しない場合は `0`。 [UInt8](../data-types/int-uint.md).

**例**

クエリ:

``` sql
CREATE TABLE tab (ordinary_col UInt32, nullable_col Nullable(UInt32)) ENGINE = Log;
INSERT INTO tab (ordinary_col, nullable_col) VALUES (1,1), (2, 2), (3,3);
SELECT isNullable(ordinary_col), isNullable(nullable_col) FROM tab;    
```

結果:

``` text
   ┌───isNullable(ordinary_col)──┬───isNullable(nullable_col)──┐
1. │                           0 │                           1 │
2. │                           0 │                           1 │
3. │                           0 │                           1 │
   └─────────────────────────────┴─────────────────────────────┘
```

## isNotNull

引数が [NULL](../../sql-reference/syntax.md#null-literal) でないかどうかを返します。

演算子 [`IS NOT NULL`](../operators/index.md#is_not_null) も参照してください。

``` sql
isNotNull(x)
```

**引数:**

- `x` — 非複合データ型の値。

**返される値**

- `x` が `NULL` でない場合は `1`。
- `x` が `NULL` の場合は `0`。

**例**

テーブル:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリ:

``` sql
SELECT x FROM t_null WHERE isNotNull(y);
```

結果:

``` text
┌─x─┐
│ 2 │
└───┘
```

## isNotDistinctFrom

NULL セーフな比較を行います。これは、JOIN ON セクションで NULL 値を含む JOIN キーを比較するために使用されます。この関数は 2 つの `NULL` 値を同一とみなし、通常の等号の動作とは異なり、2 つの `NULL` 値を比較すると `NULL` を返します。

:::note
この関数は JOIN ON の実装に使用される内部関数です。クエリで手動で使用しないでください。
:::

**構文**

``` sql
isNotDistinctFrom(x, y)
```

**引数**

- `x` — 第1の JOIN キー。
- `y` — 第2の JOIN キー。

**返される値**

- `x` と `y` が両方とも `NULL` の場合は `true`。
- それ以外の場合は `false`。

**例**

完全な例については、[JOIN キーにおける NULL 値](../../sql-reference/statements/select/join#null-values-in-join-keys)を参照してください。

## isZeroOrNull

引数が 0（ゼロ）または [NULL](../../sql-reference/syntax.md#null-literal) かどうかを返します。

``` sql
isZeroOrNull(x)
```

**引数:**

- `x` — 非複合データ型の値。

**返される値**

- `x` が 0（ゼロ）または `NULL` の場合は `1`。
- それ以外は `0`。

**例**

テーブル:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    0 │
│ 3 │    3 │
└───┴──────┘
```

クエリ:

``` sql
SELECT x FROM t_null WHERE isZeroOrNull(y);
```

結果:

``` text
┌─x─┐
│ 1 │
│ 2 │
└───┘
```

## coalesce

最も左にある`NULL` でない引数を返します。

``` sql
coalesce(x,...)
```

**引数:**

- 複合型でない任意の数のパラメータ。すべてのパラメータは互いに互換性のあるデータ型である必要があります。

**返される値**

- 最初の `NULL` でない引数
- すべての引数が `NULL` の場合、`NULL`。

**例**

顧客に連絡するための複数の方法を示す連絡先のリストを考えてみましょう。

``` text
┌─name─────┬─mail─┬─phone─────┬──telegram─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │       123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │      ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴───────────┘
```

`mail` と `phone` フィールドは String の型ですが、`telegram` フィールドは `UInt32` なので `String` に変換する必要があります。

顧客のために最初に利用可能な連絡方法を連絡先リストから取得します:

``` sql
SELECT name, coalesce(mail, phone, CAST(telegram,'Nullable(String)')) FROM aBook;
```

``` text
┌─name─────┬─coalesce(mail, phone, CAST(telegram, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                                 │
│ client 2 │ ᴺᵁᴸᴸ                                                      │
└──────────┴───────────────────────────────────────────────────────────┘
```

## ifNull

引数が `NULL` である場合に代替値を返します。

``` sql
ifNull(x, alt)
```

**引数:**

- `x` — `NULL` かどうかを確認する値。
- `alt` — `x` が `NULL` である場合に関数が返す値。

**返される値**

- `x` が `NULL` でない場合、`x`。
- `x` が `NULL` の場合、`alt`。

**例**

クエリ:

``` sql
SELECT ifNull('a', 'b');
```

結果:

``` text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

クエリ:

``` sql
SELECT ifNull(NULL, 'b');
```

結果:

``` text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullIf

2つの引数が等しい場合、`NULL` を返します。

``` sql
nullIf(x, y)
```

**引数:**

`x`, `y` — 比較する値。互換性のある型である必要があります。

**返される値**

- 引数が等しい場合、`NULL`。
- 引数が等しくない場合、`x`。

**例**

クエリ:

``` sql
SELECT nullIf(1, 1);
```

結果:

``` text
┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

クエリ:

``` sql
SELECT nullIf(1, 2);
```

結果:

``` text
┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull

[Nullable](../data-types/nullable.md) 型の値に対して、対応する非 `Nullable` 値を返します。元の値が `NULL` の場合、任意の結果が返される可能性があります。関数 `ifNull` および `coalesce` も参照してください。

``` sql
assumeNotNull(x)
```

**引数:**

- `x` — 元の値。

**返される値**

- 値が `NULL` でない場合、入力値を非 `Nullable` 型で返します。
- 入力値が `NULL` の場合、任意の値。

**例**

テーブル:

``` text

┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリ:

``` sql
SELECT assumeNotNull(y) FROM table;
```

結果:

``` text
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

クエリ:

``` sql
SELECT toTypeName(assumeNotNull(y)) FROM t_null;
```

結果:

``` text
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable

引数の型を `Nullable` に変換します。

``` sql
toNullable(x)
```

**引数:**

- `x` — 非複合型の値。

**返される値**

- 入力値を `Nullable` 型として返します。

**例**

クエリ:

``` sql
SELECT toTypeName(10);
```

結果:

``` text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

クエリ:

``` sql
SELECT toTypeName(toNullable(10));
```

結果:

``` text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```
