---
slug: /ja/sql-reference/functions/logical-functions
sidebar_position: 110
sidebar_label: 論理
---

# 論理関数

以下の関数は任意の数値型の引数に対して論理演算を行います。それらは [UInt8](../data-types/int-uint.md) または場合によっては `NULL` として 0 または 1 を返します。

引数としてのゼロは `false` とみなされ、ゼロ以外の値は `true` とみなされます。

## and

二つ以上の値の論理積を計算します。

[short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) を設定することで、短絡評価を使用するかどうかを制御します。 有効になっている場合、`val_i` は `(val_1 AND val_2 AND ... AND val_{i-1})` が `true` の場合にのみ評価されます。例えば、短絡評価を使用すると、クエリ `SELECT and(number = 2, intDiv(1, number)) FROM numbers(5)` を実行してもゼロ除算例外は発生しません。

**構文**

``` sql
and(val1, val2...)
```

別名: [AND 演算子](../../sql-reference/operators/index.md#logical-and-operator)。

**引数**

- `val1, val2, ...` — 少なくとも二つの値のリスト。[Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) または [Nullable](../data-types/nullable.md)。

**返される値**

- 少なくとも一つの引数が `false` の場合は `0`、
- 引数が全て `false` でなく少なくとも一つが `NULL` の場合は `NULL`、
- それ以外の場合は `1`。

型: [UInt8](../../sql-reference/data-types/int-uint.md) または [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md))。

**例**

``` sql
SELECT and(0, 1, -2);
```

結果:

``` text
┌─and(0, 1, -2)─┐
│             0 │
└───────────────┘
```

`NULL` を含めた場合:

``` sql
SELECT and(NULL, 1, 10, -2);
```

結果:

``` text
┌─and(NULL, 1, 10, -2)─┐
│                 ᴺᵁᴸᴸ │
└──────────────────────┘
```

## or

二つ以上の値の論理和を計算します。

[short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) を設定することで、短絡評価を使用するかどうかを制御します。有効になっている場合、`val_i` は `((NOT val_1) AND (NOT val_2) AND ... AND (NOT val_{i-1}))` が `true` の場合にのみ評価されます。例えば、短絡評価を使用すると、クエリ `SELECT or(number = 0, intDiv(1, number) != 0) FROM numbers(5)` を実行してもゼロ除算例外は発生しません。

**構文**

``` sql
or(val1, val2...)
```

別名: [OR 演算子](../../sql-reference/operators/index.md#logical-or-operator)。

**引数**

- `val1, val2, ...` — 少なくとも二つの値のリスト。[Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) または [Nullable](../data-types/nullable.md)。

**返される値**

- 少なくとも一つの引数が `true` の場合は `1`、
- 引数が全て `false` の場合は `0`、
- 引数が全て `false` で少なくとも一つが `NULL` の場合は `NULL`。

型: [UInt8](../../sql-reference/data-types/int-uint.md) または [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md))。

**例**

``` sql
SELECT or(1, 0, 0, 2, NULL);
```

結果:

``` text
┌─or(1, 0, 0, 2, NULL)─┐
│                    1 │
└──────────────────────┘
```

`NULL` を含めた場合:

``` sql
SELECT or(0, NULL);
```

結果:

``` text
┌─or(0, NULL)─┐
│        ᴺᵁᴸᴸ │
└─────────────┘
```

## not

値の論理否定を計算します。

**構文**

``` sql
not(val);
```

別名: [否定演算子](../../sql-reference/operators/index.md#logical-negation-operator)。

**引数**

- `val` — 値。[Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) または [Nullable](../data-types/nullable.md)。

**返される値**

- `val` が `false` の場合は `1`、
- `val` が `true` の場合は `0`、
- `val` が `NULL` の場合は `NULL`。

型: [UInt8](../../sql-reference/data-types/int-uint.md) または [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md))。

**例**

``` sql
SELECT NOT(1);
```

結果:

``` test
┌─not(1)─┐
│      0 │
└────────┘
```

## xor

二つ以上の値の排他的論理和を計算します。二つ以上の入力値については、まず最初の二つの値に対して xor を行い、次にその結果と三つ目の値について xor を行うというように進めます。

**構文**

``` sql
xor(val1, val2...)
```

**引数**

- `val1, val2, ...` — 少なくとも二つの値のリスト。[Int](../data-types/int-uint.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md) または [Nullable](../data-types/nullable.md)。

**返される値**

- 二つの値について、一方が `false` で他方が `true` の場合は `1`、
- 二つの値がどちらも `false` または `true` の場合は `0`、
- 少なくとも一つの入力が `NULL` の場合は `NULL`。

型: [UInt8](../../sql-reference/data-types/int-uint.md) または [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md))。

**例**

``` sql
SELECT xor(0, 1, 1);
```

結果:

``` text
┌─xor(0, 1, 1)─┐
│            0 │
└──────────────┘
```
