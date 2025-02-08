---
slug: /ja/sql-reference/functions/conditional-functions
sidebar_position: 40
sidebar_label: 条件付き
---

# 条件付き関数

## if

条件分岐を実行します。

条件 `cond` が非ゼロ値を評価する場合、関数は式 `then` の結果を返します。もし `cond` がゼロまたは `NULL` を評価する場合、`else` 式の結果が返されます。

[short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) を設定することでショートサーキット評価が使用されるかどうかを制御します。有効にすると、`then` 式は `cond` が `true` の行でのみ評価され、`else` 式は `cond` が `false` の行でのみ評価されます。例えば、ショートサーキット評価を使用すると、クエリ `SELECT if(number = 0, 0, intDiv(42, number)) FROM numbers(10)` を実行する際にゼロ除算例外が発生しません。

`then` と `else` は似た型である必要があります。

**構文**

``` sql
if(cond, then, else)
```
エイリアス: `cond ? then : else` (三項演算子)

**引数**

- `cond` – 評価される条件。UInt8, Nullable(UInt8) または NULL。
- `then` – `condition` が真であるときに返される式。
- `else` – `condition` が偽または NULL のときに返される式。

**返される値**

条件 `cond` に依存して、`then` または `else` 式の結果を返します。

**例**

``` sql
SELECT if(1, plus(2, 2), plus(2, 6));
```

結果:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

## multiIf

クエリ内で [CASE](../../sql-reference/operators/index.md#conditional-expression) 演算子をよりコンパクトに記述することができます。

**構文**

``` sql
multiIf(cond_1, then_1, cond_2, then_2, ..., else)
```

[short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) を設定することでショートサーキット評価が使用されるかどうかを制御します。有効にすると、`then_i` 式は `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}) AND cond_i)` が `true` の行でのみ評価され、`cond_i` は `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}))` が `true` の行でのみ評価されます。例えば、ショートサーキット評価を使用すると、クエリ `SELECT multiIf(number = 2, intDiv(1, number), number = 5) FROM numbers(10)` を実行する際にゼロ除算例外が発生しません。

**引数**

この関数は `2N+1` のパラメーターを受け取ります:
- `cond_N` — `then_N` が返されるかどうかを制御する N 番目の評価条件。
- `then_N` — `cond_N` が真であるときの関数の結果。
- `else` — いずれの条件も真でない場合の関数の結果。

**返される値**

条件 `cond_N` に依存して、`then_N` または `else` 式のいずれかの結果を返します。

**例**

以下のテーブルを想定します:

``` text
┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM LEFT_RIGHT

┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │     4 │ Null value      │
│    1 │     3 │ left is smaller │
│    2 │     2 │ Both equal      │
│    3 │     1 │ left is greater │
│    4 │  ᴺᵁᴸᴸ │ Null value      │
└──────┴───────┴─────────────────┘
```

## 条件付き結果を直接使用する

条件式は常に `0`, `1` または `NULL` に評価されます。そのため、条件式の結果を直接使用することができます:

``` sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

## 条件式における NULL 値

条件式に `NULL` 値が関与する場合、結果も `NULL` になります。

``` sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

そのため、型が `Nullable` の場合はクエリを慎重に構築する必要があります。

以下の例では、`multiIf` に等しい条件を追加しないことでエラーを示します。

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

## greatest

値のリストから最大のものを返します。リストのメンバーはすべて比較可能な型でなければなりません。

例:

```sql
SELECT greatest(1, 2, toUInt8(3), 3.) result,  toTypeName(result) type;
```
```response
┌─result─┬─type────┐
│      3 │ Float64 │
└────────┴─────────┘
```

:::note
返される型は Float64 であり、UInt8 は比較のために 64 ビットに昇格しなければならないためです。
:::

```sql
SELECT greatest(['hello'], ['there'], ['world'])
```
```response
┌─greatest(['hello'], ['there'], ['world'])─┐
│ ['world']                                 │
└───────────────────────────────────────────┘
```

```sql
SELECT greatest(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3))
```
```response
┌─greatest(toDateTime32(plus(now(), toIntervalDay(1))), toDateTime64(now(), 3))─┐
│                                                       2023-05-12 01:16:59.000 │
└──---──────────────────────────────────────────────────────────────────────────┘
```

:::note
返される型は DateTime64 であり、DateTime32 は比較のために 64 ビットに昇格しなければならないためです。
:::

## least

値のリストから最小のものを返します。リストのメンバーはすべて比較可能な型でなければなりません。

例:

```sql
SELECT least(1, 2, toUInt8(3), 3.) result,  toTypeName(result) type;
```
```response
┌─result─┬─type────┐
│      1 │ Float64 │
└────────┴─────────┘
```

:::note
返される型は Float64 であり、UInt8 は比較のために 64 ビットに昇格しなければならないためです。
:::

```sql
SELECT least(['hello'], ['there'], ['world'])
```
```response
┌─least(['hello'], ['there'], ['world'])─┐
│ ['hello']                              │
└────────────────────────────────────────┘
```

```sql
SELECT least(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3))
```
```response
┌─least(toDateTime32(plus(now(), toIntervalDay(1))), toDateTime64(now(), 3))─┐
│                                                    2023-05-12 01:16:59.000 │
└────────────────────────────────────────────────────────────────────────────┘
```

:::note
返される型は DateTime64 であり、DateTime32 は比較のために 64 ビットに昇格しなければならないためです。
:::

## clamp

返される値を A と B の間に制約します。

**構文**

``` sql
clamp(value, min, max)
```

**引数**

- `value` – 入力値。
- `min` – 下限の制限。
- `max` – 上限の制限。

**返される値**

値が最小値より小さい場合、最小値を返し、最大値より大きい場合、最大値を返します。さもなければ、現在の値を返します。

例:

```sql
SELECT clamp(1, 2, 3) result,  toTypeName(result) type;
```
```response
┌─result─┬─type────┐
│      2 │ Float64 │
└────────┴─────────┘
```
