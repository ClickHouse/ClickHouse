---
description: 'ClickHouse の Tuple データ型に関するドキュメント'
sidebar_label: 'Tuple(T1, T2, ...)'
sidebar_position: 34
slug: /sql-reference/data-types/tuple
title: 'Tuple(T1, T2, ...)'
doc_type: 'reference'
---

各要素がそれぞれ独自の [型](/ja/sql-reference/data-types) を持つタプルです。Tuple には少なくとも 1 つの要素が含まれている必要があります。

Tuple は、一時的にカラムをグループ化するために使用されます。カラムは、クエリで IN 式を使用する場合や、ラムダ関数の特定の形式パラメーターを指定する場合にグループ化できます。詳細については、[IN operators](../../sql-reference/operators/in.md) および [Higher order functions](/ja/sql-reference/functions/overview#higher-order-functions) を参照してください。

Tuple はクエリの結果になることもあります。この場合、JSON 以外のテキストフォーマットでは、値は `()` 内にカンマ区切りで出力されます。JSON フォーマットでは、Tuple は配列 (`[]`) として出力されます。

<div id="creating-tuples">
  ## タプルの作成
</div>

関数を使ってタプルを作成できます。

```sql
tuple(T1, T2, ...)
```

タプルを作成する例:

```sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

```text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

Tuple は 1 つの要素だけを含めることができます

例:

```sql
SELECT tuple('a') AS x;
```

```text
┌─x─────┐
│ ('a') │
└───────┘
```

複数の要素からなるタプルは、`tuple()` 関数を呼び出さなくても、`(tuple_element1, tuple_element2)` という構文で作成できます。

例:

```sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

```text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

<div id="data-type-detection">
  ## データ型の検出
</div>

その場でタプルを作成する際、ClickHouse は、指定された引数の値を保持できる最小の型としてタプルの引数の型を推定します。値が [NULL](/ja/operations/settings/formats#input_format_null_as_default) の場合、推定される型は [Nullable](../../sql-reference/data-types/nullable.md) になります。

データ型の自動検出の例:

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

```text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

<div id="referring-to-tuple-elements">
  ## Tuple要素の参照
</div>

Tuple の要素は、名前またはインデックスで参照できます。

```sql title="Query"
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- by name
SELECT a.2 FROM named_tuples; -- by index
```

```text title="Response"
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

<div id="comparison-operations-with-tuple">
  ## Tuple に対する比較演算
</div>

2 つのTupleは、左から右へ各要素を順番に比較して判定されます。最初のTupleの要素が 2 番目のTupleの対応する要素より大きい (小さい) 場合、最初のTupleは 2 番目のTupleより大きい (小さい) と判定されます。そうでない場合 (両方の要素が等しい場合) は、次の要素を比較します。

例:

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

```text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

実際の例:

```sql
CREATE TABLE test
(
    `year` Int16,
    `month` Int8,
    `day` Int8
)
ENGINE = Memory AS
SELECT *
FROM values((2022, 12, 31), (2000, 1, 1));

SELECT * FROM test;

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
│ 2000 │     1 │   1 │
└──────┴───────┴─────┘

SELECT *
FROM test
WHERE (year, month, day) > (2010, 1, 1);

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
└──────┴───────┴─────┘
CREATE TABLE test
(
    `key` Int64,
    `duration` UInt32,
    `value` Float64
)
ENGINE = Memory AS
SELECT *
FROM values((1, 42, 66.5), (1, 42, 70), (2, 1, 10), (2, 2, 0));

SELECT * FROM test;

┌─key─┬─duration─┬─value─┐
│   1 │       42 │  66.5 │
│   1 │       42 │    70 │
│   2 │        1 │    10 │
│   2 │        2 │     0 │
└─────┴──────────┴───────┘

-- Let's find a value for each key with the biggest duration, if durations are equal, select the biggest value

SELECT
    key,
    max(duration),
    argMax(value, (duration, value))
FROM test
GROUP BY key
ORDER BY key ASC;

┌─key─┬─max(duration)─┬─argMax(value, tuple(duration, value))─┐
│   1 │            42 │                                    70 │
│   2 │             2 │                                     0 │
└─────┴───────────────┴───────────────────────────────────────┘
```

<div id="nullable-tuple">
  ## Nullable(Tuple(T1, T2, ...))
</div>

:::note ベータ機能
`SET enable_nullable_tuple_type = 1` が必要です
これはベータ機能です。
:::

`Tuple(Nullable(T1), Nullable(T2), ...)` では各要素だけを `NULL` にできるのに対し、こちらではタプル全体を `NULL` にできます。

| Type                                       | タプル全体をNULLにできる | 要素をNULLにできる |
| ------------------------------------------ | -------------- | ----------- |
| `Nullable(Tuple(String, Int64))`           | ✅              | ❌           |
| `Tuple(Nullable(String), Nullable(Int64))` | ❌              | ✅           |

例:

```sql
SET enable_nullable_tuple_type = 1;

CREATE TABLE test (
    id UInt32,
    data Nullable(Tuple(String, Int64))
) ENGINE = Memory;

INSERT INTO test VALUES (1, ('hello', 42)), (2, NULL);

SELECT * FROM test WHERE data IS NULL;
```

```txt
 ┌─id─┬─data─┐
 │  2 │ ᴺᵁᴸᴸ │
 └────┴──────┘
```