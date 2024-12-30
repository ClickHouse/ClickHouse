---
slug: /ja/sql-reference/data-types/tuple
sidebar_position: 34
sidebar_label: Tuple(T1, T2, ...)
---

# Tuple(T1, T2, ...)

要素のタプルで、それぞれが個別の[型](../../sql-reference/data-types/index.md#data_types)を持ちます。タプルは少なくとも1つの要素を含む必要があります。

タプルは一時的なカラムのグループ化に使用されます。カラムはクエリでIN式が使用される場合や、ラムダ関数の特定の形式パラメータを指定するためにグループ化できます。詳細については、[IN 演算子](../../sql-reference/operators/in.md)および[高階関数](../../sql-reference/functions/index.md#higher-order-functions)のセクションを参照してください。

タプルはクエリの結果として得られることがあります。この場合、JSON以外のテキスト形式では、値が括弧内でカンマ区切りされます。JSON形式では、タプルは配列（角括弧）の形で出力されます。

## タプルの作成

タプルを作成するためには関数を使用できます。

``` sql
tuple(T1, T2, ...)
```

タプル作成の例:

``` sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

タプルは単一の要素を含むことができます

例:

``` sql
SELECT tuple('a') AS x;
```

``` text
┌─x─────┐
│ ('a') │
└───────┘
```

`tuple()` 関数を呼び出さずに複数の要素のタプルを作成するために、構文 `(tuple_element1, tuple_element2)` を使用することができます。

例:

``` sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

``` text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

## データ型の検出

タプルをオンザフライで作成する場合、ClickHouseはタプルの引数の型を、与えられた引数の値を保持できる最小の型として判断します。値が[NULL](../../sql-reference/syntax.md#null-literal)の場合、判断される型は[Nullable](../../sql-reference/data-types/nullable.md)です。

自動データ型検出の例:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

## タプル要素への参照

タプル要素は名前またはインデックスで参照できます:

``` sql
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- 名前で参照
SELECT a.2 FROM named_tuples; -- インデックスで参照
```

結果:

``` text
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

## タプルの比較演算

2つのタプルは、要素を左から右に順番に比較することで比較されます。最初のタプルの要素が2番目のタプルの対応する要素より大きい（小さい）場合、最初のタプルは2番目より大きい（小さい）とされます。そうでない場合（両方の要素が等しい場合）、次の要素が比較されます。

例:

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

``` text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

実例:

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

-- 各キーに対して最大の継続時間を持つ値を見つけます。継続時間が等しい場合、最大の値を選択します。

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
