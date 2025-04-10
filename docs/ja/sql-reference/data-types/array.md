---
slug: /ja/sql-reference/data-types/array
sidebar_position: 32
sidebar_label: Array(T)
---

# Array(T)

`T` 型の項目の配列で、配列の開始インデックスは1です。`T` は、配列を含む任意のデータ型にすることができます。

## 配列の作成

配列を作成するための関数を使用できます：

``` sql
array(T)
```

また、角括弧を使用することもできます。

``` sql
[]
```

配列を作成する例：

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

## データ型の操作

配列をその場で作成するとき、ClickHouseはリストされたすべての引数を格納できる最も狭いデータ型として引数の型を自動的に定義します。[Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable)やリテラルの[NULL](../../sql-reference/syntax.md#null-literal)値がある場合、配列要素の型も[Nullable](../../sql-reference/data-types/nullable.md)になります。

ClickHouseがデータ型を決定できなかった場合は、例外を生成します。例えば、文字列と数値を同時に使用して配列を作成しようとしたとき（`SELECT array(1, 'a')`）です。

自動データ型検出の例：

``` sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

``` text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

互換性のないデータ型の配列を作成しようとすると、ClickHouseは例外を投げます：

``` sql
SELECT array(1, 'a')
```

``` text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

## 配列のサイズ

全体のカラムを読み取らずに、`size0` サブカラムを使用することで配列のサイズを取得できます。多次元配列の場合、`sizeN-1` を使用できます。ここで `N` は求める次元です。

**例**

クエリ：

```sql
CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);

SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
```

結果：

``` text
┌─arr.size0─┬─arr.size1─┬─arr.size2─┐
│         1 │ [2]       │ [[4,1]]   │
└───────────┴───────────┴───────────┘
```

## 配列からのネストされたサブカラムの読み取り

もし `Array` 内のネストされた型 `T` がサブカラムを持っている場合（例えば、それが[named tuple](./tuple.md)の場合）、同じサブカラム名で `Array(T)` 型からサブカラムを読み取ることができます。サブカラムの型は、元のサブカラムの型の `Array` になります。

**例**

```sql
CREATE TABLE t_arr (arr Array(Tuple(field1 UInt32, field2 String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'Hello'), (2, 'World')]), ([(3, 'This'), (4, 'is'), (5, 'subcolumn')]);
SELECT arr.field1, toTypeName(arr.field1), arr.field2, toTypeName(arr.field2) from t_arr;
```

```test
┌─arr.field1─┬─toTypeName(arr.field1)─┬─arr.field2────────────────┬─toTypeName(arr.field2)─┐
│ [1,2]      │ Array(UInt32)          │ ['Hello','World']         │ Array(String)          │
│ [3,4,5]    │ Array(UInt32)          │ ['This','is','subcolumn'] │ Array(String)          │
└────────────┴────────────────────────┴───────────────────────────┴────────────────────────┘
```
