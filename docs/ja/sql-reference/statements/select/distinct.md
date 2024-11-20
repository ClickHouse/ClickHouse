---
slug: /ja/sql-reference/statements/select/distinct
sidebar_label: DISTINCT
---

# DISTINCT句

`SELECT DISTINCT`が指定されている場合、クエリ結果にはユニークな行だけが残ります。つまり、結果内で完全に一致する行のセットからは、単一の行のみが残ります。

ユニークな値を持つべきカラムのリストを指定できます: `SELECT DISTINCT ON (column1, column2,...)`。カラムが指定されていない場合は、すべてが考慮されます。

以下のテーブルを考えてみましょう:

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

カラムを指定せずに`DISTINCT`を使用する場合:

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

カラムを指定して`DISTINCT`を使用する場合:

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

## DISTINCTとORDER BY

ClickHouseは、異なるカラムに対して`DISTINCT`および`ORDER BY`句を使用することをサポートしています。`DISTINCT`句は`ORDER BY`句よりも先に実行されます。

以下のテーブルを考えてみましょう:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

データを選択する場合:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```
異なるソート順でデータを選択する場合:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

行`2, 4`はソートの前にカットされました。

クエリをプログラムするときには、この実装の特性に注意してください。

## NULLの処理

`DISTINCT`は、[NULL](../../../sql-reference/syntax.md#null-literal)を特定の値とみなして処理し、`NULL==NULL`であるかのように動作します。言い換えれば、`DISTINCT`の結果では、`NULL`との異なる組み合わせは一度だけ発生します。他の多くの文脈における`NULL`の処理とは異なります。

## 代替案

`SELECT`句で指定した同じ値のセットに対して[GROUP BY](../../../sql-reference/statements/select/group-by.md)を適用することで、集計関数を使用せずに同じ結果を得ることができます。しかし、`GROUP BY`アプローチとはいくつかの違いがあります:

- `DISTINCT`は`GROUP BY`とともに適用できます。
- [ORDER BY](../../../sql-reference/statements/select/order-by.md)が省略され、[LIMIT](../../../sql-reference/statements/select/limit.md)が定義されている場合、クエリは必要な異なる行の数を読み取った時点で即座に終了します。
- データブロックはクエリの実行が完了するのを待たずに処理されるとそのまま出力されます。
