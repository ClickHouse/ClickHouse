---
description: 'DISTINCT 句のドキュメント'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'DISTINCT 句'
doc_type: 'reference'
---

`SELECT DISTINCT` を指定すると、クエリ結果には一意の行のみが残ります。つまり、結果内で完全に一致する行の各組について、残るのは 1 行だけです。

一意である必要があるカラムの一覧は、`SELECT DISTINCT ON (column1, column2,...)` のように指定できます。カラムを指定しない場合は、すべてのカラムが考慮されます。

次のテーブルについて考えます:

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

カラムを指定せずに `DISTINCT` を使用する場合:

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

指定したカラムで `DISTINCT` を使用する場合:

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

<div id="distinct-and-order-by">
  ## DISTINCT と ORDER BY
</div>

ClickHouse では、1 つのクエリ内で異なるカラムに対して `DISTINCT` 句と `ORDER BY` 句を使用できます。`DISTINCT` 句は `ORDER BY` 句より前に実行されます。

次のテーブルについて考えます。

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

データの選択：

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

ソート方向を変えてデータを選択する:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

行 `2, 4` はソート前に除外されました。

クエリを記述する際は、この実装上の特性を考慮してください。

<div id="null-processing">
  ## NULL の処理
</div>

`DISTINCT` は [NULL](/ja/sql-reference/syntax#null) を、`NULL` が特定の値であり、`NULL==NULL` であるかのように扱います。言い換えると、`DISTINCT` の結果では、`NULL` を含む異なる組み合わせは 1 度しか現れません。これは、ほとんどの他の文脈での `NULL` の扱いとは異なります。

<div id="alternatives">
  ## 代替手段
</div>

集約関数を使用せず、`SELECT` 句で指定したものと同じ値の集合に [GROUP BY](/ja/sql-reference/statements/select/group-by) を適用して、同じ結果を得ることもできます。ただし、この方法は `GROUP BY` を使う方法と比べていくつか違いがあります。

* `DISTINCT` は `GROUP BY` と組み合わせて使用できます。
* [ORDER BY](../../../sql-reference/statements/select/order-by.md) を省略し、[LIMIT](../../../sql-reference/statements/select/limit.md) を指定した場合、必要な数の異なる行を読み取った時点で、クエリの実行は直ちに停止します。
* データブロックは、クエリ全体の実行が完了するのを待たず、処理され次第そのまま出力されます。