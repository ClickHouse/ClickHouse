---
slug: /sql-reference/table-functions/generate_series
sidebar_position: 146
sidebar_label: 'generate_series'
title: 'generate_series (generateSeries)'
description: 'start から stop までの整数を両端を含めて格納した、単一の `generate_series` カラム（UInt64）を持つテーブルを返します。'
doc_type: 'reference'
---

エイリアス: `generateSeries`

<div id="syntax">
  ## 構文
</div>

start から stop までの整数を両端を含めて格納する、単一の &#39;generate&#95;series&#39; カラム (`UInt64`) を持つテーブルを返します。

```sql
generate_series(START, STOP)
```

`STEP` で指定した値の間隔で、start から stop まで (両端を含む) の整数を含む、単一の &#39;generate&#95;series&#39; カラム (`UInt64`) を持つテーブルを返します:

```sql
generate_series(START, STOP, STEP)
```

`STEP` には負の値を指定できます。その場合、数列は `START` から `STOP` に向かって降順で生成されます。`STEP` が負で、かつ `START < STOP` の場合、結果は空です。

<div id="examples">
  ## 例
</div>

以下のクエリは、内容は同じでカラム名が異なるテーブルを返します。

```sql
SELECT * FROM numbers(10, 5);
```

```response
┌─number─┐
│     10 │
│     11 │
│     12 │
│     13 │
│     14 │
└────────┘
```

```sql
SELECT * FROM generate_series(10, 14);
```

```response
┌─generate_series─┐
│              10 │
│              11 │
│              12 │
│              13 │
│              14 │
└─────────────────┘
```

また、次のクエリは内容は同じでカラム名だけが異なるテーブルを返します (ただし、2つ目の方法のほうが効率的です) 。

```sql
SELECT * FROM numbers(10, 11) WHERE number % 3 == (10 % 3);
```

```response
┌─number─┐
│     10 │
│     13 │
│     16 │
│     19 │
└────────┘
```

```sql
SELECT * FROM generate_series(10, 20, 3);
```

```response
┌─generate_series─┐
│              10 │
│              13 │
│              16 │
│              19 │
└─────────────────┘
```

降順の数列を生成します:

```sql
SELECT * FROM generate_series(9, 0, -1);
```

```response
┌─generate_series─┐
│               9 │
│               8 │
│               7 │
│               6 │
│               5 │
│               4 │
│               3 │
│               2 │
│               1 │
│               0 │
└─────────────────┘
```