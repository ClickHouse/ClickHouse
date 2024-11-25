---
slug: /ja/sql-reference/aggregate-functions/reference/boundingRatio
sidebar_position: 114
title: boundingRatio
---

グループの値の中で最も左と最も右の点間の勾配を計算する集計関数。

例:

サンプルデータ:
```sql
SELECT
    number,
    number * 1.5
FROM numbers(10)
```
```response
┌─number─┬─multiply(number, 1.5)─┐
│      0 │                     0 │
│      1 │                   1.5 │
│      2 │                     3 │
│      3 │                   4.5 │
│      4 │                     6 │
│      5 │                   7.5 │
│      6 │                     9 │
│      7 │                  10.5 │
│      8 │                    12 │
│      9 │                  13.5 │
└────────┴───────────────────────┘
```

`boundingRatio()` 関数は、最も左の点と最も右の点の間の線の勾配を返します。上記のデータでのこれらの点は `(0,0)` と `(9,13.5)` です。

```sql
SELECT boundingRatio(number, number * 1.5)
FROM numbers(10)
```
```response
┌─boundingRatio(number, multiply(number, 1.5))─┐
│                                          1.5 │
└──────────────────────────────────────────────┘
```


