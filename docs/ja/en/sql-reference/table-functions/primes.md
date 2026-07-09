---
slug: /sql-reference/table-functions/primes
sidebar_position: 145
sidebar_label: 'primes'
title: 'primes'
description: '素数を含む単一の `prime` カラムを持つテーブルを返します。'
doc_type: 'reference'
---

* `primes()` – 2 から始まる素数を昇順で含む、単一の `prime` カラム (UInt64) を持つ無限のテーブルを返します。行数を制限するには `LIMIT` (必要に応じて `OFFSET`) を使用します。

* `primes(N)` – 2 から始まる最初の `N` 個の素数を含む、単一の `prime` カラム (UInt64) を持つテーブルを返します。

* `primes(N, M)` – `N` 番目の素数 (0 始まり) から始まる `M` 個の素数を含む、単一の `prime` カラム (UInt64) を持つテーブルを返します。

* `primes(N, M, S)` – 素数のインデックスにおけるステップ `S` で、`N` 番目の素数 (0 始まり) から `M` 個の素数を含む、単一の `prime` カラム (UInt64) を持つテーブルを返します。返される素数はインデックス `N, N + S, N + 2S, ..., N + (M - 1)S` に対応します。`S` は `>= 1` でなければなりません。

これはシステムテーブル [`system.primes`](/ja/operations/system-tables/primes) に似ています。

次のクエリは同等です:

```sql
SELECT * FROM primes(10);
SELECT * FROM primes(0, 10);
SELECT * FROM primes() LIMIT 10;
SELECT * FROM system.primes LIMIT 10;
SELECT * FROM system.primes WHERE prime IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
```

以下のクエリも同等です。

```sql
SELECT * FROM primes(10, 10);
SELECT * FROM primes() LIMIT 10 OFFSET 10;
SELECT * FROM system.primes LIMIT 10 OFFSET 10;
```

<div id="examples">
  ### 例
</div>

最初の10個の素数。

```sql
SELECT * FROM primes(10);
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

1e15を超える最初の素数。

```sql
SELECT prime FROM primes() WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```

非常に大きな範囲で素数に関する剰余制約を解きます: `p >= 10^15` を満たし、`p` を `65537` で割った余りが `1` となる最初の素数を見つけてください。

```sql
SELECT prime
FROM primes()
WHERE prime >= 1e15
  AND prime % 65537 = 1
LIMIT 1;
```

```response
 ┌────────────prime─┐
 │ 1000000001218399 │ -- 1.00 quadrillion
 └──────────────────┘
```

最初の7つのメルセンヌ素数。

```sql
SELECT prime
FROM primes()
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 7;
```

```response
  ┌──prime─┐
  │      3 │
  │      7 │
  │     31 │
  │    127 │
  │   8191 │
  │ 131071 │
  │ 524287 │
  └────────┘
```

<div id="notes">
  ### 注記
</div>

* 最も高速なのは、デフォルトの step (`1`) を使う単純な範囲クエリと point filter クエリです。たとえば、`primes(N)` や `primes() LIMIT N` です。これらの形式では、最適化された素数ジェネレータにより、非常に大きな素数も効率よく計算できます。
* 境界のないソース (`primes()` / `system.primes`) では、`prime BETWEEN ...`、`prime IN (...)`、`prime = ...` のような単純な値フィルタを生成時に適用して、探索する値の範囲を絞り込めます。たとえば、次のクエリはほぼ瞬時に実行されます。

```sql
SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN 1e6 AND 1e6 + 100
   OR prime BETWEEN 1e12 AND 1e12 + 100
   OR prime BETWEEN 1e15 AND 1e15 + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;
```

```response
  ┌───────sum(prime)─┐
  │ 2004010006000641 │ -- 2.00 quadrillion
  └──────────────────┘

1 row in set. Elapsed: 0.090 sec. 
```

* この値域最適化は、`WHERE` を伴う境界付き table function (`primes(N)`、`primes(offset, count[, step])`) には適用されません。これらのバリアントは素数のインデックスによって有限の table を定義するため、意味を保つには、その table を生成した後で filter を評価する必要があるためです。
* 0 以外の offset や 1 より大きい step を使用する場合 (`primes(offset, count)` / `primes(offset, count, step)`) 、内部的に追加の素数を生成してスキップする必要が生じることがあるため、処理が遅くなる可能性があります。offset や step が不要であれば、省略してください。