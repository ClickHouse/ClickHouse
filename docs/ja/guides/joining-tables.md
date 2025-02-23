---
title: ClickHouseでのJOINの使用
description: ClickHouseでテーブルを結合する方法
keywords: [joins, join tables]
---

ClickHouseは、多様な結合アルゴリズムを備えた[フルの`JOIN`サポート](https://clickhouse.com/blog/clickhouse-fully-supports-joins-part1)を提供しています。パフォーマンスを最大限に引き出すために、このガイドに記載されているJOINの最適化の提案に従うことを推奨します。

- 最適なパフォーマンスを得るには、特にリアルタイムの分析ワークロードにおいて、クエリでの`JOIN`の数を削減することを目指すべきです。クエリ内のJOINは最大3から4までに制限してください。[データモデリングセクション](/ja/data-modeling/schema-design)では、正規化解除、Dictionary、マテリアライズドビューなど、JOINを最小化するためのいくつかの変更について詳しく説明しています。
- 現在、ClickHouseはJOINの順序を変更しません。常に最小のテーブルをJOINの右側に配置することを確認してください。ほとんどの結合アルゴリズムではこれがメモリに保持され、クエリのメモリオーバーヘッドを最低限に抑えます。
- クエリが直接のJOINを必要とする場合、つまり`LEFT ANY JOIN`のような場合は、可能であれば[Dictionary](/ja/dictionary)を使用することをお勧めします。

<br />

<img src={require('./images/joins-1.png').default}
    alt='NEEDS ALT'
    class='image'
    style={{width: '250px'}}
/>

<br />

- INNER JOINを実行する場合、これらを`IN`句を使用したサブクエリとして書くことがしばしばより最適です。以下のクエリを考えてみましょう。これらは機能的には同等で、両方とも質問にClickHouseが言及されていないが、`comments`には言及されている`posts`の数を見つけます。

```sql
SELECT count()
FROM stackoverflow.posts AS p
ANY INNER `JOIN` stackoverflow.comments AS c ON p.Id = c.PostId
WHERE (p.Title != '') AND (p.Title NOT ILIKE '%clickhouse%') AND (p.Body NOT ILIKE '%clickhouse%') AND (c.Text ILIKE '%clickhouse%')

┌─count()─┐
│  	86 │
└─────────┘

1 row in set. Elapsed: 8.209 sec. Processed 150.20 million rows, 56.05 GB (18.30 million rows/s., 6.83 GB/s.)
Peak memory usage: 1.23 GiB.
```

注意すべきは、直積を避けるため`ANY INNER JOIN`を使用していることであり、つまり各ポストに対して1つの一致のみを望んでいます。

このJOINはサブクエリを使用して書き直すことで、パフォーマンスが大幅に向上します。

```sql
SELECT count()
FROM stackoverflow.posts
WHERE (Title != '') AND (Title NOT ILIKE '%clickhouse%') AND (Body NOT ILIKE '%clickhouse%') AND (Id IN (
	SELECT PostId
	FROM stackoverflow.comments
	WHERE Text ILIKE '%clickhouse%'
))
┌─count()─┐
│  	86 │
└─────────┘

1 row in set. Elapsed: 2.284 sec. Processed 150.20 million rows, 16.61 GB (65.76 million rows/s., 7.27 GB/s.)
Peak memory usage: 323.52 MiB.
```

ClickHouseはすべてのJOIN句やサブクエリに条件を適用しようとしますが、可能であれば常に条件を手動ですべてのサブ句に適用し、`JOIN`するデータのサイズを最小化することをお勧めします。以下の例を考えてみましょう。ここでは、2020年以降のJava関連の投稿についてのアップボート数を計算したいと考えています。

大きなテーブルを左側に配置した素朴なクエリは56秒で完了します。

```sql
SELECT countIf(VoteTypeId = 2) AS upvotes
FROM stackoverflow.posts AS p
INNER JOIN stackoverflow.votes AS v ON p.Id = v.PostId
WHERE has(arrayFilter(t -> (t != ''), splitByChar('|', p.Tags)), 'java') AND (p.CreationDate >= '2020-01-01')

┌─upvotes─┐
│  261915 │
└─────────┘

1 row in set. Elapsed: 56.642 sec. Processed 252.30 million rows, 1.62 GB (4.45 million rows/s., 28.60 MB/s.)
```

JOINを再配置することでパフォーマンスが劇的に改善され、1.5秒になりました。

```sql
SELECT countIf(VoteTypeId = 2) AS upvotes
FROM stackoverflow.votes AS v
INNER JOIN stackoverflow.posts AS p ON v.PostId = p.Id
WHERE has(arrayFilter(t -> (t != ''), splitByChar('|', p.Tags)), 'java') AND (p.CreationDate >= '2020-01-01')

┌─upvotes─┐
│  261915 │
└─────────┘

1 row in set. Elapsed: 1.519 sec. Processed 252.30 million rows, 1.62 GB (166.06 million rows/s., 1.07 GB/s.)
```

右側のテーブルにフィルタを追加することで、パフォーマンスがさらに向上し、0.5秒になりました。

```sql
SELECT countIf(VoteTypeId = 2) AS upvotes
FROM stackoverflow.votes AS v
INNER JOIN stackoverflow.posts AS p ON v.PostId = p.Id
WHERE has(arrayFilter(t -> (t != ''), splitByChar('|', p.Tags)), 'java') AND (p.CreationDate >= '2020-01-01') AND (v.CreationDate >= '2020-01-01')

┌─upvotes─┐
│  261915 │
└─────────┘

1 row in set. Elapsed: 0.597 sec. Processed 81.14 million rows, 1.31 GB (135.82 million rows/s., 2.19 GB/s.)
Peak memory usage: 249.42 MiB.
```

このクエリは前述のように`INNER JOIN`をサブクエリに移動し、外側および内側のクエリにフィルタを維持することでさらに改善できます。

```sql
SELECT count() AS upvotes
FROM stackoverflow.votes
WHERE (VoteTypeId = 2) AND (PostId IN (
	SELECT Id
	FROM stackoverflow.posts
	WHERE (CreationDate >= '2020-01-01') AND has(arrayFilter(t -> (t != ''), splitByChar('|', Tags)), 'java')
))

┌─upvotes─┐
│  261915 │
└─────────┘

1 row in set. Elapsed: 0.383 sec. Processed 99.64 million rows, 804.55 MB (259.85 million rows/s., 2.10 GB/s.)
Peak memory usage: 250.66 MiB.
```

## JOINアルゴリズムの選択

ClickHouseは、いくつかの[joinアルゴリズム](https://clickhouse.com/blog/clickhouse-fully-supports-joins-part1)をサポートしています。これらのアルゴリズムは通常、メモリ使用量とパフォーマンスをトレードオフします。以下は、メモリ消費と実行時間に基づくClickHouse joinアルゴリズムの概要です：

<br />

<img src={require('./images/joins-2.png').default}
    alt='NEEDS ALT'
    class='image'
    style={{width: '500px'}}
/>

<br />

これらのアルゴリズムは、JOINクエリの計画と実行方法を決定します。デフォルトでは、ClickHouseは使用されたJOINタイプおよび結合テーブルのエンジンに基づいて直接またはハッシュJOINアルゴリズムを使用します。代わりに、ClickHouseを設定して、リソースの可用性と使用量に応じてランタイムで使用するJOINアルゴリズムを柔軟に選択し、動的に変更することもできます：`join_algorithm=auto`の場合、ClickHouseは最初にハッシュJOINアルゴリズムを試し、そのアルゴリズムのメモリ制限が超えた場合、そのアルゴリズムを部分マージJOINに即座に切り替えます。選択されたアルゴリズムをトレースログで観察することができます。ClickHouseはまた、`join_algorithm`設定を介してユーザーが希望のJOINアルゴリズムを自分で指定することも許可しています。

各JOINアルゴリズムのサポートされている`JOIN`タイプは以下に示されており、最適化を考慮する前に考慮するべきです：

<br />

<img src={require('./images/joins-3.png').default}
    alt='NEEDS ALT'
    class='image'
    style={{width: '600px'}}
/>

<br />

各`JOIN`アルゴリズムの詳細な説明、利点、欠点、スケーリング特性については[こちら](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)で見つけることができます。

適切なJOINアルゴリズムの選択は、メモリの最適化を行うかパフォーマンスを最適化するかによります。

## JOINパフォーマンスの最適化

主要な最適化指標がパフォーマンスであり、JOINを可能な限り早く実行したい場合は、以下の意思決定ツリーを使用して適切なJOINアルゴリズムを選択できます：

<br />

<img src={require('./images/joins-4.png').default}
    alt='NEEDS ALT'
    class='image'
    style={{width: '600px'}}
/>

<br />

- **(1)** 右側のテーブルからのデータがメモリ内の低レイテンシのキー・バリューデータ構造、例えばDictionaryに事前ロードでき、JOINキーが基礎となるキー・バリューのストレージのキー属性に一致し、`LEFT ANY JOIN`のセマンティクスで十分である場合、**直接JOIN**が適用可能であり、最速のアプローチを提供します。

- **(2)** テーブルの[物理的な行順序](/ja/optimize/sparse-primary-indexes#data-is-stored-on-disk-ordered-by-primary-key-columns)がJOINキーのソート順に一致する場合、この場合、**フルソートマージJOIN**はソートフェーズを[スキップ](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3#utilizing-physical-row-order)し、メモリ消費を大幅に削減し、データサイズやJOINキー値の分布に応じて、一部のハッシュJOINアルゴリズムよりも速い実行時間を持ちます。

- **(3)** 右テーブルがメモリに収まる場合、追加のメモリ使用量のオーバーヘッドがあっても、**並列ハッシュJOIN**はこのアルゴリズムまたはハッシュJOINがより速い可能性があります。これはデータサイズ、データ型、およびJOINキーの値の分布に依存します。

- **(4)** 右テーブルがメモリに収まらない場合、それは再び依存します。ClickHouseは3つのメモリ非依存のJOINアルゴリズムを提供しています。すべてが一時的にデータをディスクにスピルします。**フルソートマージJOIN**および**部分マージJOIN**は、データの事前ソートを必要とします。**グレースハッシュJOIN**は、代わりにデータからハッシュテーブルを構築します。データ量、データ型、JOINキーの値の分布に基づいて、データをソートするよりも、データからハッシュテーブルを構築する方が速いシナリオ、またその逆のシナリオがあります。

部分マージJOINは、大きなテーブルを結合する際にメモリ使用量を最小化するよう最適化されており、JOIN速度が非常に遅い代償としてあります。これは特に、左テーブルの物理的な行順序がJOINキーのソート順と一致しない場合に顕著です。

グレースハッシュJOINは、3つのメモリ非依存のJOINアルゴリズムの中で最も柔軟であり、[grace_hash_join_initial_buckets](https://github.com/ClickHouse/ClickHouse/blob/23.5/src/Core/Settings.h#L759)設定を使用してメモリ使用量とJOIN速度を良好に制御できます。データ量に応じて、グレースハッシュが部分マージアルゴリズムよりも速いまたは遅い場合があります。両方のアルゴリズムのメモリ使用量がほぼ一致するように選択されたバケツ数の場合です。メモリ使用量が完全ソートマージとほぼ一致するようにグレースハッシュJOINのメモリ使用量を構成すると、完全ソートマージがテストランでは常に速かったです。

メモリ非依存の3つのアルゴリズムのうち、どれが最速かは、データの量、データ型、JOINキーの値の分布に依存します。現実的なデータ量で現実的なデータを使用してベンチマークを行い、どのアルゴリズムが最速かを判断するのが最良です。

## メモリの最適化

JOINを最速ではなく最も低いメモリ使用量で最適化したい場合は、代わりに以下の意思決定ツリーを使用できます：

<br />

<img src={require('./images/joins-5.png').default}
    alt='NEEDS ALT'
    class='image'
    style={{width: '400px'}}
/>

<br />

- **(1)** テーブルの物理的な行順序がJOINキーのソート順に一致する場合、**フルソートマージJOIN**のメモリ使用量は、これ以上低くはなりません。加えて、ソートフェーズが[無効](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3#utilizing-physical-row-order)になっているため、良好なJOIN速度の利点があります。
- **(2)** **グレースハッシュJOIN**は[JOIN速度](https://github.com/ClickHouse/ClickHouse/blob/23.5/src/Core/Settings.h#L759)の犠牲にして、高い[バケット数](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2#description-2)を構成することで、非常に低いメモリ使用に調整できます。**部分マージJOIN**は、意図的にメインメモリの使用を低くします。**フルソートマージJOIN**は、外部ソートが有効になっている場合は通常、部分マージJOINよりも多くのメモリを使用しますが、それにより大幅に良好なJOIN実行時間の利点があります。

上記の詳細についてさらなる情報が必要なユーザには、この[ブログシリーズ](https://clickhouse.com/blog/clickhouse-fully-supports-joins-part1)をお勧めします。

