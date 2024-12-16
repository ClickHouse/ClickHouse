---
slug: /ja/materialized-view
title: Materialized View
description: Materialized Viewを使用してクエリを高速化する方法
keywords: [materialized views, クエリを高速化, クエリ最適化]
---

# Materialized View

Materialized Viewを使用すると、計算のコストをクエリ時から挿入時に移行し、`SELECT`クエリを高速化できます。

Postgresなどのトランザクショナルデータベースとは異なり、ClickHouseのMaterialized Viewは、データがテーブルに挿入される際にクエリを実行するトリガーに過ぎません。このクエリの結果は、2つ目の「ターゲット」テーブルに挿入されます。さらに多くの行が挿入されると、その結果がターゲットテーブルに送られ、中間結果が更新およびマージされます。このマージされた結果は、元のデータ全体に対してクエリを実行したときと同等です。

Materialized Viewの主な動機は、ターゲットテーブルに挿入される結果が、行に対する集計、フィルタリング、または変換の結果を表していることです。これらの結果は、しばしば元のデータの小さな表現（集計の場合は部分的なスケッチ）になります。これにより、ターゲットテーブルから結果を読み取るためのクエリが単純であるため、元のデータに対して同じ計算を行った場合よりもクエリ時間が短くなることが保証され、計算（したがってクエリの遅延）がクエリ時から挿入時に移行されます。

ClickHouseのMaterialized Viewは、ベースにしたテーブルにデータが流入するとリアルタイムで更新され、継続的に更新され続けるインデックスのように機能します。これは、多くのデータベースにおいてMaterialized Viewがクエリの静的なスナップショットであり、新しくリフレッシュする必要があること（ClickHouseの[リフレッシャブルMaterialized View](/ja/sql-reference/statements/create/view#refreshable-materialized-view)と似ています）とは対照的です。

<img src={require('./images/materialized-view-diagram.png').default}    
  class='image'
  alt='Materialized view diagram'
  style={{width: '500px'}} />

## 例

1日の投稿に対するアップ投票とダウン投票の数を取得したいとします。

ClickHouseの[`toStartOfDay`](/ja/sql-reference/functions/date-time-functions#tostartofday)関数のおかげで、これはかなり簡単なクエリです:

```sql
SELECT toStartOfDay(CreationDate) AS day,
       countIf(VoteTypeId = 2) AS UpVotes,
       countIf(VoteTypeId = 3) AS DownVotes
FROM votes
GROUP BY day
ORDER BY day ASC
LIMIT 10

┌─────────────────day─┬─UpVotes─┬─DownVotes─┐
│ 2008-07-31 00:00:00 │   	6 │     	0 │
│ 2008-08-01 00:00:00 │ 	182 │    	50 │
│ 2008-08-02 00:00:00 │ 	436 │   	107 │
│ 2008-08-03 00:00:00 │ 	564 │   	100 │
│ 2008-08-04 00:00:00 │	1306 │   	259 │
│ 2008-08-05 00:00:00 │	1368 │   	269 │
│ 2008-08-06 00:00:00 │	1701 │   	211 │
│ 2008-08-07 00:00:00 │	1544 │   	211 │
│ 2008-08-08 00:00:00 │	1241 │   	212 │
│ 2008-08-09 00:00:00 │ 	576 │    	46 │
└─────────────────────┴─────────┴───────────┘

10 rows in set. Elapsed: 0.133 sec. Processed 238.98 million rows, 2.15 GB (1.79 billion rows/s., 16.14 GB/s.)
Peak memory usage: 363.22 MiB.
```

このクエリは、すでにClickHouseのおかげで高速ですが、さらに改善できるでしょうか？

Materialized Viewを使用してこれを挿入時に計算したい場合、結果を受け取るテーブルが必要です。このテーブルは、1日につき1行だけを保持する必要があります。既存の日のデータが更新される場合、他のカラムは既存の日の行にマージされるべきです。このインクリメンタルな状態のマージを行うためには、他のカラムの部分的な状態を保持する必要があります。

これは、ClickHouseにおける特別なエンジンタイプを必要とします: [SummingMergeTree](/ja/engines/table-engines/mergetree-family/summingmergetree)。これは、全ての数値カラムの合計値を含む1つの行に置き換えます。以下のテーブルは、同じ日付を持つ行をマージし、数値カラムを合計します:

```
CREATE TABLE up_down_votes_per_day
(
  `Day` Date,
  `UpVotes` UInt32,
  `DownVotes` UInt32
)
ENGINE = SummingMergeTree
ORDER BY Day
```

Materialized Viewを示すために、votesテーブルが空であり、まだデータを受け取っていないと仮定します。我々のMaterialized Viewは、`votes`に挿入されたデータに対して上記の`SELECT`を実行し、その結果を`up_down_votes_per_day`に送ります:

```sql
CREATE MATERIALIZED VIEW up_down_votes_per_day_mv TO up_down_votes_per_day AS
SELECT toStartOfDay(CreationDate)::Date AS Day,
       countIf(VoteTypeId = 2) AS UpVotes,
       countIf(VoteTypeId = 3) AS DownVotes
FROM votes
GROUP BY Day
```

ここでの`TO`句は重要で、結果が送られる場所、すなわち`up_down_votes_per_day`を示しています。

以前の挿入からvotesテーブルを再度ポピュレートできます:

```sql
INSERT INTO votes SELECT toUInt32(Id) AS Id, toInt32(PostId) AS PostId, VoteTypeId, CreationDate, UserId, BountyAmount
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes.parquet')

0 rows in set. Elapsed: 111.964 sec. Processed 477.97 million rows, 3.89 GB (4.27 million rows/s., 34.71 MB/s.)
Peak memory usage: 283.49 MiB.
```

完了後、`up_down_votes_per_day`のサイズを確認できます - 1日につき1行になるはずです:

```
SELECT count()
FROM up_down_votes_per_day
FINAL

┌─count()─┐
│	5723 │
└─────────┘
```

ここで、`votes`における238百万行から、このクエリの結果を保存することによって5000に行数を効果的に減少させました。ここでの鍵は、`votes`テーブルに新しい投票が挿入されるたびに、新しい値がその日に対して`up_down_votes_per_day`に送られ、それらはバックグラウンドで自動的に非同期でマージされることです - 1日につき1行のみを保持します。`up_down_votes_per_day`は常に小さく、最新の状態に保たれます。

行のマージが非同期で行われるため、ユーザーがクエリする際に1日あたり複数の投票が存在する可能性があります。クエリ時に未結合の行を確実にマージするために、以下の2つのオプションがあります:

- テーブル名に`FINAL`修飾子を使用する。これは上記のカウントクエリで行いました。
- 最終テーブルで使用した並び替えキー（例: `CreationDate`）で集約し、メトリックを合計します。これは通常、より効率的で柔軟ですが（テーブルが他のことにも使用できます）、前者は一部のクエリにとっては単純かもしれません。以下で両方を示します:

```sql
SELECT
	Day,
	UpVotes,
	DownVotes
FROM up_down_votes_per_day
FINAL
ORDER BY Day ASC
LIMIT 10

10 rows in set. Elapsed: 0.004 sec. Processed 8.97 thousand rows, 89.68 KB (2.09 million rows/s., 20.89 MB/s.)
Peak memory usage: 289.75 KiB.

SELECT Day, sum(UpVotes) AS UpVotes, sum(DownVotes) AS DownVotes
FROM up_down_votes_per_day
GROUP BY Day
ORDER BY Day ASC
LIMIT 10
┌────────Day─┬─UpVotes─┬─DownVotes─┐
│ 2008-07-31 │   	6 │     	0 │
│ 2008-08-01 │ 	182 │    	50 │
│ 2008-08-02 │ 	436 │   	107 │
│ 2008-08-03 │ 	564 │   	100 │
│ 2008-08-04 │	1306 │   	259 │
│ 2008-08-05 │	1368 │   	269 │
│ 2008-08-06 │	1701 │   	211 │
│ 2008-08-07 │	1544 │   	211 │
│ 2008-08-08 │	1241 │   	212 │
│ 2008-08-09 │ 	576 │    	46 │
└────────────┴─────────┴───────────┘

10 rows in set. Elapsed: 0.010 sec. Processed 8.97 thousand rows, 89.68 KB (907.32 thousand rows/s., 9.07 MB/s.)
Peak memory usage: 567.61 KiB.
```

これにより、クエリ時間を0.133秒から0.004秒 - 25倍以上の改善になりました！

### より複雑な例

上記の例では、Dailyの合計値を2つ取り扱うためにMaterialized Viewを使用しています。合計は、部分的な状態を維持するための最も簡単な集計形式です - 新しい値が到着したときに既存の値に追加するだけで済みます。しかし、ClickHouseのMaterialized Viewは、あらゆるタイプの集計に使用することができます。

たとえば、投稿の統計を各日数に対して計算したいとしましょう: `Score`の99.9パーセンタイルと`CommentCount`の平均です。この計算のためのクエリは以下のようになるかもしれません:

```sql
SELECT
	toStartOfDay(CreationDate) AS Day,
	quantile(0.999)(Score) AS Score_99th,
	avg(CommentCount) AS AvgCommentCount
FROM posts
GROUP BY Day
ORDER BY Day DESC
LIMIT 10

	┌─────────────────Day─┬────────Score_99th─┬────AvgCommentCount─┐
 1. │ 2024-03-31 00:00:00 │  5.23700000000008 │ 1.3429811866859624 │
 2. │ 2024-03-30 00:00:00 │             	5 │ 1.3097158891616976 │
 3. │ 2024-03-29 00:00:00 │  5.78899999999976 │ 1.2827635327635327 │
 4. │ 2024-03-28 00:00:00 │             	7 │  1.277746158224246 │
 5. │ 2024-03-27 00:00:00 │ 5.738999999999578 │ 1.2113264918282023 │
 6. │ 2024-03-26 00:00:00 │             	6 │ 1.3097536945812809 │
 7. │ 2024-03-25 00:00:00 │             	6 │ 1.2836721018539201 │
 8. │ 2024-03-24 00:00:00 │ 5.278999999999996 │ 1.2931667891256429 │
 9. │ 2024-03-23 00:00:00 │ 6.253000000000156 │  1.334061135371179 │
10. │ 2024-03-22 00:00:00 │ 9.310999999999694 │ 1.2388059701492538 │
	└─────────────────────┴───────────────────┴────────────────────┘

10 rows in set. Elapsed: 0.113 sec. Processed 59.82 million rows, 777.65 MB (528.48 million rows/s., 6.87 GB/s.)
Peak memory usage: 658.84 MiB.
```

前述のように、postsテーブルに新しい投稿が挿入されるとき、上記のクエリを実行するMaterialized Viewを作成できます。

例の目的で、S3からの投稿データのロードを避けるために、`posts`と同じスキーマを持つ重複テーブル`posts_null`を作成します。ただし、このテーブルはデータを保存せず、Materialized Viewが行を挿入する際に使用されます。データの保存を防ぐために、[`Null`テーブルエンジンタイプ](/ja/engines/table-engines/special/null)を使用できます。

```sql
CREATE TABLE posts_null AS posts ENGINE = Null
```

Nullテーブルエンジンは強力な最適化です - `/dev/null`のようなものです。我々のMaterialized Viewは、挿入時に`posts_null`テーブルが行を受け取ると統計を計算して格納します - ただのトリガーに過ぎません。ただし、生データは保存されません。実際には、元の投稿を保存する必要があるかもしれませんが、このアプローチは集計を計算しながら生データのストレージオーバーヘッドを回避するために使用できます。

それゆえ、Materialized Viewは以下のようになります:

```sql
CREATE MATERIALIZED VIEW post_stats_mv TO post_stats_per_day AS
       SELECT toStartOfDay(CreationDate) AS Day,
       quantileState(0.999)(Score) AS Score_quantiles,
       avgState(CommentCount) AS AvgCommentCount
FROM posts_null
GROUP BY Day
```

注: ここでは、集約関数の末尾に`State`サフィックスを付けています。これにより、関数の集約状態が返され、最終的な結果ではなく、他の状態とマージするための追加情報を持つ部分状態が含まれます。たとえば平均の場合、これはカウントとカラムの合計を含みます。

> 部分的な集計状態は正しい結果を計算するために必要です。たとえば、平均を計算する場合、サブレンジの平均を単純に平均化することは誤った結果をもたらします。

次に、このビューのターゲットテーブル`post_stats_per_day`を作成し、これらの部分的集計状態を保存します:

```sql
CREATE TABLE post_stats_per_day
(
  `Day` Date,
  `Score_quantiles` AggregateFunction(quantile(0.999), Int32),
  `AvgCommentCount` AggregateFunction(avg, UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY Day
```

以前は、カウントを保存するための`SummingMergeTree`が十分でしたが、他の関数にはより高度なエンジンタイプが必要です: [`AggregatingMergeTree`](/ja/engines/table-engines/mergetree-family/aggregatingmergetree)。
ClickHouseに集約状態が保存されることを確実に知らせるために、`Score_quantiles`と`AvgCommentCount`を`AggregateFunction`タイプとして定義し、部分的状態とソースカラムのタイプの関数ソースを指定します。`SummingMergeTree`のように、同じ`ORDER BY`キー値を持つ行がマージされます（この例の`Day`）

Materialized Viewを経由して`post_stats_per_day`をポピュレートするには、`posts`から`posts_null`にすべての行を挿入するだけです:

```sql
INSERT INTO posts_null SELECT * FROM posts

0 rows in set. Elapsed: 13.329 sec. Processed 119.64 million rows, 76.99 GB (8.98 million rows/s., 5.78 GB/s.)
```

> プロダクションでは、おそらくMaterialized Viewを`posts`テーブルにアタッチすることになります。ここでは、nullテーブルを示すために`posts_null`を使用しました。

最終クエリは、関数に`Merge`サフィックスを使用する必要があります（カラムが部分的な集計状態を格納しているため）:

```sql
SELECT
	Day,
	quantileMerge(0.999)(Score_quantiles),
	avgMerge(AvgCommentCount)
FROM post_stats_per_day
GROUP BY Day
ORDER BY Day DESC
LIMIT 10
```

ここでは、`FINAL`を使用する代わりに`GROUP BY`を使用します。

## 他の用途

上記では主に、データの部分集計をインクリメンタルに更新する目的でMaterialized Viewを使用し、クエリ時間から挿入時間へと計算を移行することに焦点を当てています。この一般的な使用ケースを超え、Materialized Viewには他の多くの用途があります。

### フィルタリングと変換

場合によっては、挿入時に行とカラムのサブセットのみを挿入したい場合があります。この場合、`posts_null`テーブルは挿入を受け、`posts`テーブルへの挿入前に`SELECT` クエリで行をフィルタリングできます。たとえば、`posts`テーブルの`Tags`カラムを変換したいとします。これはタグ名のパイプ区切りリストを含んでいます。これらを配列に変換することで、個々のタグ値による集計がより簡単になります。

> この変換は、`INSERT INTO SELECT` を実行するときにも実行できます。Materialized ViewはこのロジックをClickHouse DDLにカプセル化し、`INSERT`をシンプルに保ちながら、新しい行に変換を適用できます。

この変換のためのMaterialized Viewは以下のようになります:

```sql
CREATE MATERIALIZED VIEW posts_mv TO posts AS
   	SELECT * EXCEPT Tags, arrayFilter(t -> (t != ''), splitByChar('|', Tags)) as Tags FROM posts_null
```

### 参照テーブル

ユーザーはClickHouseの順序付けキーを選択する際に、そのフィルタリングや集計句で頻繁に使用されるカラムを選択する必要があります。しかし、これは単一のカラムセットでカプセル化できない、より多様なアクセスパターンを持つシナリオにおいて制約を与える可能性があります。たとえば、次の`comments`テーブルを考えてみましょう:

```sql
CREATE TABLE comments
(
	`Id` UInt32,
	`PostId` UInt32,
	`Score` UInt16,
	`Text` String,
	`CreationDate` DateTime64(3, 'UTC'),
	`UserId` Int32,
	`UserDisplayName` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY PostId

0 rows in set. Elapsed: 46.357 sec. Processed 90.38 million rows, 11.14 GB (1.95 million rows/s., 240.22 MB/s.)
```

ここでの順序付けキーは、`PostId` でフィルタリングするクエリに対してテーブルを最適化しています。

特定の`UserId`でフィルタリングし、平均`Score`を計算したい状況を考えてみましょう:

```sql
SELECT avg(Score)
FROM comments
WHERE UserId = 8592047

   ┌──────────avg(Score)─┐
1. │ 0.18181818181818182 │
   └─────────────────────┘

1 row in set. Elapsed: 0.778 sec. Processed 90.38 million rows, 361.59 MB (116.16 million rows/s., 464.74 MB/s.)
Peak memory usage: 217.08 MiB.
```

高速です（データはClickHouseにとって小さい）が、プロセスされた行数 - 90.38百万からフルテーブルスキャンを必要とすることがわかります。より大きなデータセットに対して、フィルタリングカラム`UserId`のための順序付けキー値`PostId`を参照するMaterialized Viewを使用して、このクエリを効率的に加速することができます。これらの値は効率的な参照に使用できます。

この例では、Materialized Viewはとてもシンプルで、挿入時に`comments`から`PostId`と`UserId`のみを選択します。これらの結果は、`UserId`で並び替えられた`comments_posts_users`テーブルに送られます。以下で`comments`テーブルのnullバージョンを作成し、このビューと`comments_posts_users`テーブルをポピュレートするために使用します:

```sql
CREATE TABLE comments_posts_users (
  PostId UInt32,
  UserId Int32
) ENGINE = MergeTree ORDER BY UserId 


CREATE TABLE comments_null AS comments
ENGINE = Null

CREATE MATERIALIZED VIEW comments_posts_users_mv TO comments_posts_users AS
SELECT PostId, UserId FROM comments_null

INSERT INTO comments_null SELECT * FROM comments

0 rows in set. Elapsed: 5.163 sec. Processed 90.38 million rows, 17.25 GB (17.51 million rows/s., 3.34 GB/s.)
```

このビューをサブクエリで使用して前のクエリを加速できます:

```sql
SELECT avg(Score)
FROM comments
WHERE PostId IN (
	SELECT PostId
	FROM comments_posts_users
	WHERE UserId = 8592047
) AND UserId = 8592047


   ┌──────────avg(Score)─┐
1. │ 0.18181818181818182 │
   └─────────────────────┘

1 row in set. Elapsed: 0.012 sec. Processed 88.61 thousand rows, 771.37 KB (7.09 million rows/s., 61.73 MB/s.)
```

### チェイン化

Materialized Viewはチェイン化可能で、複雑なワークフローを確立できます。実際の例については、この[ブログ投稿](https://clickhouse.com/blog/chaining-materialized-views)をお勧めします。
