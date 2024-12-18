---
slug: /ja/data-modeling/denormalization
title: データの非正規化
description: クエリパフォーマンスを向上させるためのデータ非正規化の使用法
keywords: [データの非正規化, 非正規化, クエリ最適化]
---

# データの非正規化

データの非正規化は、ClickHouseにおいて、テーブルを平坦化して結合を避けることでクエリ遅延を最小化する技術です。

## 正規化されたスキーマ vs. 非正規化されたスキーマの比較

データの非正規化は、特定のクエリパターンのためにデータベースパフォーマンスを最適化する目的で、意図的に正規化プロセスを逆にします。正規化されたデータベースでは、冗長性を最小化しデータ整合性を確保するために、データは複数の関連したテーブルに分割されます。非正規化は、テーブルを結合し、データを重複させ、計算されたフィールドを単一または少数のテーブルに組み込むことで冗長性を再導入します - つまり、クエリ時に結合するのではなく挿入時に結合させます。

このプロセスはクエリ時の複雑な結合の必要性を削減し、読み取り操作を大幅に高速化できるため、重い読み込み要求と複雑なクエリを持つアプリケーションに最適です。しかし、書き込み操作やメンテナンスの複雑さが増し、重複したデータの変更を全てのインスタンスに伝播して整合性を保つ必要があります。

<img src={require('./images/denormalization-diagram.png').default}
  class='image'
  alt='ClickHouseにおける非正規化'
  style={{width: '100%', background: 'none' }} />

<br />

NoSQLソリューションが普及した一般的な技術は、`JOIN`サポートがない場合にデータを非正規化して、統計や関連する行を親行にカラムやネストされたオブジェクトとして保存することです。例えば、ブログの例のスキーマでは、全ての`Comments`を該当する投稿のオブジェクトの`Array`として保存できます。

## 非正規化を使用するべき時

一般的に、以下のケースで非正規化をお勧めします:

- 頻繁に変更されないテーブル、またはデータが分析クエリに利用可能になるまでの遅延が許容できるテーブルを非正規化します。つまり、データをバッチで完全に再ロードできます。
- 多対多の関係を非正規化しないようにします。1つの元データ行が変更されると多くの行を更新する必要が生じる可能性があります。
- カーディナリティが高い関係を非正規化しないようにします。テーブル内の各行が別のテーブルに数千の関連エントリを持つ場合、これらは`Array`として表現される必要があります - プリミティブタイプまたはタプルのどちらかです。通常、1000以上のタプルを持つ配列はお勧めしません。
- 全てのカラムをネストされたオブジェクトとして非正規化するより、Materialized Viewを使用して統計のみを非正規化することを検討してください（以下参照）。

全ての情報を非正規化する必要はなく、頻繁にアクセスされる重要な情報のみを非正規化してください。

非正規化の作業はClickHouseまたは上流で処理できます。例えば、Apache Flinkを使用します。

## 頻繁に更新されるデータでの非正規化を避ける

ClickHouseのために、非正規化はクエリパフォーマンスを最適化するためにユーザーが使用できるいくつかのオプションの1つですが、注意して使用する必要があります。データが頻繁に更新され、リアルタイムに近い更新が必要な場合、このアプローチは避けるべきです。メインテーブルが主に追加のみであるか、バッチとして定期的に再ロードできる場合に使用します。 

アプローチとしては主に、書き込みパフォーマンスとデータの更新という1つの主要な課題を抱えています。具体的には、非正規化はデータの結合の責任をクエリ時からインジェスト時に効果的に移します。これによりクエリパフォーマンスが大幅に向上する一方で、インジェストを複雑にし、データパイプラインが変更された場合に行をClickHouseに再挿入する必要があります。つまり、単一の元データ行の変更は、ClickHouse内の多くの行を更新する必要があるかもしれません。複雑なスキーマで、一連の結合から構成された行がある場合、結合のネストされたコンポーネントの1行の変更は、潜在的に何百万もの行を更新しなければならないかもしれません。

リアルタイムでこれを達成するのは現実的でないことが多く、大きな技術的努力が必要です。その理由は2つの課題によります:

1. テーブル行が変更されたときに適切な結合ステートメントをトリガーすることです。理想的には結合の全てのオブジェクトが更新される原因とならないようにする必要があります - 影響を受けたものだけです。高スループット下で正しい行にフィルタリングするために結合を修正し、この作業を実現するには外部ツールやエンジニアリングが必要です。
1. ClickHouseでの行の更新には慎重な管理が必要で、追加の複雑さが導入されます。

<br />

したがって、すべての非正規化されたオブジェクトを定期的に再ロードするバッチ更新プロセスがより一般的です。

## 非正規化の実際的なケース

非正規化が理にかなっている場合と、他の方法が望ましい場合の実際的な例をいくつか考えてみましょう。

既に`AnswerCount`や`CommentCount`などの統計が非正規化されている`Posts`テーブルを考えてみます。ソースデータがこの形で提供されています。実際、これは頻繁に変更される可能性が高いため、実際にはこの情報を正規化したいかもしれません。これらの多くのカラムも他のテーブルから利用可能です 例えば、投稿に対するコメントは`PostId`カラムと`Comments`テーブルを介して利用可能です。例示目的のため、投稿がバッチプロセスで再ロードされると仮定します。

また、分析のためのメインテーブルとして`Posts`に他のテーブルを非正規化するだけを考えます。非正規化は、同じ考慮事項が適用されるクエリのために他の方向性にも適しています。

*以下の各例では、どちらのテーブルも結合で使用されるクエリが存在すると想定します。*

### Posts と Votes

Votesは別のテーブルとして表現されています。この最適化されたスキーマは以下に示されており、データをロードするための挿入コマンドも示されています：

```sql
CREATE TABLE votes
(
	`Id` UInt32,
	`PostId` Int32,
	`VoteTypeId` UInt8,
	`CreationDate` DateTime64(3, 'UTC'),
	`UserId` Int32,
	`BountyAmount` UInt8
)
ENGINE = MergeTree
ORDER BY (VoteTypeId, CreationDate, PostId)

INSERT INTO votes SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes/*.parquet')

0 rows in set. Elapsed: 26.272 sec. Processed 238.98 million rows, 2.13 GB (9.10 million rows/s., 80.97 MB/s.)
```

一見、これらは投稿テーブルで非正規化するための候補かもしれません。しかし、このアプローチにはいくつかの課題があります。

投票は頻繁に投稿に追加されます。時間が経つにつれて投稿ごとに減少するかもしれませんが、以下のクエリは、3万件の投稿に対して時間あたり約4万件の投票があることを示しています。

```sql
SELECT round(avg(c)) AS avg_votes_per_hr, round(avg(posts)) AS avg_posts_per_hr
FROM
(
	SELECT
    	toStartOfHour(CreationDate) AS hr,
    	count() AS c,
    	uniq(PostId) AS posts
	FROM votes
	GROUP BY hr
)

┌─avg_votes_per_hr─┬─avg_posts_per_hr─┐
│         	41759 │        		33322 │
└──────────────────┴──────────────────┘
```

遅延が許容できる場合はバッチ処理で対策できますが、これでも更新を処理する必要があり、全ての投稿を定期的に再ロードする（望ましいとは限りません）必要があります。

さらに問題なのは、いくつかの投稿には非常に多くの投票があることです：

```sql
SELECT PostId, concat('https://stackoverflow.com/questions/', PostId) AS url, count() AS c
FROM votes
GROUP BY PostId
ORDER BY c DESC
LIMIT 5

┌───PostId─┬─url──────────────────────────────────────────┬─────c─┐
│ 11227902 │ https://stackoverflow.com/questions/11227902 │ 35123 │
│   927386 │ https://stackoverflow.com/questions/927386   │ 29090 │
│ 11227809 │ https://stackoverflow.com/questions/11227809 │ 27475 │
│   927358 │ https://stackoverflow.com/questions/927358   │ 26409 │
│  2003515 │ https://stackoverflow.com/questions/2003515  │ 25899 │
└──────────┴──────────────────────────────────────────────┴───────┘
```

ここでの主な観察点は、各投稿の集計された投票統計がほとんどの分析には十分であるということです - 全ての投票情報を非正規化する必要はありません。例えば、現在の`Score`カラムは例えば総アップ票数と総ダウン票数の差という統計を表しています。理想的には、シンプルなルックアップでクエリ時にこれらの統計を取得できる（[dictionaries](/ja/dictionary)参照）と良いでしょう。

### Users と Badges

次に`Users`と`Badges`を考えてみましょう：

<img src={require('./images/denormalization-schema.png').default}
  class='image'
  alt='UsersとBadgesのスキーマ'
  style={{width: '100%', background: 'none' }} />

<p></p>
まず以下のコマンドを使ってデータを挿入します：
<p></p>

```sql
CREATE TABLE users
(
    `Id` Int32,
    `Reputation` LowCardinality(String),
    `CreationDate` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `DisplayName` String,
    `LastAccessDate` DateTime64(3, 'UTC'),
    `AboutMe` String,
    `Views` UInt32,
    `UpVotes` UInt32,
    `DownVotes` UInt32,
    `WebsiteUrl` String,
    `Location` LowCardinality(String),
    `AccountId` Int32
)
ENGINE = MergeTree
ORDER BY (Id, CreationDate)
```

```sql
CREATE TABLE badges
(
    `Id` UInt32,
    `UserId` Int32,
    `Name` LowCardinality(String),
    `Date` DateTime64(3, 'UTC'),
    `Class` Enum8('Gold' = 1, 'Silver' = 2, 'Bronze' = 3),
    `TagBased` Bool
)
ENGINE = MergeTree
ORDER BY UserId

INSERT INTO users SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet')

0 rows in set. Elapsed: 26.229 sec. Processed 22.48 million rows, 1.36 GB (857.21 thousand rows/s., 51.99 MB/s.)

INSERT INTO badges SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/badges.parquet')

0 rows in set. Elapsed: 18.126 sec. Processed 51.29 million rows, 797.05 MB (2.83 million rows/s., 43.97 MB/s.)
```

ユーザーは頻繁にバッジを獲得する可能性がありますが、このデータセットを1日以上にわたって更新する必要はないかもしれません。バッジとユーザーの関係は一対多です。タプルのリストとしてユーザーにバッジを単純に非正規化できないでしょうか？ 可能ですが、以下のクエリから確認する、ユーザーごとのバッジの最高数が理想的でないことを示唆しています：

```sql
SELECT UserId, count() AS c FROM badges GROUP BY UserId ORDER BY c DESC LIMIT 5

┌─UserId─┬─────c─┐
│  22656 │ 19334 │
│   6309 │ 10516 │
│ 100297 │  7848 │
│ 157882 │  7574 │
│  29407 │  6512 │
└────────┴───────┘
```

19k以上のオブジェクトを単一行に非正規化するのは現実的ではないかもしれません。 この関係は別テーブルとして残すか、統計を追加するのが良いでしょう。

> バッジの統計をユーザーに対して非正規化したいかもしれません。 このデータセットでの挿入時にdictionariesを使用した例を考慮しています。

### Posts と PostLinks

`PostLinks`はユーザーが関連しているまたは重複していると考える`Posts`を接続します。 以下のクエリはスキーマとロードコマンドを示しています：

```sql
CREATE TABLE postlinks
(
  `Id` UInt64,
  `CreationDate` DateTime64(3, 'UTC'),
  `PostId` Int32,
  `RelatedPostId` Int32,
  `LinkTypeId` Enum('Linked' = 1, 'Duplicate' = 3)
)
ENGINE = MergeTree
ORDER BY (PostId, RelatedPostId)

INSERT INTO postlinks SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/postlinks.parquet')

0 rows in set. Elapsed: 4.726 sec. Processed 6.55 million rows, 129.70 MB (1.39 million rows/s., 27.44 MB/s.)
```

非正規化を防ぐ投稿の多くのリンクがないことを確認できます：

```sql
SELECT PostId, count() AS c
FROM postlinks
GROUP BY PostId
ORDER BY c DESC LIMIT 5

┌───PostId─┬───c─┐
│ 22937618 │ 125 │
│  9549780 │ 120 │
│  3737139 │ 109 │
│ 18050071 │ 103 │
│ 25889234 │  82 │
└──────────┴─────┘
```

同様に、これらのリンクは極端に頻繁に発生するイベントではありません：

```sql
SELECT
  round(avg(c)) AS avg_votes_per_hr,
  round(avg(posts)) AS avg_posts_per_hr
FROM
(
  SELECT
  toStartOfHour(CreationDate) AS hr,
  count() AS c,
  uniq(PostId) AS posts
  FROM postlinks
  GROUP BY hr
)

┌─avg_votes_per_hr─┬─avg_posts_per_hr─┐
│      		 54 │      		 44	│
└──────────────────┴──────────────────┘
```

これを以下の非正規化の例として使用します。

### シンプルな統計の例

ほとんどの場合、非正規化は親行に単一のカラムまたは統計を追加する必要があります。例えば、複製投稿の数で投稿を豊かにしたい場合は、単一のカラムを追加するだけです。

```sql
CREATE TABLE posts_with_duplicate_count
(
  `Id` Int32 CODEC(Delta(4), ZSTD(1)),
   ... -other columns
   `DuplicatePosts` UInt16
) ENGINE = MergeTree
ORDER BY (PostTypeId, toDate(CreationDate), CommentCount)
```

このテーブルを埋めるために、重複統計と投稿を結合する`INSERT INTO SELECT`を利用します。

```sql
INSERT INTO posts_with_duplicate_count SELECT
    posts.*,
    DuplicatePosts
FROM posts AS posts
LEFT JOIN
(
    SELECT PostId, countIf(LinkTypeId = 'Duplicate') AS DuplicatePosts
    FROM postlinks
    GROUP BY PostId
) AS postlinks ON posts.Id = postlinks.PostId
```

### 多対一の関係に対する複雑な型の活用

非正規化を行うためには、複雑な型を活用する必要があることが多いです。一対一の関係が非正規化され、カラムが少ない場合、ユーザーはこれらを元の型のまま行として追加できます。しかし、より大きなオブジェクトや一対多の関係でこれは望ましくないことがあります。

複雑なオブジェクトや一対多の関係の場合、ユーザーは以下を使用できます：

- 名前付きタプル - 複雑な構造を列の集合として表現します。
- Array(Tuple) または Nested - 名前付きタプルの配列、つまりNested、各エントリがオブジェクトを表現します。一対多の関係に適用されます。

例として、`PostLinks`を`Posts`に非正規化する過程を示します。

各投稿は他の投稿へのリンクを含むことができます。以前に示した`PostLinks`スキーマとして。ネストされた型として、リンクと重複した投稿を以下のように表現することができます：

```sql
SET flatten_nested=0
CREATE TABLE posts_with_links
(
  `Id` Int32 CODEC(Delta(4), ZSTD(1)),
   ... -other columns
   `LinkedPosts` Nested(CreationDate DateTime64(3, 'UTC'), PostId Int32),
   `DuplicatePosts` Nested(CreationDate DateTime64(3, 'UTC'), PostId Int32),
) ENGINE = MergeTree
ORDER BY (PostTypeId, toDate(CreationDate), CommentCount)
```

> ネストデータのフラット化を無効にする設定 `flatten_nested=0` の使用に注意してください。 ネストデータのフラット化を無効にすることを推奨しています。

`OUTER JOIN`クエリを使った`INSERT INTO SELECT`でこの非正規化を実行できます：

```sql
INSERT INTO posts_with_links
SELECT
    posts.*,
    arrayMap(p -> (p.1, p.2), arrayFilter(p -> p.3 = 'Linked' AND p.2 != 0, Related)) AS LinkedPosts,
    arrayMap(p -> (p.1, p.2), arrayFilter(p -> p.3 = 'Duplicate' AND p.2 != 0, Related)) AS DuplicatePosts
FROM posts
LEFT JOIN (
    SELECT
   	 PostId,
   	 groupArray((CreationDate, RelatedPostId, LinkTypeId)) AS Related
    FROM postlinks
    GROUP BY PostId
) AS postlinks ON posts.Id = postlinks.PostId

0 rows in set. Elapsed: 155.372 sec. Processed 66.37 million rows, 76.33 GB (427.18 thousand rows/s., 491.25 MB/s.)
Peak memory usage: 6.98 GiB.
```

> ここでの時間に注意してください。約2分で6600万行を非正規化することができました。後で見るように、これはスケジュールできる操作です。

`PostId`ごとに`PostLinks`を配列に圧縮するために`groupArray`関数を使用して結合前に行います。次にこの配列を、`LinkedPosts`と`DuplicatePosts`の2つのサブリストにフィルタリングし、外部結合からの空の結果を除外します。

新しい非正規化された構造を見るためにいくつかの行を選択できます：

```sql
SELECT LinkedPosts, DuplicatePosts
FROM posts_with_links
WHERE (length(LinkedPosts) > 2) AND (length(DuplicatePosts) > 0)
LIMIT 1
FORMAT Vertical

Row 1:
──────
LinkedPosts:	[('2017-04-11 11:53:09.583',3404508),('2017-04-11 11:49:07.680',3922739),('2017-04-11 11:48:33.353',33058004)]
DuplicatePosts: [('2017-04-11 12:18:37.260',3922739),('2017-04-11 12:18:37.260',33058004)]
```

## 非正規化のオーケストレーションとスケジューリング

### バッチ

非正規化を利用するには、それを実行し、オーケストレーションできる変換プロセスが必要です。

上記で示したように、データをロードした後に`INSERT INTO SELECT`を通じてこの変換を実行できます。これは定期的なバッチ変換に適しています。

ユーザーは、定期的なバッチロードプロセスが許容されるものとすると、ClickHouseでこれをオーケストレーションするためのいくつかのオプションを持っています：

- **外部ツール** - [dbt](https://www.getdbt.com/)や[Airflow](https://airflow.apache.org/)などのツールを利用して変換を定期的にスケジュールします。 [ClickHouseのdbt統合](/ja/integrations/dbt)は、ターゲットテーブルの新しいバージョンが作成され、その後原子に関連するバージョンと交換されることを保証します（[EXCHANGE](/ja/sql-reference/statements/exchange)コマンドを介して）。
- **[Refreshable Materialized Views（エクスペリメンタル）](/ja/materialized-view/refreshable-materialized-view)** - 更新可能なマテリアライズドビューは、クエリの結果がターゲットテーブルに送られるように定期的にスケジュールするために使用されます。クエリが実行されると、ビューはターゲットテーブルが原子に更新されることを保証します。これは、ClickHouseネイティブな方法でこの作業をスケジュールするものです。

### ストリーミング

ユーザーは、代わりにストリーミング技術、例えば[Apache Flink](https://flink.apache.org/)を使用して、ClickHouseの外部で挿入前にこれを実行することを希望するかもしれません。代わりに、インクリメンタルな[マテリアライズドビュー](/ja/guides/developer/cascading-materialized-views)を使用して、データが挿入されるとこのプロセスを実行することができます。
