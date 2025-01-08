---
slug: /ja/guides/replacing-merge-tree
title: ReplacingMergeTree
description: ClickHouseでのReplacingMergeTreeエンジンの使用
keywords: [replacingmergetree, 挿入, 重複排除]
---

トランザクションデータベースはトランザクションの更新や削除のワークロードに最適化されていますが、OLAPデータベースはそのような操作の保証を減少させ、高速な分析クエリを実現するために不変データをバッチで挿入することに最適化されています。ClickHouseは、突然変異を通じて更新操作を提供し、行を軽量に削除する手段も提供していますが、その列指向の構造により、これらの操作は注意深くスケジュールされるべきです。これらの操作は非同期で処理され、単一のスレッドで行われ、（更新の場合は）データがディスクに書き直される必要があります。そのため、少数の小規模な変更には使用すべきではありません。

上記の使用パターンを避けつつ更新と削除の行を処理するために、ClickHouseのテーブルエンジンReplaceingMergeTreeを使用できます。

## 挿入された行の自動アップサート

[ReplacingMergeTreeテーブルエンジン](/ja/engines/table-engines/mergetree-family/replacingmergetree)は、非効率的な`ALTER`や`DELETE`文を使用せずに行に対して更新操作を適用することを可能にし、ユーザーが同じ行の複数のコピーを挿入し、最新バージョンを指定できるようにします。このプロセスはバックグラウンドで非同期に古いバージョンの行を削除し、不変の挿入を使用して効率的に更新操作を模倣します。
これは、テーブルエンジンが重複行を識別する能力に依存しています。これは、`ORDER BY`句を使用して一意性を決定することによって達成されます。つまり、`ORDER BY`で指定されたカラムの値が同じである場合、2行は重複と見なされます。テーブルを定義する際に指定される`バージョン`カラムは、重複として識別された2行の場合、より高いバージョンの値を持つ行が保持されることを可能にします。

以下の例でこのプロセスを示します。ここでは、行はAカラム（テーブルの`ORDER BY`）によって一意に識別されます。これらの行は2つのバッチとして挿入されたと仮定し、ディスク上に2つのデータパーツを形成しています。後で、非同期のバックグラウンドプロセス中に、これらのパーツは一緒にマージされます。

ReplacingMergeTreeはさらに、削除されたカラムを指定することができます。これは0または1を含み、値が1である場合、行（およびその重複）が削除されたことを示し、それ以外はゼロとして使用されます。**注：削除された行はマージ時に削除されません。**

このプロセス中に、パーツのマージ中に次のことが行われます：

- カラムAの値1で識別された行は、バージョン2の更新行と、バージョン3の削除行（および削除カラムの値1）を持っています。削除された最新の行が保持されます。
- カラムAの値2で識別された行は2つの更新行を持っており、後者の行が価格カラムの値6で保持されます。
- カラムAの値3で識別された行は、バージョン1の行とバージョン2の削除行があります。この削除行が保持されます。

このマージプロセスの結果として、最終状態を表す4行が得られます。

<br />

<img src={require('../images/postgres-replacingmergetree.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px', background: 'none'}} />

<br />

削除された行は決して削除されないことに注意してください。これらは`OPTIMIZE table FINAL CLEANUP`で強制的に削除することができます。これにはエクスペリメンタルな設定`allow_experimental_replacing_merge_with_cleanup=1`が必要です。これは次の条件下でのみ発行されるべきです：

1. クリーンアップで削除された行に対して古いバージョンの行が操作発行後に挿入されないことを確認できる場合。これらが挿入された場合、削除された行がもはや存在しないため、誤って保持されます。
2. クリーンアップを発行する前にすべてのレプリカが同期されていることを確認します。これは次のコマンドで達成できます：

<br />

```sql
SYSTEM SYNC REPLICA table
```

このコマンドと後続のクリーンアップが完了するまで、挿入を一時停止することをお勧めします。

> ReplacingMergeTreeで削除を扱うことは、クリーンアップを上記の条件でスケジュールできる期間を除き、削除数が少から中程度（10％未満）のテーブルにのみ推奨されます。

> ヒント: ユーザーは、変更がない選択的なパーティションに対して`OPTIMIZE FINAL CLEANUP`を発行することもできます。

## 主キー/重複排除キーの選択

上記で説明したように、ReplacingMergeTreeの場合には満たされるべき重要な追加の制約があります：`ORDER BY`カラムの値が変更をまたいで行を一意に識別しなければならないことです。Postgresのようなトランザクションデータベースから移行する場合、オリジナルのPostgres主キーはClickHouseの`ORDER BY`句に含める必要があります。

ClickHouseのユーザーは、自分のテーブルの`ORDER BY`句でカラムを選択することに[クエリパフォーマンスを最適化する](/ja/data-modeling/schema-design#choosing-an-ordering-key)ことに慣れています。一般的に、これらのカラムは、[頻繁なクエリに基づいて選択され、増加するカーディナリティに応じてリストされるべきです](/ja/optimize/sparse-primary-indexes)。重要なポイントとして、ReplacingMergeTreeは追加の制約を課します - これらのカラムは不変でなければならず、つまりPostgresからのレプリケーションの場合、基になるPostgresデータで変更されないカラムのみをこの句に追加する必要があります。他のカラムは変更できるが、これらは一意の行識別のために一貫している必要があります。

分析ワークロードではポストグレスの主キーは一般的にあまり役立たないことがあります、なぜならユーザーはまれにポイント行のルックアップを行うからです。カラムは増加するカーディナリティの順に並べられるべきと述べた通り、および[ORDER BYでリストされているカラムのマッチングが通常より高速である](/ja/optimize/sparse-primary-indexes#secondary-key-columns-can-not-be-inefficient)ため、Postgres主キーは`ORDER BY`の最後に追加されるべきです（分析的価値がある場合を除きます）。Postgresで複数のカラムが主キーを形成する場合は、カーディナリティとクエリ値の可能性を考慮して、`ORDER BY`に追加されるべきです。また、ユーザーは`MATERIALIZED`カラムを通じて値の連結を使用して一意の主キーを生成することを考慮するかもしれません。

Stack Overflowデータセットからのポストテーブルを考えてみましょう。

```sql
CREATE TABLE stackoverflow.posts_updateable
(
       `Version` UInt32,
       `Deleted` UInt8,
	`Id` Int32 CODEC(Delta(4), ZSTD(1)),
	`PostTypeId` Enum8('Question' = 1, 'Answer' = 2, 'Wiki' = 3, 'TagWikiExcerpt' = 4, 'TagWiki' = 5, 'ModeratorNomination' = 6, 'WikiPlaceholder' = 7, 'PrivilegeWiki' = 8),
	`AcceptedAnswerId` UInt32,
	`CreationDate` DateTime64(3, 'UTC'),
	`Score` Int32,
	`ViewCount` UInt32 CODEC(Delta(4), ZSTD(1)),
	`Body` String,
	`OwnerUserId` Int32,
	`OwnerDisplayName` String,
	`LastEditorUserId` Int32,
	`LastEditorDisplayName` String,
	`LastEditDate` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
	`LastActivityDate` DateTime64(3, 'UTC'),
	`Title` String,
	`Tags` String,
	`AnswerCount` UInt16 CODEC(Delta(2), ZSTD(1)),
	`CommentCount` UInt8,
	`FavoriteCount` UInt8,
	`ContentLicense` LowCardinality(String),
	`ParentId` String,
	`CommunityOwnedDate` DateTime64(3, 'UTC'),
	`ClosedDate` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(Version, Deleted)
PARTITION BY toYear(CreationDate)
ORDER BY (PostTypeId, toDate(CreationDate), CreationDate, Id)
```

`ORDER BY`キーとして`(PostTypeId, toDate(CreationDate), CreationDate, Id)`を使用しています。各投稿に一意の`Id`カラムは、行が重複排除されることを保証します。スキーマに必要に応じて`Version`および`Deleted`カラムを追加します。

## ReplacingMergeTreeのクエリ

マージ時に、ReplacingMergeTreeは`ORDER BY`カラムの値を一意の識別子として使用して重複行を識別し、最高のバージョンを保持するか、最新バージョンが削除を示す場合はすべての重複を削除します。ただし、これは最終的な正確性のみを提供し、行が重複排除される保証はありません。また、それに依存するべきではありません。そのため、クエリは更新および削除行がクエリで考慮されるため、誤った回答を生成する可能性があります。

正確な回答を得るためには、ユーザーはクエリタイムでの重複排除および削除削除とバックグラウンドのマージを補完する必要があります。これは`FINAL`オペレーターを使用して実現できます。

上記の投稿テーブルを考えてみましょう。このデータセットをロードする通常の方法を使用できますが、削除されたカラムとバージョンカラムを指定し、値を0に加えます。例として、10000行のみをロードします。

```sql
INSERT INTO stackoverflow.posts_updateable SELECT 0 AS Version, 0 AS Deleted, *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/*.parquet') WHERE AnswerCount > 0 LIMIT 10000

0 rows in set. Elapsed: 1.980 sec. Processed 8.19 thousand rows, 3.52 MB (4.14 thousand rows/s., 1.78 MB/s.)
```

行数を確認しましょう：

```sql
SELECT count() FROM stackoverflow.posts_updateable

┌─count()─┐
│   10000 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

投稿回答の統計を更新します。これらの値を更新するのではなく、5000行の新しいコピーを挿入し、そのバージョン番号を1つ追加します（これにより、テーブル内に150行の行が存在します）。 `INSERT INTO SELECT`を使用してこれをシミュレートできます：

```sql
INSERT INTO posts_updateable SELECT
	Version + 1 AS Version,
	Deleted,
	Id,
	PostTypeId,
	AcceptedAnswerId,
	CreationDate,
	Score,
	ViewCount,
	Body,
	OwnerUserId,
	OwnerDisplayName,
	LastEditorUserId,
	LastEditorDisplayName,
	LastEditDate,
	LastActivityDate,
	Title,
	Tags,
	AnswerCount,
	CommentCount,
	FavoriteCount,
	ContentLicense,
	ParentId,
	CommunityOwnedDate,
	ClosedDate
FROM posts_updateable --select 100 random rows
WHERE (Id % toInt32(floor(randUniform(1, 11)))) = 0
LIMIT 5000

0 rows in set. Elapsed: 4.056 sec. Processed 1.42 million rows, 2.20 GB (349.63 thousand rows/s., 543.39 MB/s.)
```

さらに、削除カラムの値1で行を再挿入することにより、1000個のランダムな投稿を削除します。これもシンプルな`INSERT INTO SELECT`でシミュレートできます。

```sql
INSERT INTO posts_updateable SELECT
	Version + 1 AS Version,
	1 AS Deleted,
	Id,
	PostTypeId,
	AcceptedAnswerId,
	CreationDate,
	Score,
	ViewCount,
	Body,
	OwnerUserId,
	OwnerDisplayName,
	LastEditorUserId,
	LastEditorDisplayName,
	LastEditDate,
	LastActivityDate,
	Title,
	Tags,
	AnswerCount + 1 AS AnswerCount,
	CommentCount,
	FavoriteCount,
	ContentLicense,
	ParentId,
	CommunityOwnedDate,
	ClosedDate
FROM posts_updateable --select 100 random rows
WHERE (Id % toInt32(floor(randUniform(1, 11)))) = 0 AND AnswerCount > 0
LIMIT 1000

0 rows in set. Elapsed: 0.166 sec. Processed 135.53 thousand rows, 212.65 MB (816.30 thousand rows/s., 1.28 GB/s.)
```

上記の操作の結果として16,000行を持つことになります。つまり、10,000 + 5000 + 1000です。ここでの正しい合計は、実際には元の合計から1000行少ないはずです。つまり、10,000 - 1000 = 9000です。

```sql
SELECT count()
FROM posts_updateable

┌─count()─┐
│   10000 │
└─────────┘
1 row in set. Elapsed: 0.002 sec.
```

ここでの結果は、発生したマージによって異なる場合があります。テーブルに`FINAL`を適用すると、正しい結果が得られます。

```sql
SELECT count()
FROM posts_updateable
FINAL

┌─count()─┐
│	9000 │
└─────────┘

1 row in set. Elapsed: 0.006 sec. Processed 11.81 thousand rows, 212.54 KB (2.14 million rows/s., 38.61 MB/s.) 
Peak memory usage: 8.14 MiB.
```

## FINALのパフォーマンス

`FINAL`オペレーターはクエリにパフォーマンスのオーバーヘッドを生じます。これに対する進行中の改善にもかかわらず、特にプライマリキーのカラムにフィルタリングしないクエリの場合に顕著です。これにより、より多くのデータが読み込まれ、重複排除のオーバーヘッドが増加します。ユーザーが`WHERE`条件でキーのカラムをフィルタリングする場合、読み込まれるデータが減少し、重複排除の対象となる部分も減少します。

`WHERE`条件にキーのカラムが使用されていない場合、ClickHouseは現在のところ`FINAL`を使用して`PREWHERE`最適化を活用していません。この最適化は、フィルタリングされていないカラムの読み取る行を減少させることを目的としています。`PREWHERE`を模倣し、潜在的にパフォーマンスを向上させる方法の例は、[こちら](https://clickhouse.com/blog/clickhouse-postgresql-change-data-capture-cdc-part-1#final-performance)をご覧ください。

## ReplacingMergeTreeでのパーティションの活用

ClickHouseでのデータのマージは、パーティションレベルで行われます。ReplacingMergeTreeを使用する場合、ユーザーは最善のパーティションの実践に従ってテーブルを分散させることをお勧めします。ただし、特に**行に対してこのパーティショニングキーが変更されないことを確認できる場合に限ります**。これにより、同じ行に関連する更新が同じClickHouseのパーティションに送信されることが保証されます。Postgresの同じパーティションキーを再利用してもよいが、ここで概説されたベストプラクティスに従うことを条件とします。

この場合、ユーザーは`do_not_merge_across_partitions_select_final=1`設定を使用して`FINAL`クエリのパフォーマンスを向上させることができます。この設定により、`FINAL`を使用しているときにパーティションが個別にマージされて処理されるようになります。

パーティショニングを使用しない投稿テーブルを考えてみましょう：

```sql
CREATE TABLE stackoverflow.posts_no_part
(
	`Version` UInt32,
	`Deleted` UInt8,
	`Id` Int32 CODEC(Delta(4), ZSTD(1)),
	…
)
ENGINE = ReplacingMergeTree
ORDER BY (PostTypeId, toDate(CreationDate), CreationDate, Id)

INSERT INTO stackoverflow.posts_no_part SELECT 0 AS Version, 0 AS Deleted, *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/*.parquet')

0 rows in set. Elapsed: 182.895 sec. Processed 59.82 million rows, 38.07 GB (327.07 thousand rows/s., 208.17 MB/s.)
```

`FINAL`がいくつかの作業を行うことを必要とするために、1mの行を更新し、その`AnswerCount`を重複行を挿入してインクリメントします。

```sql
INSERT INTO posts_no_part SELECT Version + 1 AS Version, Deleted, Id, PostTypeId, AcceptedAnswerId, CreationDate, Score, ViewCount, Body, OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, Title, Tags, AnswerCount + 1 AS AnswerCount, CommentCount, FavoriteCount, ContentLicense, ParentId, CommunityOwnedDate, ClosedDate
FROM posts_no_part
LIMIT 1000000
```

`FINAL`で年ごとの回答の合計を計算する：

```sql
SELECT toYear(CreationDate) AS year, sum(AnswerCount) AS total_answers
FROM posts_no_part
FINAL
GROUP BY year
ORDER BY year ASC

┌─year─┬─total_answers─┐
│ 2008 │    	371480 │
…
│ 2024 │    	127765 │
└──────┴───────────────┘

17 rows in set. Elapsed: 2.338 sec. Processed 122.94 million rows, 1.84 GB (52.57 million rows/s., 788.58 MB/s.)
Peak memory usage: 2.09 GiB.
```

パーティションを年ごとに区切るテーブルに対して同じ手順を繰り返し、`do_not_merge_across_partitions_select_final=1`を使用して上記のクエリを再度実行します。

```sql
CREATE TABLE stackoverflow.posts_with_part
(
	`Version` UInt32,
	`Deleted` UInt8,
	`Id` Int32 CODEC(Delta(4), ZSTD(1)),
	...
)
ENGINE = ReplacingMergeTree
PARTITION BY toYear(CreationDate)
ORDER BY (PostTypeId, toDate(CreationDate), CreationDate, Id)

// populate & update omitted

SELECT toYear(CreationDate) AS year, sum(AnswerCount) AS total_answers
FROM posts_with_part
FINAL
GROUP BY year
ORDER BY year ASC

┌─year─┬─total_answers─┐
│ 2008 │    	387832 │
│ 2009 │   	1165506 │
│ 2010 │   	1755437 │
...
│ 2023 │    	787032 │
│ 2024 │    	127765 │
└──────┴───────────────┘

17 rows in set. Elapsed: 0.994 sec. Processed 64.65 million rows, 983.64 MB (65.02 million rows/s., 989.23 MB/s.)
```

示されているように、パーティショニングにより、この場合、重複排除プロセスがパーティションレベルで並行して行われることが可能になり、クエリパフォーマンスが大幅に向上しました。

## 大きなパートのマージ動作

ClickHouseのReplacingMergeTreeエンジンは、データパーツをマージして重複行を管理し、指定された一意のキーに基づいて各行の最新バージョンのみを保持するように最適化されています。しかし、マージされたパートが[`max_bytes_to_merge_at_max_space_in_pool`](/docs/ja/operations/settings/merge-tree-settings#max-bytes-to-merge-at-max-space-in-pool)のしきい値に達すると、さらにマージされる対象として選択されなくなります。たとえ[`min_age_to_force_merge_seconds`](/docs/ja/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds)が設定されていたとしてもです。その結果、自動マージに頼って進行中のデータ挿入で累積する重複を削除することができなくなります。

この問題に対処するために、ユーザーは`OPTIMIZE FINAL`を呼び出して手動でパーツをマージし、重複を削除することができます。自動的なマージとは異なり、`OPTIMIZE FINAL`は`max_bytes_to_merge_at_max_space_in_pool`しきい値を無視し、利用可能なリソース、特にディスクスペースに基づいて、各パーティションに単一のパートが残るまでパーツをマージします。ただし、このアプローチは大きなテーブルでメモリを集中的に使用し、新しいデータが追加されると再度実行する必要があるかもしれません。

パフォーマンスを維持しつつ、より持続可能な解決策として、テーブルをパーティショニングすることをお勧めします。これにより、データパーツが最大マージサイズに達するのを防ぎ、継続的な手動の最適化の必要性が減少します。
