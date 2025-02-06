---
title: BigQueryからClickHouse Cloudへの移行
slug: /ja/migrations/bigquery/migrating-to-clickhouse-cloud
description: BigQueryからClickHouse Cloudへのデータ移行方法
keywords: [migrate, migration, migrating, data, etl, elt, bigquery]
---

## なぜBigQueryよりもClickHouse Cloudを選ぶのか？

TL;DR: ClickHouseは、現代のデータ分析においてBigQueryよりも高速で、安価で、強力です。

<br />

<img src={require('../images/bigquery-2.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

## BigQueryからClickHouse Cloudへのデータロード

### データセット

BigQueryからClickHouse Cloudへの典型的な移行を示すためのサンプルデータセットとして、[こちら](/ja/getting-started/example-datasets/stackoverflow)で説明されているStack Overflowデータセットを使用します。これは、2008年から2024年4月までのStack Overflow上のすべての`post`、`vote`、`user`、`comment`、および`badge`を含んでいます。このデータのBigQueryスキーマは以下の通りです：

<br />

<img src={require('../images/bigquery-3.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

移行手順をテストするために、このデータセットをBigQueryインスタンスで構築したいユーザーのために、GCSバケットでParquet形式のデータを提供しており、BigQueryでのテーブル作成およびロード用のDDLコマンドは[こちら](https://pastila.nl/?003fd86b/2b93b1a2302cfee5ef79fd374e73f431#hVPC52YDsUfXg2eTLrBdbA==)にあります。

### データ移行

BigQueryとClickHouse Cloud間のデータ移行には、主に2つのワークロードタイプがあります：

- **初回の一括ロードと定期的な更新** - 初回データセットの移行が必要で、その後は一定間隔での定期更新（例えば、毎日）が必要です。ここでの更新は、変更された行を再送信することで処理され、比較に使用できるカラム（例えば、日付）またはXMIN値によって識別されます。削除は、データセットの完全な定期的なリロードによって処理されます。
- **リアルタイムレプリケーションまたはCDC** - 初回データセットの移行が必要です。このデータセットへの変更は、数秒の遅延のみが許容される状態で、ClickHouseにほぼリアルタイムで反映される必要があります。これは実質的に[Change Data Capture (CDC)プロセス](https://en.wikipedia.org/wiki/Change_data_capture)であり、BigQueryのテーブルはClickHouseと同期される必要があります、すなわちBigQueryテーブルでの挿入、更新および削除は、ClickHouse内の同等のテーブルに適用されます。

#### Google Cloud Storage (GCS)による一括ロード

BigQueryはGoogleのオブジェクトストア（GCS）へのデータエクスポートをサポートしています。我々のサンプルデータセットの場合：

1. 7つのテーブルをGCSにエクスポートします。そのためのコマンドは[こちら](https://pastila.nl/?014e1ae9/cb9b07d89e9bb2c56954102fd0c37abd#0Pzj52uPYeu1jG35nmMqRQ==)にあります。

2. データをClickHouse Cloudにインポートします。ここで、[gcsテーブル関数](/ja/sql-reference/table-functions/gcs)を使用します。DDLとインポートクエリは[こちら](https://pastila.nl/?00531abf/f055a61cc96b1ba1383d618721059976#Wf4Tn43D3VCU5Hx7tbf1Qw==)にあります。ClickHouse Cloudインスタンスが複数のコンピュートノードから構成されているため、`gcs`テーブル関数の代わりに、gcsバケットでも動作しデータを並列でロードする[すべてのノードを利用するs3Clusterテーブル関数](/ja/sql-reference/table-functions/s3Cluster)を使用しています。

<br />

<img src={require('../images/bigquery-4.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '600px'}} />

<br />

このアプローチには以下の利点があります：

- BigQueryエクスポート機能は、データサブセットのエクスポートをフィルタリングするためのサポートが付いています。
- BigQueryは[Parquet, Avro, JSON, CSV](https://cloud.google.com/bigquery/docs/exporting-data)形式およびいくつかの[圧縮タイプ](https://cloud.google.com/bigquery/docs/exporting-data)へのエクスポートをサポートしており、ClickHouseはこれらすべてをサポートしています。
- GCSは[オブジェクトライフサイクル管理](https://cloud.google.com/storage/docs/lifecycle)をサポートしており、指定した期間後にClickHouseにエクスポートおよびインポートされたデータを削除できます。
- [Googleは1日50TBまでGCSへのエクスポートを無料で許可しています](https://cloud.google.com/bigquery/quotas#export_jobs)。ユーザーはGCSストレージのみに支払いを行います。
- エクスポートは自動的に複数のファイルを生成し、テーブルデータを最大1GBに制限します。これにより、Importを並列化することができます。

以下の例を試す前に、ユーザーは[エクスポートに必要な権限](https://cloud.google.com/bigquery/docs/exporting-data#required_permissions)およびエクスポートとインポートのパフォーマンスを最大化するための[地域推薦](https://cloud.google.com/bigquery/docs/exporting-data#data-locations)をレビューすることをお勧めします。

### スケジュールされたクエリによるリアルタイムレプリケーションまたはCDC

Change Data Capture (CDC) は、2つのデータベース間でテーブルを同期させておくプロセスです。更新や削除をほぼリアルタイムで処理する場合、これは非常に複雑です。1つのアプローチは、BigQueryの[スケジュールされクエリ機能](https://cloud.google.com/bigquery/docs/scheduling-queries)を使って単純に定期的なエクスポートをスケジュールすることです。ClickHouseへのデータ挿入に若干の遅延を許容できるなら、このアプローチは実装が容易で維持が簡単です。[このブログ投稿](https://clickhouse.com/blog/clickhouse-bigquery-migrating-data-for-realtime-queries#using-scheduled-queries)では、一例が示されています。

## スキーマの設計

Stack Overflowデータセットには多数の関連テーブルが含まれています。まず主となるテーブルの移行に集中することをお勧めします。これが必ずしも最大のテーブルではなく、最も分析クエリを受けると予想されるテーブルかもしれません。これはClickHouseの主要な概念に精通するための手段となるでしょう。このテーブルは、ClickHouseの特性を最大限利用し、最適なパフォーマンスを得るためにさらなるテーブルを追加する際に再モデリングが必要になるかもしれません。このモデリングプロセスは[データモデリングドキュメント](/ja/data-modeling/schema-design#next-data-modelling-techniques)で探求しています。

この原則に従い、主な`posts`テーブルに焦点を当てます。このためのBigQueryスキーマは以下の通りです：

```sql
CREATE TABLE stackoverflow.posts (
    id INTEGER,
    posttypeid INTEGER,
    acceptedanswerid STRING,
    creationdate TIMESTAMP,
    score INTEGER,
    viewcount INTEGER,
    body STRING,
    owneruserid INTEGER,
    ownerdisplayname STRING,
    lasteditoruserid STRING,
    lasteditordisplayname STRING,
    lasteditdate TIMESTAMP,
    lastactivitydate TIMESTAMP,
    title STRING,
    tags STRING,
    answercount INTEGER,
    commentcount INTEGER,
    favoritecount INTEGER,
    conentlicense STRING,
    parentid STRING,
    communityowneddate TIMESTAMP,
    closeddate TIMESTAMP
);
```

### 型の最適化

このプロセスを適用すると、以下のスキーマが得られます：

```sql
CREATE TABLE stackoverflow.posts
(
   `Id` Int32,
   `PostTypeId` Enum('Question' = 1, 'Answer' = 2, 'Wiki' = 3, 'TagWikiExcerpt' = 4, 'TagWiki' = 5, 'ModeratorNomination' = 6, 'WikiPlaceholder' = 7, 'PrivilegeWiki' = 8),
   `AcceptedAnswerId` UInt32,
   `CreationDate` DateTime,
   `Score` Int32,
   `ViewCount` UInt32,
   `Body` String,
   `OwnerUserId` Int32,
   `OwnerDisplayName` String,
   `LastEditorUserId` Int32,
   `LastEditorDisplayName` String,
   `LastEditDate` DateTime,
   `LastActivityDate` DateTime,
   `Title` String,
   `Tags` String,
   `AnswerCount` UInt16,
   `CommentCount` UInt8,
   `FavoriteCount` UInt8,
   `ContentLicense` LowCardinality(String),
   `ParentId` String,
   `CommunityOwnedDate` DateTime,
   `ClosedDate` DateTime
)
ENGINE = MergeTree
ORDER BY tuple()
COMMENT 'Optimized types'
```

このテーブルには、GCSからエクスポートされたデータを[`INSERT INTO SELECT`](/ja/sql-reference/statements/insert-into)を使って簡単に投入できます。ClickHouse Cloud上では、`gcs`互換の[s3Clusterテーブル関数](/ja/sql-reference/table-functions/s3Cluster)を使って、複数ノード経由でのロードを並列化することも可能です：

```sql
INSERT INTO stackoverflow.posts SELECT * FROM gcs( 'gs://clickhouse-public-datasets/stackoverflow/parquet/posts/*.parquet', NOSIGN);
```

新しいスキーマにnullは保持されていません。上記のインサートは、これらをそれぞれのタイプに対するデフォルト値に自動的に変換します- 整数の場合は0、文字列の場合は空の値です。ClickHouseは、数値を自動的に対象精度に変換します。

## ClickHouseの主キーはどのように異なるのか？

[こちら](/ja/migrations/bigquery)で説明されているように、BigQueryと同様に、ClickHouseはテーブルの主キーのカラム値の一意性を強制しません。

BigQueryのクラスタリングと似ていて、ClickHouseテーブルのデータは主キーのカラムによりディスク上で並べ替えられて保存されます。このソート順序は、クエリオプティマイザによってソートを回避し、結合のメモリ使用を最小化し、リミット句のショートカットを可能にします。
BigQueryとは対照的に、ClickHouseは主キーのカラム値に基づいて[（スパースな）主インデックス](/ja/optimize/sparse-primary-indexes)を自動的に作成します。このインデックスは主キーのカラム上でフィルタがあるすべてのクエリを高速化するために利用されます。具体的には：

- メモリとディスクの効率性は、ClickHouseがしばしば使用されるスケールの重要な要素です。データは部分と呼ばれるチャンクに分かれてClickHouseテーブルに書き込まれ、バックグラウンドで結合のルールが適用されます。ClickHouseでは、各部分は自分自身の主インデックスを持ちます。部分が結合されるとき、結合した部分の主インデックスも結合されます。これらのインデックスは各行ごとに構築されていません。代わりに、部分の主インデックスは行のグループごとに1つのインデックスエントリを持ちます- これはスパースインデキシングと呼ばれる技術です。
- スパースインデキシングは、ClickHouseが指定されたキーによってディスク上の部分を並べて保存するため可能です。単一行を直接特定する（B-Treeベースのインデックスのように）のではなく、スパース主インデックスはクエリに一致する可能性のある行のグループをすばやくバイナリ検索によって特定します。一致する可能性のある行のグループは並列でClickHouseエンジンにストリームされ、マッチを見つけます。このインデックス設計により、主インデックスが小さく（主メモリに完全に収まる）なりつつ、特にデータ分析の使用例で典型的な範囲クエリの実行時間を大幅に短縮します。詳しい情報については、[この詳細ガイド](/ja/optimize/sparse-primary-indexes)をお勧めします。

<br />

<img src={require('../images/bigquery-5.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

選択されたClickHouseの主キーは、インデックスだけでなく、データのディスクへの書き込み順序も決定します。このため、これは圧縮レベルに大きな影響を与える可能性があります。そして、それはクエリパフォーマンスに影響を与えることができます。すべてのカラムは指定された順序キーの値に基づいてソートされます。例えば`CreationDate`がキーとして使われる場合、すべての他のカラムの値の順序は`CreationDate`カラムの値の順序に対応します。複数の順序キーを指定することができます - これは`SELECT`クエリの`ORDER BY`句と同じセマンティクスで順序付けされます。

### 順序キーの選択

順序キーの選択における考慮事項とステップについて、例としてpostsテーブルを使用した場合は[こちら](/ja/data-modeling/schema-design#choosing-an-ordering-key)を参照してください。

## データモデリング技術

BigQueryから移行するユーザーには、[ClickHouseでのデータモデリングガイド](/ja/data-modeling/schema-design)を読むことをお勧めします。このガイドは同じStack Overflowデータセットを使用し、ClickHouseの機能を使用した複数のアプローチを探ります。

### パーティション

BigQueryのユーザーは、テーブルを小さく管理しやすいパーツ（パーティション）に分割することで、大規模なデータベースのパフォーマンスと管理を向上させる方法であるテーブルパーティションの概念に親しんでいるでしょう。このパーティションは、指定されたカラムの範囲（例：日付）、定義されたリスト、またはキーに基づくハッシュによって達成されます。これにより、管理者は特定の基準（例：日付範囲や地理的な位置）に基づいてデータを整理できます。

パーティション化は、パーティションプルーニングを通じて迅速なデータアクセスと効率的なインデックス作成を実現し、クエリ性能を向上させます。また、バックアップやデータ削除などの保守タスクを個別のパーティション上で実行できるようにし、テーブル全体に対して操作せずに済むようにします。それにより、パーティション化は複数のパーティション間で負荷を分散することで、BigQueryデータベースのスケーラビリティを大幅に向上させます。

ClickHouseでは、パーティション化は最初に表を定義する際に[`PARTITION BY`](/ja/engines/table-engines/mergetree-family/custom-partitioning-key)句によって指定されます。この句は、任意のカラムのSQL式を含むことができ、この結果によって行がどのパーティションに送られるかが定義されます。

<br />

<img src={require('../images/bigquery-6.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

データパーツはディスク上で各パーティションに論理的に関連付き、単独でクエリを実行できます。以下の例では、[`toYear(CreationDate)`](/ja/sql-reference/functions/date-time-functions#toyear)を使用して年ごとに投稿テーブルをパーティション化しています。行がClickHouseに挿入されるとき、この式は各行に対して評価され、クエリ中に結果パーティションにルーティングされます。

```sql
CREATE TABLE posts
(
	`Id` Int32 CODEC(Delta(4), ZSTD(1)),
	`PostTypeId` Enum8('Question' = 1, 'Answer' = 2, 'Wiki' = 3, 'TagWikiExcerpt' = 4, 'TagWiki' = 5, 'ModeratorNomination' = 6, 'WikiPlaceholder' = 7, 'PrivilegeWiki' = 8),
	`AcceptedAnswerId` UInt32,
	`CreationDate` DateTime64(3, 'UTC'),
...
	`ClosedDate` DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (PostTypeId, toDate(CreationDate), CreationDate)
PARTITION BY toYear(CreationDate)
```

#### アプリケーション

ClickHouseのパーティション化は、BigQueryにおける同様のアプリケーションを持っていますが、いくつかの微妙な違いがあります。具体的には：

- **データ管理** - ClickHouseでは、ユーザーは主にパーティションをデータ管理機能と見なすべきです。データを特定のキーに基づいて論理的に分割することで、各パーティションに個別に操作を加えられるようになります。これにより、ユーザーはパーティションを権利により迅速に移動したり、[ストレージティアにデータを動かしたり](/ja/integrations/s3#storage-tiers)するなどの効率的な操作が可能になります。例として、古いポストを削除する場合：

```sql
SELECT DISTINCT partition
FROM system.parts
WHERE `table` = 'posts'

┌─partition─┐
│ 2008  	│
│ 2009  	│
│ 2010  	│
│ 2011  	│
│ 2012  	│
│ 2013  	│
│ 2014  	│
│ 2015  	│
│ 2016  	│
│ 2017  	│
│ 2018  	│
│ 2019  	│
│ 2020  	│
│ 2021  	│
│ 2022  	│
│ 2023  	│
│ 2024  	│
└───────────┘

17 rows in set. Elapsed: 0.002 sec.
	
	ALTER TABLE posts
	(DROP PARTITION '2008')

Ok.

0 rows in set. Elapsed: 0.103 sec.
```

- **クエリ最適化** - パーティションはクエリ性能に役立つ可能性がありますが、これがどの程度の利点をもたらすかはアクセスパターンに大きく依存します。クエリが少数のパーティション（理想的にはひとつ）に限定されている場合、パフォーマンスが向上する可能性があります。これは、通常、主キーにないパーティションキーでフィルタリングする場合に役立ちます。しかし、多くのパーティションをカバーするクエリは、パーティションが使用されない場合よりもクエリ性能が低下する可能性があります（パーティションによって可能な部分が増加するため）。パーティションを利用して`GROUP BY`クエリを最適化することができますが、これは各パーティション内の値がユニークである場合のみ効果的です。しかし、一般的には主キーが最適化されていることを確認し、アクセスパターンが特定の予測可能なサブセットに絞られている特別な場合にのみクエリ最適化技法としてパーティション化を検討すべきです、例えば日付でパーティション化し、最近の日付のクエリが多い場合に有用です。

#### 推奨事項

パーティション化はデータ管理技術であると考えるべきです。これは、時系列データの操作時、例えば最も古いパーティションを[単に削除する](/ja/sql-reference/statements/alter/partition#alter_drop-partition)といった方法でクラスタからデータを削除する必要がある場合に理想的です。

重要：パーティションキーの式が高いカーディナリティのセットを生成しないようにしてください、すなわち100以上のパーティションの作成は避けるべきです。例えば、クライアント識別子や名前といった高カーディナリティカラムでデータをパーティション化しないでください。代わりに、クライアント識別子や名前を`ORDER BY`式の最初のカラムにします。

> 内部的に、ClickHouseは挿入されたデータの[部分を作成](/ja/optimize/sparse-primary-indexes#clickhouse-index-design)します。より多くのデータが挿入されると、部分の数が増加します。部分の数が指定された制限を超えた場合、ClickHouseは["too many parts"エラー](/docs/knowledgebase/exception-too-many-parts)として設定されたルールに従って挿入時に例外を投げます。これは通常の操作下では発生しません。ClickHouseが不適切に設定されている場合や間違って使用されている場合（例：多くの小さい挿入）だけです。部分はパーティションごとに分離して作成されるため、パーティション数を増やすことは部分の数も増加させます。高カーディナリティのパーティションキーはこのエラーを引き起こす可能性があるので避けるべきです。

## Materialized ViewとProjectionの違い

ClickHouseのプロジェクションの概念により、ユーザーはテーブルの複数の`ORDER BY`句を指定できます。

[ClickHouseのデータモデリング](/ja/data-modeling/schema-design)では、ClickHouseにおいて集計を事前計算するためにMaterialized Viewを使用し、行を変換し、異なるアクセスパターンのクエリを最適化する方法について探ります。後者については、Materialized Viewが行を異なる順序のターゲットテーブルに送信する場合の[例を提供しました](/ja/materialized-view#lookup-table)。

例えば、次のクエリを考えてみてください：

```sql
SELECT avg(Score)
FROM comments
WHERE UserId = 8592047

   ┌──────────avg(Score)─┐
   │ 0.18181818181818182  │
   └────────────────────┘

1 row in set. Elapsed: 0.040 sec. Processed 90.38 million rows, 361.59 MB (2.25 billion rows/s., 9.01 GB/s.)
Peak memory usage: 201.93 MiB.
```

このクエリは`UserId`が順序キーではないため、すべての90m行をスキャンする必要があります。以前は、リッチアップ用のMaterialized Viewを使って`PostId`のLOOKUPを提供しました。プロジェクションを用いても同じ問題を解決できます。以下のコマンドは`ORDER BY user_id`のプロジェクションを追加します。

```sql
ALTER TABLE comments ADD PROJECTION comments_user_id (
SELECT * ORDER BY UserId
)

ALTER TABLE comments MATERIALIZE PROJECTION comments_user_id
```

最初にプロジェクションを作成し、その後にマテリアライズしなければなりません。この後者のコマンドはデータを2回異なる順序でディスクに保存させます。プロジェクションは、以下に示すようにデータが作成されるときに定義することもでき、挿入されるデータに応じて自動的に保守されます。

```sql
CREATE TABLE comments
(
	`Id` UInt32,
	`PostId` UInt32,
	`Score` UInt16,
	`Text` String,
	`CreationDate` DateTime64(3, 'UTC'),
	`UserId` Int32,
	`UserDisplayName` LowCardinality(String),
	PROJECTION comments_user_id
	(
    	SELECT *
    	ORDER BY UserId
	)
)
ENGINE = MergeTree
ORDER BY PostId
```

プロジェクションが`ALTER`コマンドを介して作成される場合、`MATERIALIZE PROJECTION`コマンドが発行されたときに作成は非同期です。ユーザーは次のクエリを使ってこの操作の進捗を確認し、`is_done=1`を待つことができます。

```sql
SELECT
	parts_to_do,
	is_done,
	latest_fail_reason
FROM system.mutations
WHERE (`table` = 'comments') AND (command LIKE '%MATERIALIZE%')

   ┌─parts_to_do─┬─is_done─┬─latest_fail_reason─┐
1. │       	1 │   	0 │                	│
   └─────────────┴─────────┴────────────────────┘

1 row in set. Elapsed: 0.003 sec.
```

このクエリを繰り返すと、追加のストレージを使用する代わりにパフォーマンスが大幅に向上していることが確認できます。

```sql
SELECT avg(Score)
FROM comments
WHERE UserId = 8592047

   ┌──────────avg(Score)─┐
1. │ 0.18181818181818182 │
   └────────────────────┘

1 row in set. Elapsed: 0.008 sec. Processed 16.36 thousand rows, 98.17 KB (2.15 million rows/s., 12.92 MB/s.)
Peak memory usage: 4.06 MiB.
```

[`EXPLAIN`コマンド](/ja/sql-reference/statements/explain)を使用して、このクエリにプロジェクションが使用されたことを確認することもできます：

```sql
EXPLAIN indexes = 1
SELECT avg(Score)
FROM comments
WHERE UserId = 8592047

	┌─explain─────────────────────────────────────────────┐
 1. │ Expression ((Projection + Before ORDER BY))     	│
 2. │   Aggregating                                   	│
 3. │ 	Filter                                      	│
 4. │   	ReadFromMergeTree (comments_user_id)      	│
 5. │   	Indexes:                                  	│
 6. │     	PrimaryKey                              	│
 7. │       	Keys:                                 	│
 8. │         	UserId                              	│
 9. │       	Condition: (UserId in [8592047, 8592047]) │
10. │       	Parts: 2/2                            	│
11. │       	Granules: 2/11360                     	│
	└─────────────────────────────────────────────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

### プロジェクションの使用時期

プロジェクションは自動的に保守されるため、新しいユーザーには魅力的です。さらに、一つのテーブルに単にクエリを送信し、プロジェクションによって可能な場合に応じて応答時間を短縮できます。

<br />

<img src={require('../images/bigquery-7.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

これは、ユーザーが適切な最適化済ターゲットテーブルを選ぶか、フィルタに応じてクエリを書き換える必要があるMaterialized Viewとは対照的です。これにより、ユーザーアプリケーションにより大きな重点が置かれ、クライアント側の複雑さが増します。

これらの利点にもかかわらず、プロジェクションにはいくつかの制約があり、ユーザーはこれを認識し、慎重に展開するべきです：

- プロジェクションは、元のテーブルと異なる有効期限（TTL）を使用することはできません。Materialized viewで異なるTTLを使用することができます。
- プロジェクションは、（サードテーブル）において現在[`optimize_read_in_order`](https://clickhouse.com/blog/clickhouse-faster-queries-with-projections-and-primary-indexes)をサポートしていません。
- プロジェクションを持つテーブルには論理更新や削除がサポートされていません。
- Materialized viewはチェーン可能：あるMaterialized viewのターゲットテーブルが別のMaterialized viewのソーステーブルになることができ、さらに続けることができます。プロジェクションではこれは不可能です。
- プロジェクションはジョインをサポートしていませんが、Materialized viewはサポートしています。
- プロジェクションはフィルタ（`WHERE`句）をサポートしていませんが、Materialized viewはサポートしています。

プロジェクションを使うことをお勧めする場合：

- データの完全な並べ替えが必要です。プロジェクション内の式は理論的には`GROUP BY`を用いることができますが、Materialized viewの方が集計を保守するにはより効果的です。クエリオプティマイザも単純な並べ替えを使用するプロジェクションの方が`SELECT * ORDER BY x`によるものとして利用しやすくなっています。ユーザーはストレージフットプリントを削減するために、この式で一部のカラムを選択することができます。
- データを2回書き込むことによるストレージフットプリントの拡大、およびオーバーヘッドに慣れている。挿入速度への影響をテストし、[ストレージオーバーヘッドを評価](/ja/data-compression/compression-in-clickhouse)します。

## BigQueryクエリのClickHouseへの書き換え

以下は、BigQueryとClickHouseを比較するクエリの例を提供します。このリストは、ClickHouseの機能を利用してクエリを大幅に簡素化する方法を示しています。以下の例は、Stack Overflowデータセット（2024年4月まで）を使用しています。

**10個以上の質問を持つユーザーで最も多くビューを得たもの：**

_BigQuery_

<img src={require('../images/bigquery-8.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '500px'}} />

<br />

_ClickHouse_

```sql
SELECT
    OwnerDisplayName,
    sum(ViewCount) AS total_views
FROM stackoverflow.posts
WHERE (PostTypeId = 'Question') AND (OwnerDisplayName != '')
GROUP BY OwnerDisplayName
HAVING count() > 10
ORDER BY total_views DESC
LIMIT 5

   ┌─OwnerDisplayName─┬─total_views─┐
1. │ Joan Venge       │    25520387 │
2. │ Ray Vega         │    21576470 │
3. │ anon             │    19814224 │
4. │ Tim              │    19028260 │
5. │ John             │    17638812 │
   └──────────────────┴─────────────┘

5 rows in set. Elapsed: 0.076 sec. Processed 24.35 million rows, 140.21 MB (320.82 million rows/s., 1.85 GB/s.)
Peak memory usage: 323.37 MiB.
```

**最も多くのビューを受けたタグ：**

_BigQuery_

<br />

<img src={require('../images/bigquery-9.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '400px'}} />

<br />

_ClickHouse_

```sql
-- ClickHouse
SELECT
    arrayJoin(arrayFilter(t -> (t != ''), splitByChar('|', Tags))) AS tags,
    sum(ViewCount) AS views
FROM stackoverflow.posts
GROUP BY tags
ORDER BY views DESC
LIMIT 5


   ┌─tags───────┬──────views─┐
1. │ javascript │ 8190916894 │
2. │ python     │ 8175132834 │
3. │ java       │ 7258379211 │
4. │ c#         │ 5476932513 │
5. │ android    │ 4258320338 │
   └────────────┴────────────┘

5 rows in set. Elapsed: 0.318 sec. Processed 59.82 million rows, 1.45 GB (188.01 million rows/s., 4.54 GB/s.)
Peak memory usage: 567.41 MiB.
```

## 集計関数

可能な限り、ユーザーはClickHouseの集計関数を利用すべきです。以下では、各年で最もビューの多い質問を計算するために[`argMax`関数](/ja/sql-reference/aggregate-functions/reference/argmax)を使用する例を示しています。

_BigQuery_

<br />

<img src={require('../images/bigquery-10.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '500px'}} />

<br />

<img src={require('../images/bigquery-11.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '500px'}} />

<br />

_ClickHouse_

```sql
-- ClickHouse
SELECT
    toYear(CreationDate) AS Year,
    argMax(Title, ViewCount) AS MostViewedQuestionTitle,
    max(ViewCount) AS MaxViewCount
FROM stackoverflow.posts
WHERE PostTypeId = 'Question'
GROUP BY Year
ORDER BY Year ASC
FORMAT Vertical


Row 1:
──────
Year:                    2008
MostViewedQuestionTitle: How to find the index for a given item in a list?
MaxViewCount:            6316987

Row 2:
──────
Year:                    2009
MostViewedQuestionTitle: How do I undo the most recent local commits in Git?
MaxViewCount:            13962748

…

Row 16:
───────
Year:                    2023
MostViewedQuestionTitle: How do I solve "error: externally-managed-environment" every time I use pip 3?
MaxViewCount:            506822

Row 17:
───────
Year:                    2024
MostViewedQuestionTitle: Warning "Third-party cookie will be blocked. Learn more in the Issues tab"
MaxViewCount:            66975

17 rows in set. Elapsed: 0.225 sec. Processed 24.35 million rows, 1.86 GB (107.99 million rows/s., 8.26 GB/s.)
Peak memory usage: 377.26 MiB.
```

## 条件式と配列

条件式と配列関数はクエリを大幅に簡素化します。次のクエリは、2022年から2023年にかけて最も割合増加したタグ（10000件以上の出現数があるもの）を計算します。次のClickHouseクエリは、条件式、配列関数、および`HAVING`や`SELECT`句でのエイリアス再利用の能力により簡潔です。

_BigQuery_

<br />

<img src={require('../images/bigquery-12.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '500px'}} />

<br />

_ClickHouse_

```sql
SELECT
    arrayJoin(arrayFilter(t -> (t != ''), splitByChar('|', Tags))) AS tag,
    countIf(toYear(CreationDate) = 2023) AS count_2023,
    countIf(toYear(CreationDate) = 2022) AS count_2022,
    ((count_2023 - count_2022) / count_2022) * 100 AS percent_change
FROM stackoverflow.posts
WHERE toYear(CreationDate) IN (2022, 2023)
GROUP BY tag
HAVING (count_2022 > 10000) AND (count_2023 > 10000)
ORDER BY percent_change DESC
LIMIT 5

┌─tag─────────┬─count_2023─┬─count_2022─┬──────percent_change─┐
│ next.js   │   13788 │     10520 │   31.06463878326996 │
│ spring-boot │     16573 │     17721 │  -6.478189718413183 │
│ .net      │   11458 │     12968 │ -11.644046884639112 │
│ azure     │   11996 │     14049 │ -14.613139725247349 │
│ docker    │   13885 │     16877 │  -17.72826924216389 │
└─────────────┴────────────┴────────────┴─────────────────────┘

5 rows in set. Elapsed: 0.096 sec. Processed 5.08 million rows, 155.73 MB (53.10 million rows/s., 1.63 GB/s.)
Peak memory usage: 410.37 MiB.
```

これで、BigQueryからClickHouseへのユーザー移行用の基本ガイドが終了します。BigQueryからの移行を行うユーザーには、[ClickHouseでのデータモデリングガイド](/ja/data-modeling/schema-design)を読み、高度なClickHouseの機能を学ぶことをお勧めします。
```
