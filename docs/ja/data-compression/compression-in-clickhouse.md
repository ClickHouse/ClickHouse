---
slug: /ja/data-compression/compression-in-clickhouse
title: ClickHouseにおける圧縮
description: ClickHouseの圧縮アルゴリズムの選択
keywords: [圧縮, コーデック, エンコーディング]
---

ClickHouseのクエリパフォーマンスの秘密の一つは圧縮です。

ディスク上のデータが少なければ、I/Oが減少し、クエリや挿入が速くなります。通常、CPUに関連する任意の圧縮アルゴリズムのオーバーヘッドは、I/Oの削減によって上回ります。そのため、ClickHouseのクエリを高速化するためには、データの圧縮を改善することに最初に注力するべきです。

> ClickHouseがデータをうまく圧縮する理由については、[この記事](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)をお勧めします。要約すると、列指向データベースとして、値はカラム順に書き込まれます。これらの値がソートされている場合、同じ値が互いに隣接します。圧縮アルゴリズムは連続するデータパターンを利用します。さらに、ClickHouseにはコーデックや細かなデータ型が存在し、ユーザーが圧縮技術をさらに調整できるようになっています。

ClickHouseにおける圧縮は主に以下の3つの要因に影響されます：
- 順序キー
- データ型
- 使用されるコーデック

これらはすべてスキーマを通じて設定されます。

## 圧縮を最適化するために適切なデータ型を選択

Stack Overflowのデータセットを例にしましょう。以下の`posts`テーブルのスキーマについての圧縮統計を比較します：

- `posts` - 順序キーのないタイプ最適化がされていないスキーマ。
- `posts_v3` - 各カラムに適切な型とビットサイズを持ち、順序キー`(PostTypeId, toDate(CreationDate), CommentCount)`を備えたタイプ最適化スキーマ。

以下のクエリを使用して、各カラムの現在の圧縮済みおよび未圧縮のサイズを測定できます。最初の`posts`の順序キーなしでの最適化スキーマのサイズを調べてみましょう。

```sql
SELECT name,
   formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
   formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
   round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.columns
WHERE table = 'posts'
GROUP BY name

┌─name──────────────────┬─compressed_size─┬─uncompressed_size─┬───ratio─┐
│ Body              	│ 46.14 GiB   	│ 127.31 GiB    	│	2.76 │
│ Title             	│ 1.20 GiB    	│ 2.63 GiB      	│	2.19 │
│ Score             	│ 84.77 MiB   	│ 736.45 MiB    	│	8.69 │
│ Tags              	│ 475.56 MiB  	│ 1.40 GiB      	│	3.02 │
│ ParentId          	│ 210.91 MiB  	│ 696.20 MiB    	│ 	3.3 │
│ Id                	│ 111.17 MiB  	│ 736.45 MiB    	│	6.62 │
│ AcceptedAnswerId  	│ 81.55 MiB   	│ 736.45 MiB    	│	9.03 │
│ ClosedDate        	│ 13.99 MiB   	│ 517.82 MiB    	│   37.02 │
│ LastActivityDate  	│ 489.84 MiB  	│ 964.64 MiB    	│	1.97 │
│ CommentCount      	│ 37.62 MiB   	│ 565.30 MiB    	│   15.03 │
│ OwnerUserId       	│ 368.98 MiB  	│ 736.45 MiB    	│   	2 │
│ AnswerCount       	│ 21.82 MiB   	│ 622.35 MiB    	│   28.53 │
│ FavoriteCount     	│ 280.95 KiB  	│ 508.40 MiB    	│ 1853.02 │
│ ViewCount         	│ 95.77 MiB   	│ 736.45 MiB    	│	7.69 │
│ LastEditorUserId  	│ 179.47 MiB  	│ 736.45 MiB    	│ 	4.1 │
│ ContentLicense    	│ 5.45 MiB    	│ 847.92 MiB    	│   155.5 │
│ OwnerDisplayName  	│ 14.30 MiB   	│ 142.58 MiB    	│	9.97 │
│ PostTypeId        	│ 20.93 MiB   	│ 565.30 MiB    	│  	27 │
│ CreationDate      	│ 314.17 MiB  	│ 964.64 MiB    	│	3.07 │
│ LastEditDate      	│ 346.32 MiB  	│ 964.64 MiB    	│	2.79 │
│ LastEditorDisplayName │ 5.46 MiB    	│ 124.25 MiB    	│   22.75 │
│ CommunityOwnedDate	│ 2.21 MiB    	│ 509.60 MiB    	│  230.94 │
└───────────────────────┴─────────────────┴───────────────────┴─────────┘
```

ここでは圧縮済みサイズと未圧縮サイズの両方を表示しています。どちらも重要です。圧縮済みサイズはディスクから読み取る必要があるものを指し、これはクエリのパフォーマンス（およびストレージコスト）を最小化するために減らしたいものです。このデータは読み取る前に解凍する必要があります。この未圧縮サイズはデータ型に依存します。このサイズを最小化することで、クエリのメモリオーバーヘッドを減少させ、クエリで処理する必要があるデータを削減し、キャッシュの利用効率を向上させ、最終的にクエリ時間を短縮します。

> 上記のクエリは、システムデータベースの`columns`テーブルに依存しています。このデータベースはClickHouseによって管理され、クエリのパフォーマンスメトリクスからバックグラウンドクラスターログに至るまで、さまざまな有用な情報の宝庫です。好奇心旺盛な読者には、["System Tables and a Window into the Internals of ClickHouse"](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)や、関連する記事[[1]](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)[[2]](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)をお勧めします。

テーブル全体のサイズを要約するには、以下のクエリを簡素化できます：

```sql
SELECT formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.columns
WHERE table = 'posts'

┌─compressed_size─┬─uncompressed_size─┬─ratio─┐
│ 50.16 GiB   	│ 143.47 GiB    	│  2.86 │
└─────────────────┴───────────────────┴───────┘
```

最適化された型と順序キーを持つ`posts_v3`テーブルに対してこのクエリを繰り返すことで、未圧縮サイズと圧縮済みサイズが大幅に削減されていることがわかります。

```sql
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.columns
WHERE `table` = 'posts_v3'

┌─compressed_size─┬─uncompressed_size─┬─ratio─┐
│ 25.15 GiB   	│ 68.87 GiB     	│  2.74 │
└─────────────────┴───────────────────┴───────┘
```

カラム別の詳細な分析では、データを圧縮前に順序化し、適切な型を使用することで、`Body`、`Title`、`Tags`、`CreationDate`カラムで大きな削減が達成されています。

```sql
SELECT
    name,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.columns
WHERE `table` = 'posts_v3'
GROUP BY name

┌─name──────────────────┬─compressed_size─┬─uncompressed_size─┬───ratio─┐
│ Body                  │ 23.10 GiB       │ 63.63 GiB         │    2.75 │
│ Title                 │ 614.65 MiB      │ 1.28 GiB          │    2.14 │
│ Score                 │ 40.28 MiB       │ 227.38 MiB        │    5.65 │
│ Tags                  │ 234.05 MiB      │ 688.49 MiB        │    2.94 │
│ ParentId              │ 107.78 MiB      │ 321.33 MiB        │    2.98 │
│ Id                    │ 159.70 MiB      │ 227.38 MiB        │    1.42 │
│ AcceptedAnswerId      │ 40.34 MiB       │ 227.38 MiB        │    5.64 │
│ ClosedDate            │ 5.93 MiB        │ 9.49 MiB          │     1.6 │
│ LastActivityDate      │ 246.55 MiB      │ 454.76 MiB        │    1.84 │
│ CommentCount          │ 635.78 KiB      │ 56.84 MiB         │   91.55 │
│ OwnerUserId           │ 183.86 MiB      │ 227.38 MiB        │    1.24 │
│ AnswerCount           │ 9.67 MiB        │ 113.69 MiB        │   11.76 │
│ FavoriteCount         │ 19.77 KiB       │ 147.32 KiB        │    7.45 │
│ ViewCount             │ 45.04 MiB       │ 227.38 MiB        │    5.05 │
│ LastEditorUserId      │ 86.25 MiB       │ 227.38 MiB        │    2.64 │
│ ContentLicense        │ 2.17 MiB        │ 57.10 MiB         │   26.37 │
│ OwnerDisplayName      │ 5.95 MiB        │ 16.19 MiB         │    2.72 │
│ PostTypeId            │ 39.49 KiB       │ 56.84 MiB         │ 1474.01 │
│ CreationDate          │ 181.23 MiB      │ 454.76 MiB        │    2.51 │
│ LastEditDate          │ 134.07 MiB      │ 454.76 MiB        │    3.39 │
│ LastEditorDisplayName │ 2.15 MiB        │ 6.25 MiB          │    2.91 │
│ CommunityOwnedDate    │ 824.60 KiB      │ 1.34 MiB          │    1.66 │
└───────────────────────┴─────────────────┴───────────────────┴─────────┘
```

## 適切なカラムの圧縮コーデックの選択

カラム圧縮コーデックを使用すると、各カラムをエンコードおよび圧縮するために使用されるアルゴリズム（およびその設定）を変更できます。

エンコーディングと圧縮は若干異なる仕組みで動作しますが、目的は同じです：データサイズを削減することです。エンコーディングは私たちのデータにマッピングを適用し、データ型の特性を利用した関数に基づいて値を変換します。逆に、圧縮はバイトレベルでデータを圧縮する一般的なアルゴリズムを使用します。

通常、エンコーディングは圧縮が使用される前に適用されます。異なるエンコーディングと圧縮アルゴリズムは、異なる値の分布に対して有効なので、データを理解する必要があります。

ClickHouseは多くのコーデックと圧縮アルゴリズムをサポートしています。以下は重要度の順にいくつかの推奨事項です：

- **`ZSTD`を基本とする** - `ZSTD`圧縮は最高の圧縮率を提供します。`ZSTD(1)`は、最も一般的な型のデフォルトとして設定されるべきです。圧縮率を高めるために数値を変更することが試みられますが、挿入速度が遅くなるコストを考慮すると、3以上の値を使用することはほとんどないでしょう。
- **整数や日付のシーケンスには`Delta`を** - `Delta`ベースのコーデックは、単調なシーケンスや連続する値の間で小さなデルタがある場合にうまく機能します。より具体的には、`Delta`コーデックは、微分が小さな数値を生成する場合によく機能します。そうでない場合は、`DoubleDelta`を試す価値があります（これは通常、`Delta`の最初のレベルの導関数がすでに非常に小さい場合にはさほど影響を与えません）。単調な増加が一定のシーケンス、例えば日付時間フィールドはさらによく圧縮されます。
- **`Delta`は`ZSTD`を改善する** - `ZSTD`はデルタデータにおいて効果的なコーデックです。逆に、デルタエンコーディングは`ZSTD`の圧縮を改善することができます。`ZSTD`が存在する場合、他のコーデックはさらなる改善をほとんど提供しません。
- **可能であれば`ZSTD`よりも`LZ4`を** - `LZ4`と`ZSTD`が同等の圧縮を提供する場合、後者のほうが高速な解凍を提供し、より少ないCPUを必要とするため、`LZ4`を選ぶほうが良いでしょう。しかし、ほとんどの場合`ZSTD`は`LZ4`を大幅に上回る性能を発揮します。これらのコーデックの一部は、`LZ4`と組み合わせることで、`ZSTD`に匹敵する圧縮を提供しながら高速に動作する場合がありますが、これにはテストが必要です。
- **スパースデータや小範囲には`T64`を** - `T64`はスパースデータやブロック内の範囲が小さい場合に効果的です。ランダムな数値には`T64`を避けるべきです。
- **未知のパターンには`Gorilla`と`T64`を試す** - データが未知のパターンを持つ場合、`Gorilla`と`T64`を試す価値があるかもしれません。
- **ゲージデータには`Gorilla`を** - `Gorilla`は浮動小数点データ、特にゲージ読み取りを表すデータ、すなわちランダムなスパイクに効果的です。

さらに詳しいオプションは[こちら](/ja/sql-reference/statements/create/table#column_compression_codec)を参照してください。

以下では、`Id`、`ViewCount`、`AnswerCount`に対して`Delta`コーデックを指定します。これらが順序キーと線形に相関していると仮定し、Deltaエンコードから恩恵を受けるはずです。

```sql
CREATE TABLE posts_v4
(
	`Id` Int32 CODEC(Delta, ZSTD),
	`PostTypeId` Enum('Question' = 1, 'Answer' = 2, 'Wiki' = 3, 'TagWikiExcerpt' = 4, 'TagWiki' = 5, 'ModeratorNomination' = 6, 'WikiPlaceholder' = 7, 'PrivilegeWiki' = 8),
	`AcceptedAnswerId` UInt32,
	`CreationDate` DateTime64(3, 'UTC'),
	`Score` Int32,
	`ViewCount` UInt32 CODEC(Delta, ZSTD),
	`Body` String,
	`OwnerUserId` Int32,
	`OwnerDisplayName` String,
	`LastEditorUserId` Int32,
	`LastEditorDisplayName` String,
	`LastEditDate` DateTime64(3, 'UTC'),
	`LastActivityDate` DateTime64(3, 'UTC'),
	`Title` String,
	`Tags` String,
	`AnswerCount` UInt16 CODEC(Delta, ZSTD),
	`CommentCount` UInt8,
	`FavoriteCount` UInt8,
	`ContentLicense` LowCardinality(String),
	`ParentId` String,
	`CommunityOwnedDate` DateTime64(3, 'UTC'),
	`ClosedDate` DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (PostTypeId, toDate(CreationDate), CommentCount)
```

これらのカラムの圧縮の改善は以下の通りです：

```sql
SELECT
    `table`,
    name,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.columns
WHERE (name IN ('Id', 'ViewCount', 'AnswerCount')) AND (`table` IN ('posts_v3', 'posts_v4'))
GROUP BY
    `table`,
    name
ORDER BY
    name ASC,
    `table` ASC

┌─table────┬─name────────┬─compressed_size─┬─uncompressed_size─┬─ratio─┐
│ posts_v3 │ AnswerCount │ 9.67 MiB    	│ 113.69 MiB    	│ 11.76 │
│ posts_v4 │ AnswerCount │ 10.39 MiB   	│ 111.31 MiB    	│ 10.71 │
│ posts_v3 │ Id      	│ 159.70 MiB  	│ 227.38 MiB    	│  1.42 │
│ posts_v4 │ Id      	│ 64.91 MiB   	│ 222.63 MiB    	│  3.43 │
│ posts_v3 │ ViewCount   │ 45.04 MiB   	│ 227.38 MiB    	│  5.05 │
│ posts_v4 │ ViewCount   │ 52.72 MiB   	│ 222.63 MiB    	│  4.22 │
└──────────┴─────────────┴─────────────────┴───────────────────┴───────┘

6 rows in set. Elapsed: 0.008 sec
```

### ClickHouse Cloudにおける圧縮

ClickHouse Cloudでは、デフォルトで`ZSTD`圧縮アルゴリズム（デフォルト値は1）を利用しています。このアルゴリズムの圧縮速度は圧縮レベルによって異なりますが（圧縮レベルが高いほど遅くなる）、解凍が一貫して高速（約20％のばらつき）であり、並列化の恩恵を受ける能力を持っているという利点を持っています。過去のテストでは、このアルゴリズムが非常に効果的であり、`LZ4`とコーデックの組み合わせよりも優れている場合が多いことが示されています。ほとんどのデータ型と情報の分布に対して効果的であるため、一般目的のデフォルトとして理にかなっており、最初の圧縮が最適化されていなくてもすでに優れている理由です。
