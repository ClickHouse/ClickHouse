---
slug: /ja/data-modeling/schema-design
title: スキーマ設計
description: ClickHouse スキーマのクエリパフォーマンス最適化
keywords: [スキーマ, スキーマ設計, クエリ最適化]
---

効果的なスキーマ設計を理解することは、ClickHouse のパフォーマンスを最適化する鍵であり、これは多くの場合トレードオフを伴う選択肢を含みます。最適なアプローチは提供されるクエリやデータの更新頻度、レイテンシ要件、データ量などの要因に依存します。このガイドでは、ClickHouse のパフォーマンスを最適化するためのスキーマ設計のベストプラクティスとデータモデリング技法の概要を提供します。

## Stack Overflow データセット

このガイドの例では、Stack Overflow データセットのサブセットを使用します。これは、2008年から2024年4月まで、Stack Overflow で発生したすべての投稿、投票、ユーザー、コメント、バッジを含んでいます。このデータは、以下のスキーマを使用して Parquet フォーマットで S3 バケット `s3://datasets-documentation/stackoverflow/parquet/` にて公開されています:

> 指定された主キーとリレーションシップは制約を通じて強制されているものではなく (Parquet はファイルフォーマットでありテーブルフォーマットではないため)、データがどのように関係しており、どのようなユニークキーを持つかを単に示しています。

<img src={require('./images/stackoverflow-schema.png').default}    
  class='image'
  alt='Stack Overflow スキーマ'
  style={{width: '800px', background: 'none'}} />

<br />

Stack Overflow データセットには、いくつかの関連テーブルが含まれています。いかなるデータモデリングタスクにおいても、ユーザーはまず主テーブルのロードに集中することをお勧めします。それは必ずしも最大のテーブルである必要はありませんが、むしろあなたが最も分析クエリを受け取ると予想しているテーブルです。これにより、特に OLTP 背景から移行する場合、ClickHouse の主な概念と型に慣れることができます。このテーブルは、ClickHouse の機能を完全に活用し最適なパフォーマンスを得るために、追加のテーブルが追加されるにつれて再モデリングが必要になるかもしれません。

上記のスキーマは、このガイドの目的のために意図的に最適化されていません。

## 初期スキーマの確立

`posts` テーブルはほとんどの分析クエリのターゲットとなるため、このテーブルのスキーマを確立することに焦点を当てます。このデータは S3 のパブリックバケット `s3://datasets-documentation/stackoverflow/parquet/posts/*.parquet` にて年毎のファイルで提供されています。

> Parquet フォーマットでの S3 からデータをロードすることは、最も一般的で好ましい ClickHouse にデータをロードする方法です。ClickHouse は Parquet の処理に最適化されており、S3 から毎秒何千万もの行を読み込み、挿入することも可能です。

ClickHouse は、データセットの型を自動的に識別するスキーマ推論機能を提供しています。これは、Parquet を含むすべてのデータフォーマットでサポートされています。この機能を活用して、s3 テーブル関数および[`DESCRIBE`](/ja/sql-reference/statements/describe-table) コマンドを通じて ClickHouse の型を識別できます。以下では、パターン `*.parquet` を使用して `stackoverflow/parquet/posts` フォルダ内のすべてのファイルを読み取っています。

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/*.parquet')
SETTINGS describe_compact_output = 1

┌─name──────────────────┬─type───────────────────────────┐
│ Id                	│ Nullable(Int64)            	│
│ PostTypeId        	│ Nullable(Int64)            	│
│ AcceptedAnswerId  	│ Nullable(Int64)            	│
│ CreationDate      	│ Nullable(DateTime64(3, 'UTC')) │
│ Score             	│ Nullable(Int64)            	│
│ ViewCount         	│ Nullable(Int64)            	│
│ Body              	│ Nullable(String)           	│
│ OwnerUserId       	│ Nullable(Int64)            	│
│ OwnerDisplayName  	│ Nullable(String)           	│
│ LastEditorUserId  	│ Nullable(Int64)            	│
│ LastEditorDisplayName │ Nullable(String)           	│
│ LastEditDate      	│ Nullable(DateTime64(3, 'UTC')) │
│ LastActivityDate  	│ Nullable(DateTime64(3, 'UTC')) │
│ Title             	│ Nullable(String)           	│
│ Tags              	│ Nullable(String)           	│
│ AnswerCount       	│ Nullable(Int64)            	│
│ CommentCount      	│ Nullable(Int64)            	│
│ FavoriteCount     	│ Nullable(Int64)            	│
│ ContentLicense    	│ Nullable(String)           	│
│ ParentId          	│ Nullable(String)           	│
│ CommunityOwnedDate	│ Nullable(DateTime64(3, 'UTC')) │
│ ClosedDate        	│ Nullable(DateTime64(3, 'UTC')) │
└───────────────────────┴────────────────────────────────┘
```

> [s3 テーブル関数](/ja/sql-reference/table-functions/s3)は、ClickHouse から S3 内のデータをそのままクエリすることを可能にします。この関数は、ClickHouse がサポートするすべてのファイルフォーマットと互換性があります。

これにより、最初の非最適化スキーマが提供されます。デフォルトでは、ClickHouse はこれらを同等の Nullable 型にマッピングします。この型を使用して、簡単な `CREATE EMPTY AS SELECT` コマンドで ClickHouse テーブルを作成できます。

```sql
CREATE TABLE posts
ENGINE = MergeTree
ORDER BY () EMPTY AS
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/*.parquet')
```

いくつかの重要な点:

このコマンドを実行した後、私たちの posts テーブルは空です。データはロードされていません。  
MergeTree をテーブルエンジンとして指定しています。MergeTree はおそらくあなたが最も使用する ClickHouse テーブルエンジンです。これは、鉅大なデータの処理が可能な ClickHouse ボックス内の万能ツールであり、ほとんどの分析ユースケースに対応します。他のテーブルエンジンは、効率的な更新をサポートする必要がある CDC などのユースケースに存在します。

句 `ORDER BY ()` はインデックスがなく、より具体的にはデータに順序がないことを意味します。これについては後ほど詳細に説明しますが、今はすべてのクエリが線形スキャンを必要とすることを知っておいてください。

テーブルが作成されたことを確認するには:

```sql
SHOW CREATE TABLE posts

CREATE TABLE posts
(
	`Id` Nullable(Int64),
	`PostTypeId` Nullable(Int64),
	`AcceptedAnswerId` Nullable(Int64),
	`CreationDate` Nullable(DateTime64(3, 'UTC')),
	`Score` Nullable(Int64),
	`ViewCount` Nullable(Int64),
	`Body` Nullable(String),
	`OwnerUserId` Nullable(Int64),
	`OwnerDisplayName` Nullable(String),
	`LastEditorUserId` Nullable(Int64),
	`LastEditorDisplayName` Nullable(String),
	`LastEditDate` Nullable(DateTime64(3, 'UTC')),
	`LastActivityDate` Nullable(DateTime64(3, 'UTC')),
	`Title` Nullable(String),
	`Tags` Nullable(String),
	`AnswerCount` Nullable(Int64),
	`CommentCount` Nullable(Int64),
	`FavoriteCount` Nullable(Int64),
	`ContentLicense` Nullable(String),
	`ParentId` Nullable(String),
	`CommunityOwnedDate` Nullable(DateTime64(3, 'UTC')),
	`ClosedDate` Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY tuple()
```

初期スキーマが定義されたら、s3 テーブル関数を使用してデータを読み取り、`INSERT INTO SELECT` を使用してデータを投入することができます。以下の例では、8コアの ClickHouse Cloud インスタンスで約2分で `posts` データをロードします。

```sql
INSERT INTO posts SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/*.parquet')

0 rows in set. Elapsed: 148.140 sec. Processed 59.82 million rows, 38.07 GB (403.80 thousand rows/s., 257.00 MB/s.)
```

> 上記のクエリは 60m 行をロードします。ClickHouse にとって小規模のデータですが、インターネット接続が遅いユーザーはデータのサブセットをロードしたい場合があるでしょう。これはパターン e.g. `https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2008.parquet` または `https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/{2008, 2009}.parquet` を指定することで実現できます。パターンを使ってターゲットファイルのサブセットを指定する方法については[こちら](/ja/sql-reference/table-functions/file#globs-in-path)を参照してください。

## 型の最適化

ClickHouse クエリパフォーマンスの秘密のひとつは圧縮です。

ディスク上のデータが少ないほど、I/O が減り、クエリや挿入が高速になります。CPU に関しては、いかなる圧縮アルゴリズムのオーバーヘッドも I/O の削減によってほとんどの場合上回るでしょう。そのため、ClickHouse クエリが速いことを保証する際には、最初にデータの圧縮を向上することを第一の課題にすべきです。

> ClickHouse がデータを非常に効率よく圧縮する理由については、[こちらの記事](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema) をお勧めします。要約すると、列指向データベースであるため、値はカラム順に書き込まれます。これらの値が並べ替えられると、同一の値が隣接して書き込まれ、圧縮アルゴリズムは連続したデータパターンを利用します。これに加えて、ClickHouse にはコーデックや精密なデータ型があり、ユーザーが圧縮技術をさらに調整することが可能です。

ClickHouse での圧縮は、3つの主要な要因によって影響を受けます：オーダリングキー、データ型、および使用するコーデック。これらはすべてスキーマを通じて設定されます。

最初の大きな圧縮とクエリパフォーマンスの改善は、単純な型最適化のプロセスを通じて得られます。いくつかの簡単なルールを適用することでスキーマを最適化できます:

- **厳密な型を使用する** - 我々の初期スキーマは多くのカラムで文字列型を使用していますが、これらは明らかに数値です。そのため、適切な型を使用することで、フィルタリングや集計時に期待される意味論を確保できます。同じことが、Parquet ファイルで正しく提供されている日付型にも当てはまります。
- **Nullable カラムの回避** - 上記のカラムはデフォルトで Null と仮定されています。Nullable 型はクエリが空値と Null 値を区別することを可能にしますが、これにより UInt8 型の別のカラムが作成されます。この追加のカラムは、ユーザーが Nullable カラムを操作するたびに処理される必要があり、追加のストレージが使用され、ほぼ常にクエリパフォーマンスに悪影響を与えます。型のデフォルトの空の値と Null との違いがある場合に限り、Nullable を使用してください。例えば、`ViewCount` カラムの空の値に対して 0 を扱うことは、ほとんどのクエリで問題なく影響を与えない可能性があります。空の値が異なる扱いを必要とする場合、フィルタリングでしばしばクエリから除外することができます。
- **数値型の最小精度を使用する** - ClickHouse には、異なる数値範囲と精度に合わせた数多くの数値型があります。常にカラムを表すのに使用されるビット数を最小化することを心がけてください。例えば、より小さなビット数の整数など e.g. Int16 に加え、負でないバリアントをサポートし、最小値が 0 の無符号型が存在します。これにより、カラムに使用するビット数を減らすことができます。可能であれば、これらの型を大きな無符号バリアントより優先させるべきです。
- **日付型の最小精度** - ClickHouse は、いくつかの日付および日時型をサポートしています。Date と Date32 は純粋な日付の保存に使用でき、後者はより大きな日付範囲をサポートしますが、より多くのビットを使用します。DateTime および DateTime64 は日時のサポートを提供します。DateTime は秒単位の精度に制限されており、32 ビットを使用します。DateTime64 はその名の通り 64 ビットを使用しますがナノ秒単位までの精度を提供します。常にクエリに必要な最も粗いバージョンを選び、必要なビット数を最小化してください。
- **LowCardinality の使用** - 低数のユニーク値を持つ数値、文字列、DateまたはDateTimeカラムは、LowCardinality 型を使用してエンコードできる可能性があります。これは値をディクショナリエンコードしてディスク上のサイズを減少させます。1万未満のユニーク値を持つカラムに対してこれを考慮してください。
固定長文字列を特別なケースで使用 - 固定長の文字列は FixedString 型でエンコードすることができます、例えば言語や通貨コード。この種のエンコーディングはデータが正確に N バイトの長さを持つ時に効率的です。その他の場合においては、効率を低下させる可能性があり、LowCardinality が推奨されます。
- **データ検証に対する列挙型の利用** - Enum 型は列挙型を効率的にエンコードするために使用することができます。Enum はユニークな値を格納する必要に応じて 8 ビットまたは 16 ビットになり得ます。この型の利用を検討する際には、登録されていない値を挿入時に拒否する関連検証を必要とする場合や列挙された値に自然な順序があり、クエリのパフォーマンスを向上させる場合などを考慮してください。例として、ユーザーのフィードバックカラムが `Enum(':(' = 1, ':|' = 2, ':)' = 3)` を含む場合が挙げられます。 

> ヒント: すべてのカラムの範囲およびユニーク値の数を見つけるには、ユーザーは簡単なクエリ `SELECT * APPLY min, * APPLY  max, * APPLY uniq FROM table FORMAT Vertical` を使用できます。これはデータの小さなサブセットに対しての実行をお勧めします、というのもこのクエリは高負荷になる可能性があるためです。このクエリが数値を正確に特定するためには、少なくとも定義が必要です。これは文字列でないことを意味します。

これらの単純なルールを posts テーブルに適用することにより、各カラムの最適な型を特定することができます：

<img src={require('./images/schema-design-types.png').default}    
  class='image'
  alt='Stack Overflow スキーマ'
  style={{width: '1000px'}} />

<br />

上記を元に以下のスキーマが示されます：

```sql
CREATE TABLE posts_v2
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
   `ContentLicense`LowCardinality(String),
   `ParentId` String,
   `CommunityOwnedDate` DateTime,
   `ClosedDate` DateTime
)
ENGINE = MergeTree
ORDER BY tuple()
COMMENT 'Optimized types'
```

これは前のテーブルから新しいテーブルへデータを読み込んで、この新しいテーブルに挿入することで埋めることができます：

```sql
INSERT INTO posts_v2 SELECT * FROM posts

0 rows in set. Elapsed: 146.471 sec. Processed 59.82 million rows, 83.82 GB (408.40 thousand rows/s., 572.25 MB/s.)
```

新しいスキーマでは、もはや null 値を保持しません。上記の挿入では、暗黙のうちにそれぞれの型に対するデフォルトの値に null 値を変換します - 整数の場合は 0、文字列の場合は空の値となります。ClickHouse はまた、あらゆる数値を目的の精度に自動的に変換します。
ClickHouse における主キー (オーダリングキー)
OLTP データベースから来たユーザーは、ClickHouse にある同等の概念を探すことがよくあります。

## オーダリングキーの選択

ClickHouse が一般的に使用されるスケールでは、メモリとディスクの効率が非常に重要です。データは ClickHouse テーブルにチャンクとして書き込まれ、このチャンクはパートとして知られ、背景でパートを統合するためのルールが適用されます。ClickHouse では、各パートにはそれ自体の主インデックスがあります。複数のパートが統合されると、統合されたパートの主インデックスも統合されます。パートの主インデックスは、行のグループごとに1つのインデックスエントリを持っています - この技法はスペースインデクシングと呼ばれます。

<img src={require('./images/schema-design-indices.png').default}    
  class='image'
  alt='ClickHouse におけるスペースインデクシング'
  style={{width: '600px'}} />

<br />

ClickHouse で選択されたキーは、インデックスだけでなく、ディスク上にデータが書き込まれる順序を決定します。このため、これはクエリパフォーマンスに影響を与える圧縮レベルに大きな影響を及ぼす可能性があります。ほとんどのカラムの値が連続した順序で書き込まれる注文キーは、選択された圧縮アルゴリズム (およびコーデック) がデータをより効果的に圧縮できるようにします。

> テーブル内のすべてのカラムは、オーダリングキーとして指定されたカラムの値に基づいてソートされます。キ自体に含まれているかどうかは問わず、すべてがソートされます。例えば、`CreationDate` がキーとして使用される場合、その他すべてのカラムの値の順序は `CreationDate` カラムの値の順序に対応します。複数のオーダリングキーを指定することが可能です - これは `SELECT` クエリで `ORDER BY` 句を使用する際の同じセマンティクスでソートします。

オーダリングキーを選択する際に簡単なルールを適用できます。以下は時には衝突することがありますので、順番に検討してください。これにより、プロセスからいくつかのキーを特定することができ、通常4-5つで十分です：

- 一般的なフィルタと一致するカラムを選択してください。カラムが `WHERE` 句によく使用される場合、優先順位付けし、使用頻度が少ないものよりもこれをキーに含めてください。
- フィルタリング時に全体の行の大部分を除外するのに役立つカラムを好んで選択してください、これは読み取る必要のあるデータ量を減少させます。
- テーブル内の他のカラムと非常に相関するカラムを好んで選択してください。これにより、これらの値が連続して保存されることを保証し、圧縮を改善します。
`GROUP BY` 及び `ORDER BY` 演算はキー内のカラムに対して行うことでよりメモリ効率を高めることができます。

オーダリングキーのサブセットを特定する際には、カラムを特定の順序で宣言してください。この順序はクエリの副次キーにおけるフィルタリングの効率性とテーブルのデータファイルの圧縮比率の両方にかなり影響を与えることがあります。一般的には、カーディナリティの昇順でキーを順序づけるのが最適です。これは、キー内の要素の順序がフィルタリングが効率的であることをバランス良く求めることになります。これらの動作をバランスよく取り入れて、アクセスパターンを考慮し（そしてもっと重要なことに異なる要因をテストしましょう）。

### 例

上記のガイドラインを `posts` テーブルに適用すると、例えば、利用者が日付および投稿タイプでフィルタリングして分析を行いたいと仮定します:

「直近3ヶ月で最もコメントされた質問はどれですか」。

この質問のクエリは、オーダリングキーを持たない `posts_v2` テーブルで型を最適化したものを使用すると次のようになります：

```
SELECT
    Id,
    Title,
    CommentCount
FROM posts_v2
WHERE (CreationDate >= '2024-01-01') AND (PostTypeId = 'Question')
ORDER BY CommentCount DESC
LIMIT 3

┌───────Id─┬─Title─────────────────────────────────────────────────────────────┬─CommentCount─┐
│ 78203063 │ How to avoid default initialization of objects in std::vector?	│       	74 │
│ 78183948 │ About memory barrier                                          	│       	52 │
│ 77900279 │ Speed Test for Buffer Alignment: IBM's PowerPC results vs. my CPU │       	49 │
└──────────┴───────────────────────────────────────────────────────────────────┴──────────────

10 rows in set. Elapsed: 0.070 sec. Processed 59.82 million rows, 569.21 MB (852.55 million rows/s., 8.11 GB/s.)
Peak memory usage: 429.38 MiB.
```

> ここではクエリがとても高速です、すべての 60m 行が線形スキャンされているにもかかわらず - ClickHouse はただ速いので :) オーダリングキーが TB や PB スケールで価値があると信じてください！

`PostTypeId` と `CreationDate` をオーダリングキーとして選択しましょう。

おそらく私たちの場合、ユーザーは常に `PostTypeId` でフィルタリングすると予想されます。これはカーディナリティが 8 であり、オーダリングキーの最初のエントリとして論理的な選択肢です。日付の粒度のフィルタリングが十分であると認識されるので (それは依然として datetime フィルターの利益を得ます) `toDate(CreationDate)` をキーの2番目の要素として使用します。これにより小さなインデックスも生成され、日付は16で表されることができ、フィルタリングを高速化します。我々の最終キーエントリは `CommentCount` で、最も多くコメントされた投稿を見つけるのを手助けします (最終ソート)。 

```sql
CREATE TABLE posts_v3
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
ORDER BY (PostTypeId, toDate(CreationDate), CommentCount)
COMMENT 'オーダリングキー'

--既存のテーブルからテーブルを埋める

INSERT INTO posts_v3 SELECT * FROM posts_v2

0 rows in set. Elapsed: 158.074 sec. Processed 59.82 million rows, 76.21 GB (378.42 thousand rows/s., 482.14 MB/s.)
Peak memory usage: 6.41 GiB.


以前のクエリに比べ、クエリ応答時間が3倍以上改善されます:

SELECT
    Id,
    Title,
    CommentCount
FROM posts_v3
WHERE (CreationDate >= '2024-01-01') AND (PostTypeId = 'Question')
ORDER BY CommentCount DESC
LIMIT 3

10 rows in set. Elapsed: 0.020 sec. Processed 290.09 thousand rows, 21.03 MB (14.65 million rows/s., 1.06 GB/s.)
```

特定の型と適切なオーダリングキーを使用することによって達成される圧縮の改善に関心を持つユーザーは、[ClickHouse における圧縮](/ja/data-compression/compression-in-clickhouse) を参照してください。ユーザーが圧縮をさらに改善する必要がある場合、[適切なカラム圧縮コーデックの選択](/ja/data-compression/compression-in-clickhouse#choosing-the-right-column-compression-codec)セクションをお勧めします。

## 次へ: データモデリング技法

ここまで、私たちは単一のテーブルのみを移行しました。これにより ClickHouse の基本的な概念を紹介することができましたが、残念ながら大半のスキーマはこれほど単純ではありません。

以下の一覧の他のガイドでは、最適な ClickHouse クエリのためにスキーマ全体を再構築するためのいくつかの技術を探求します。このプロセスを通じて、`Posts` をほとんどの分析クエリが実行される中心的なテーブルとして維持することを目指します。他のテーブルも独自にクエリすることが可能ですが、我々はほとんどの分析が `posts` を文脈としたものとして実行されることを想定しています。

> このセクションを通じて、他のテーブルの最適化されたバリアントを使用します。これらのスキーマは提供しますが、簡潔にするために行われた決定は省略します。これらは前述のルールに基づき、読者に決定を推測することを任せています。

次のアプローチはいずれも、読み取りとクエリパフォーマンスを最適化するために JOIN を使用する必要性を最小限に抑えることを目的としています。ClickHouse では JOIN が完全にサポートされていますが、最適なパフォーマンスを達成するためには、JOIN の使用を控えめにすることをお勧めします（JOIN クエリ内で2〜3テーブルにするのが理想です）。

> ClickHouse には外部キーの概念がありません。これは結合を禁止するものではありませんが、参照整合性はユーザーがアプリケーションレベルで管理する必要があることを意味します。ClickHouse のような OLAP システムでは、データの整合性は通常、データベース自体ではなくアプリケーションレベルやデータ取り込みプロセスで管理されます。これにより柔軟性が向上し、高速のデータ挿入が可能になります。このアプローチにより、非常に大きなデータセットでの読み取りと挿入クエリの速度とスケーラビリティに重点を置く ClickHouse の利点が最大化されます。

ユーザーがクエリ時に JOIN の使用を最小限に抑えるために、以下のツール/アプローチがあります:

- [**データの非正規化**](/ja/data-modeling/denormalization) - テーブルを組み合わせ、非1:1リレーションシップのために複合型を使用して、データを非正規化します。これには多くの場合、クエリ時間から挿入時間への結合の移動が含まれます。
- [**ディクショナリ**](/ja/dictionary) - 直接結合およびキー値ルックアップを扱うための ClickHouse 固有の機能。
- [**インクリメンタルマテリアライズドビュー**](/ja/materialized-view) - 計算のコストをクエリ時間から挿入時間にシフトするための ClickHouse の機能。集計値をインクリメンタルに計算する能力を含みます。
- [**リフレッシャブルマテリアライズドビュー**](/ja/materialized-view/refreshable-materialized-view) - 他のデータベース製品で使用されるマテリアライズドビューと似ており、クエリの結果を定期的に計算し、結果をキャッシュすることが可能です。

これらのアプローチを各ガイドで探求し、Stack Overflow データセットの質問を解決するためにこれを適用する方法を例示する際にどれが適切かを強調します。
