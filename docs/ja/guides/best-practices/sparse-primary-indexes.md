--- 
slug: /ja/optimize/sparse-primary-indexes 
sidebar_label: スパース主キーインデックス 
sidebar_position: 1 
description: このガイドでは、ClickHouseのインデックスについて深く掘り下げて解説します。 
---

# ClickHouseにおける主キーインデックスの実践的な紹介

## はじめに

このガイドでは、ClickHouseのインデックスについて詳しく解説します。以下の点について具体的に説明します：
- [ClickHouseのインデックスが従来のリレーショナルデータベース管理システムとどのように異なるか](#an-index-design-for-massive-data-scales)
- [ClickHouseがどのようにテーブルのスパース主キーインデックスを構築し利用しているか](#a-table-with-a-primary-key)
- [ClickHouseでのインデックス作成のベストプラクティスは何か](#using-multiple-primary-indexes)

このガイドで提案されているClickHouseのすべてのSQL文やクエリを、自分のマシンで実行することも可能です。ClickHouseのインストールと開始手順については、[クイックスタート](/docs/ja/quick-start.mdx)を参照してください。

:::note
このガイドはClickHouseのスパース主キーインデックスに焦点を当てています。

ClickHouseの[二次データスキッピングインデックス](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-data_skipping-indexes)については、[チュートリアル](/docs/ja/guides/best-practices/skipping-indexes.md)を参照してください。
:::


### データセット

このガイドを通じて、匿名化されたウェブトラフィックデータセットのサンプルを使用します。

- 8.87百万行（イベント）のサブセットを使用します。
- 圧縮されていないデータサイズは8.87百万イベントで約700 MBです。ClickHouseに格納すると200 MBに圧縮されます。
- サブセットでは、各行は特定の時間にURLをクリックしたインターネットユーザー（`UserID`カラム）が含まれています。

これらの3つのカラムを用いて、次のような一般的なウェブ分析クエリを既に立てることができます：

- 「特定のユーザーが最もクリックしたURLのトップ10は何ですか？」
- 「特定のURLを最も頻繁にクリックしたユーザーのトップ10は誰ですか？」
- 「あるユーザーが特定のURLをクリックする最も人気のある時間（例：週の日）はいつですか？」

### テストマシン

このドキュメントに示されている全ての実行時間は、Apple M1 Proチップと16GBのRAMを搭載したMacBook ProでClickHouseバージョン22.2.1をローカルで実行したものに基づいています。


### フルテーブルスキャン

主キーなしでデータセットに対してクエリがどのように実行されるかを確認するために、以下のSQL DDL文を実行してテーブルを作成します（テーブルエンジンはMergeTree）：

```sql
CREATE TABLE hits_NoPrimaryKey
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY tuple();
```

次に、以下のSQL挿入文を使用して、データセットのサブセットをテーブルに挿入します。これは、遠隔地にホストされている完全なデータセットのサブセットをロードするのに[URLテーブル関数](/docs/ja/sql-reference/table-functions/url.md)を使用します：

```sql
INSERT INTO hits_NoPrimaryKey SELECT
   intHash32(UserID) AS UserID,
   URL,
   EventTime
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz', 'TSV_FORMAT')
WHERE URL != '';
```
応答は：
```response
Ok.

0 rows in set. Elapsed: 145.993 sec. Processed 8.87 million rows, 18.40 GB (60.78 thousand rows/s., 126.06 MB/s.)
```

ClickHouseクライアントの結果出力は、上記の文が8.87百万行をテーブルに挿入したことを示しています。

最後に、このガイドでのディスカッションを簡潔にし、図や結果を再現可能にするために、FINALキーワードを使用してテーブルを[最適化](/docs/ja/sql-reference/statements/optimize.md)します：

```sql
OPTIMIZE TABLE hits_NoPrimaryKey FINAL;
```

:::note
一般的にはデータをロードした直後にテーブルを最適化することは必須でも推奨されることでもありません。この例でなぜこれが必要かは明らかになるでしょう。
:::

今、私たちは最初のウェブ分析クエリを実行します。次はインターネットユーザーID 749927693のトップ10最もクリックされたURLを計算しています：

```sql
SELECT URL, count(URL) as Count
FROM hits_NoPrimaryKey
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```
応答は：
```response
┌─URL────────────────────────────┬─Count─┐
│ http://auto.ru/chatay-barana.. │   170 │
│ http://auto.ru/chatay-id=371...│    52 │
│ http://public_search           │    45 │
│ http://kovrik-medvedevushku-...│    36 │
│ http://forumal                 │    33 │
│ http://korablitz.ru/L_1OFFER...│    14 │
│ http://auto.ru/chatay-id=371...│    14 │
│ http://auto.ru/chatay-john-D...│    13 │
│ http://auto.ru/chatay-john-D...│    10 │
│ http://wot/html?page/23600_m...│     9 │
└────────────────────────────────┴───────┘

10 rows in set. Elapsed: 0.022 sec.
// highlight-next-line
Processed 8.87 million rows,
70.45 MB (398.53 million rows/s., 3.17 GB/s.)
```

ClickHouseクライアントの結果出力は、ClickHouseがフルテーブルスキャンを実行したことを示しています！テーブルの8.87百万行のそれぞれがClickHouseにストリーミングされました。これはスケールしません。

これをより効率的で高速にするためには、適切な主キーを持つテーブルを使用する必要があります。これにより、ClickHouseは主キーのカラムに基づいて自動的にスパース主キーインデックスを作成でき、その後、クエリの実行を大幅に高速化することができます。

### 関連コンテンツ
- ブログ: [ClickHouseクエリの高速化](https://clickhouse.com/blog/clickhouse-faster-queries-with-projections-and-primary-indexes)

## ClickHouseインデックスデザイン

### 大規模データスケール向けのインデックス設計

従来のリレーショナルデータベース管理システムでは、主キーインデックスはテーブル行ごとに1つのエントリーを含みます。これにより、我々のデータセットでは主キーインデックスが8.87百万のエントリーを含むことになり、特定の行をすばやく見つけることができ、高効率なルックアップクエリとポイントアップデートが可能になります。`B(+)-Tree`データ構造でのエントリー検索は平均的に`O(log n)`の時間複雑度を持ちます。さらに正確には、`b`が通常数百から数千の範囲であるため、`B(+)-Tree`は非常に浅い構造であり、少数のディスクアクセスでレコードを見つけることができます。8.87百万の行と1000のブランチングファクターであれば、平均して2.3回のディスクアクセスが必要です。この機能はコストとともに来ます：追加のディスクとメモリのオーバーヘッド、新しい行をテーブルとインデックスに追加する際の高い挿入コスト、時にはB-Treeの再バランシングも必要です。

B-Treeインデックスに関連する課題を考慮して、ClickHouseのテーブルエンジンは異なるアプローチを利用します。ClickHouseの[MergeTreeエンジンファミリー](/docs/ja/engines/table-engines/mergetree-family/index.md)は、巨大なデータ量を処理するために設計され最適化されています。これらのテーブルは、毎秒数百万行の挿入を受け取り、非常に大きな（数百ペタバイト）のデータを保存するために設計されています。データはテーブルに対して[パートごと](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#mergetree-data-storage)に迅速に書き込まれ、バックグラウンドでパートのマージルールが適用されます。ClickHouseでは、各パートには独自の主キーインデックスがあります。パートがマージされると、マージされたパートの主キーインデックスもマージされます。ClickHouseが設計されている非常に大規模なスケールでは、ディスクとメモリの効率が非常に重要です。そのため、全ての行をインデックス化するのではなく、パートの主キーインデックスには行のグループ（「グラニュール」と呼ばれる）ごとに1つのインデックスエントリ（「マーク」として知られる）があります - このテクニックは**スパースインデックス**と呼ばれます。

スパースインデックスは、ClickHouseがディスク上で行を主キーのカラムで順序付けて保存するために可能です。`B-Tree`ベースのインデックスのように単一の行を直接見つけ出すのではなく、スパース主キーインデックスを使うことでクエリに一致する可能性のある行のグループを素早く（インデックスエントリに対するバイナリサーチを介して）特定することができます。クエリに一致する可能性がある行のグループ（グラニュール）は、その後、並行してClickHouseエンジンにストリーミングされ、マッチを見つけ出すことができます。このインデックスデザインにより、主キーインデックスは小さく（完全にメインメモリに収まる必要があります）、クエリ実行時間を大幅に短縮します：特にデータ分析ユースケースで一般的な範囲クエリの場合です。

以下では、ClickHouseがどのようにスパース主キーインデックスを構築し使用しているかを詳しく説明します。その後、テーブルのインデックス（主キーのカラム）を構築する際の選択、削除および順序付けのベストプラクティスをいくつか議論します。

### 主キーを持つテーブル

UserIDとURLをキーとした複合主キーを持つテーブルを作成します：

```sql
CREATE TABLE hits_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
// highlight-next-line
PRIMARY KEY (UserID, URL)
ORDER BY (UserID, URL, EventTime)
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, compress_primary_key = 0;
```

[//]: # (<details open>)
<details>
    <summary>
    DDL文の詳細
    </summary>
    <p>

このガイドでのディスカッションを簡潔にし、結果を再現可能にするため、DDL文は以下に示すように
<ul>
<li>`ORDER BY`句を使用してテーブルの複合ソートキーを指定します</li>
<br/>
<li>以下の設定を通じて、主キーインデックスのエントリー数を明示的に制御します：</li>
<br/>
<ul>
<li>`index_granularity`: デフォルト値の8192に明示的に設定されています。これは、8192行ごとに主キーインデックスに1つのエントリーが作成されることを意味します。つまり、テーブルに16384行が含まれている場合、インデックスには2つのエントリーが存在します。
</li>
<br/>
<li>`index_granularity_bytes`: <a href="https://clickhouse.com/docs/ja/whats-new/changelog/2019/#experimental-features-1" target="_blank">適応インデックス粒度</a>を無効にするために0に設定されます。適応インデックス粒度というのは、ClickHouseがn行のグループに対してインデックスエントリーを自動的に作成することを意味します。これらの条件が成り立つ場合：
<ul>
<li>nが8192より小さく、そのn行の合計データサイズが10 MB以上（index_granularity_bytesのデフォルト値）である場合、あるいは</li>
<li>n行の合計データサイズが10 MB未満であるが、nが8192である場合。</li>
</ul>
</li>
<br/>
<li>`compress_primary_key`: <a href="https://github.com/ClickHouse/ClickHouse/issues/34437" target="_blank">主キーの圧縮</a>を無効化するために0に設定されています。これにより、後でその内容を任意で確認できます。
</li>
</ul>
</ul>
</p>
</details>

上記のDDL文の主キーは、指定された2つのキーカラムに基づいて主キーインデックスを作成させます。

<br/>
次にデータを挿入します：

```sql
INSERT INTO hits_UserID_URL SELECT
   intHash32(UserID) AS UserID,
   URL,
   EventTime
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz', 'TSV_FORMAT')
WHERE URL != '';
```
応答は以下のようになります：
```response
0 rows in set. Elapsed: 149.432 sec. Processed 8.87 million rows, 18.40 GB (59.38 thousand rows/s., 123.16 MB/s.)
```

<br/>
そしてテーブルを最適化します：

```sql
OPTIMIZE TABLE hits_UserID_URL FINAL;
```

<br/>
以下のクエリを使用してテーブルのメタデータを取得できます：

```sql
SELECT
    part_type,
    path,
    formatReadableQuantity(rows) AS rows,
    formatReadableSize(data_uncompressed_bytes) AS data_uncompressed_bytes,
    formatReadableSize(data_compressed_bytes) AS data_compressed_bytes,
    formatReadableSize(primary_key_bytes_in_memory) AS primary_key_bytes_in_memory,
    marks,
    formatReadableSize(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE (table = 'hits_UserID_URL') AND (active = 1)
FORMAT Vertical;
```

応答は：

```response
part_type:                   Wide
path:                        ./store/d9f/d9f36a1a-d2e6-46d4-8fb5-ffe9ad0d5aed/all_1_9_2/
rows:                        8.87 million
data_uncompressed_bytes:     733.28 MiB
data_compressed_bytes:       206.94 MiB
primary_key_bytes_in_memory: 96.93 KiB
marks:                       1083
bytes_on_disk:               207.07 MiB

1 rows in set. Elapsed: 0.003 sec.
```

ClickHouseクライアントの出力が示しているのは：

- テーブルのデータが[ワイドフォーマット](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#mergetree-data-storage)で特定のディレクトリに保存されており、そのディレクトリ内でテーブルカラムごとにデータファイル（およびマークファイル）が1つあることを意味します。
- テーブルには8.87百万行が含まれています。
- 全ての行の非圧縮データサイズが733.28 MBです。
- 全ての行のディスク上での圧縮サイズは206.94 MBです。
- テーブルは、1083個のエントリー（「マーク」と呼ばれる）を持つ主キーインデックスを持ち、そのインデックスのサイズは96.93 KBです。
- 合計で、テーブルのデータおよびマークファイルと主キーインデックスファイルを合わせてディスク上で207.07 MBを消費しています。

### 主キーのカラムで順序付けされたディスク上のデータストア

上で作成したテーブルには
- 複合[主キー](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#primary-keys-and-indexes-in-queries) `(UserID, URL)` と
- 複合[ソートキー](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#choosing-a-primary-key-that-differs-from-the-sorting-key) `(UserID, URL, EventTime)` を持ちます。

:::note
- ソートキーのみを指定した場合、主キーは暗黙的にソートキーと同じものとして定義されます。

- メモリ効率を追求するために、クエリがフィルタリングするカラムだけを含む主キーを明示的に指定しました。主キーに基づく主キーインデックスは完全にメインメモリにロードされます。

- ガイド内の図に一貫性を持たせ、圧縮率を最大化するため、テーブルのすべてのカラムを含むソートキーを別途定義しました（カラム内の類似データを近くに配置すると、より良い圧縮が可能です）。

- 両方が指定された場合、主キーはソートキーの接頭辞である必要があります。
:::


挿入された行は、主キーのカラム（およびソートキーの追加カラム）で辞書的順序（昇順）でディスク上に格納されます。

:::note
ClickHouseは、主キーのカラム値が同一の複数の行を挿入することを許可しています。この場合（下図の行1と行2を参照）、最終的な順序は指定されたソートキーによって決まり、したがって`EventTime`カラムの値によって最終的な順序が決定されます。
:::



ClickHouseは<a href="https://clickhouse.com/docs/ja/introduction/distinctive-features/#true-column-oriented-dbms
" target="_blank">真の列指向データベース管理システム</a>です。図に示すように
- ディスク上の表現では、テーブルの各カラムに対して単一のデータファイル（*.bin）が存在し、そのカラムの全ての値が<a href="https://clickhouse.com/docs/ja/introduction/distinctive-features/#data-compression" target="_blank">圧縮された形式</a>で保存され、そして
- 行は、主キーのカラム（およびソートキーの追加カラム）で辞書的に昇順でディスク上に保存されています。つまりこの場合は
  - 最初に `UserID` で、
  - 次に `URL` で、
  - 最後に `EventTime` で：

<img src={require('./images/sparse-primary-indexes-01.png').default} class="image"/>
UserID.bin, URL.bin, および EventTime.binは、`UserID`、`URL`、および `EventTime`カラムの値が保存されているディスク上のデータファイルです。

<br/>
<br/>


:::note
- 主キーがディスク上の行の辞書的順序を定義するため、テーブルには1つの主キーしか持てません。

- 行を番号付けするとき、ClickHouseの内部行番号付けスキームと一致するように0から始めています。またログメッセージにも使用されます。
:::



### データはグラニュールに組織化され、並行的に処理される

データ処理の目的上、テーブルのカラムの値は論理的にグラニュールに分割されます。
グラニュールはClickHouseにストリーミングされる最小の分割可能なデータセットです。
これは、ClickHouseが個々の行を読むのではなく、常に1つのグラニュール（つまり、行のグループ）を読み取ることを意味します。
:::note
カラムの値はグラニュール内に物理的に保存されているわけではありません。グラニュールはクエリ処理のためのカラム値の論理的な組織です。
:::

以下の図は、我々のテーブルの8.87百万行（そのカラムの値）が、テーブルのDDL文に`index_granularity`（デフォルト値である8192に設定）の設定を含む結果として、1083個のグラニュールにどのように組織化されているかを示しています。

<img src={require('./images/sparse-primary-indexes-02.png').default} class="image"/>

最初の（ディスク上の物理的な順序に基づいた）8192行（そのカラムの値）は論理的にグラニュール0に属し、次の8192行（そのカラムの値）はグラニュール1に属する、という具合です。

:::note
- 最後のグラニュール（グラニュール1082）は8192行未満を「含んでいます」。

- このガイドの冒頭で述べたように、「DDL文の詳細」で適応インデックス粒度を無効化しました（このガイドのディスカッションを簡潔にし、図と結果を再現可能にするため）。

  したがって、我々の例のテーブルのすべてのグラニュール（最後の1つを除く）は同じサイズを持っています。

- 適応インデックス粒度（index_granularity_bytesが[デフォルトで](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#index_granularity_bytes)適応的である）を持つテーブルでは、一部のグラニュールのサイズが、行データサイズによっては8192行未満になることがあります。



- 主キーのカラム（`UserID`、`URL`）の一部のカラム値をオレンジ色でマークしました。
  これらのオレンジ色でマークされたカラム値は、各グラニュールの最初の行の主キーのカラム値です。
  これらのオレンジ色でマークされたカラム値が、テーブルの主キーインデックスのエントリーになります。

- グラニュールを番号付けするとき、ClickHouseの内部の番号付けスキームと一致し、またログメッセージにも使用されるように0から始めています。
:::



### 主キーインデックスはグラニュールごとに1エントリーを持つ

主キーインデックスは上図のグラニュールに基づいて作成されます。このインデックスは未圧縮のフラットアレイファイル（primary.idx）であり、0から始まる数値インデックスマークを含みます。

下記の図は、インデックスが各グラニュールの最初の行の主キーのカラム値（上図でオレンジ色でマークされている値）をどのように保存しているかを示しています。
あるいは、言い換えると：主キーインデックスは主キーのカラム値をテーブルの毎の8192行目から保存しています（物理的な順序に基づく）。
例えば
- 最初のインデックスエントリー（下図で「マーク0」）は、上図のグラニュール0の最初の行のキーカラム値を保存しています。
- 2番目のインデックスエントリー（下図で「マーク1」）は、上図のグラニュール1の最初の行のキーカラム値を保存しています。

<img src={require('./images/sparse-primary-indexes-03a.png').default} class="image"/>

合計でインデックスは、我々のテーブルの8.87百万行と1083グラニュール用に1083個のエントリーを持っています：

<img src={require('./images/sparse-primary-indexes-03b.png').default} class="image"/>

:::note
- 適応インデックス粒度（index_granularity_bytesが [デフォルトで](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#index_granularity_bytes)適応的である）を持つテーブルの場合、インデックスには、テーブル行の最後までの末尾の主キーのカラム値を記録する「最終」追加マークもストアされますが、我々の例のテーブルではこの適応インデックス粒度を無効化したため（このガイドのディスカッションを簡潔にし、図と結果を再現可能にするため）、実例のインデックスにはこの最終的なマークは含まれません。

- 主キーインデックスファイルは完全にメインメモリにロードされます。もしファイルが利用可能な空きメモリ容量より大きい場合、ClickHouseはエラーをスローします。
:::

<details>
    <summary>
    主キーインデックスの内容を調べる
    </summary>
    <p>

セルフマネージドのClickHouseクラスタでは、<a href="https://clickhouse.com/docs/ja/sql-reference/table-functions/file/" target="_blank">ファイルテーブル関数</a>を使用して、主キーインデックスの内容を調べることができます。

そのためにはまず、稼働中のクラスタのノードからの<a href="https://clickhouse.com/docs/ja/operations/server-configuration-parameters/settings/#server_configuration_parameters-user_files_path" target="_blank">user_files_path</a>に主キーインデックスファイルをコピーする必要があります：
<ul>
<li>ステップ1: 主キーインデックスファイルを含むパートパスを取得
</li>
`
SELECT path FROM system.parts WHERE table = 'hits_UserID_URL' AND active = 1
`


テストマシンでは、`/Users/tomschreiber/Clickhouse/store/85f/85f4ee68-6e28-4f08-98b1-7d8affa1d88c/all_1_9_4` を返します。

<li>ステップ2: user_files_pathを取得
</li>
Linuxの場合の<a href="https://github.com/ClickHouse/ClickHouse/blob/22.12/programs/server/config.xml#L505" target="_blank">デフォルトのuser_files_path</a>は
`/var/lib/clickhouse/user_files/`

Linuxでパスが変更されていたかどうかを確認できます。: `$ grep user_files_path /etc/clickhouse-server/config.xml`

テストマシンのパスは `/Users/tomschreiber/Clickhouse/user_files/` です。

<li>ステップ3: 主キーインデックスファイルをuser_files_pathにコピー
</li>
`
cp /Users/tomschreiber/Clickhouse/store/85f/85f4ee68-6e28-4f08-98b1-7d8affa1d88c/all_1_9_4/primary.idx /Users/tomschreiber/Clickhouse/user_files/primary-hits_UserID_URL.idx
`

<br/>

</ul>

上のSQLを使用して、主キーインデックスの内容を調べることができます：
<ul>
<li>エントリー数を取得
</li>
`
SELECT count( )<br/>FROM file('primary-hits_UserID_URL.idx', 'RowBinary', 'UserID UInt32, URL String');
`

<br/>
<br/>
`1083`を返します。
<br/>
<br/>
<li>最初の2つのインデックスマークを取得
</li>
`
SELECT UserID, URL<br/>FROM file('primary-hits_UserID_URL.idx', 'RowBinary', 'UserID UInt32, URL String')<br/>LIMIT 0, 2;
`
<br/>
<br/>
次を返します
<br/>
`
240923, http://showtopics.html%3...<br/>
4073710, http://mk.ru&pos=3_0
`
<br/>
<br/>
<li>最後のインデックスマークを取得
</li>
`
SELECT UserID, URL<br/>FROM file('primary-hits_UserID_URL.idx', 'RowBinary', 'UserID UInt32, URL String')<br/>LIMIT 1082, 1;
`
<br/>
<br/>
次を返します
<br/>
`
4292714039 │ http://sosyal-mansetleri...
`


</ul>

これは、我々の例のテーブルの主キーインデックスの内容の図と完全に一致します：
<img src={require('./images/sparse-primary-indexes-03b.png').default} class="image"/>
</p>
</details>

主キーエントリーはインデックスマークと呼ばれます、なぜなら各インデックスエントリーは特定のデータ範囲の開始を指し示しているためです。具体的にいうと例のテーブルでは：
- UserIDインデックスマーク：<br/>
  主キーインデックスに保存されている`UserID`値は昇順にソートされています。<br/>
  上記のマーク176は、すべての行がgranule 176にあり、その後のすべてのグラニュールにも`UserID`の値は749.927.693以上になることが保証されているということを示しています。

 [後述するように](#the-primary-index-is-used-for-selecting-granules)、この全体的な順序付けによりClickHouseはクエリが主キーの最初のカラムにフィルタリングしている場合インデックスマークに対して<a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1452" target="_blank">バイナリ検索アルゴリズム</a>を使用できるようになります。

- URLインデックスマーク：<br/>
  主キーのカラム`UserID`と`URL`のカードイナリティがほとんど同じであるため、最初のカラム以降のキーのインデックスマークは、一般に、先行キーの値に依存してグラニュール全体の値範囲のみを示します。<br/>
  たとえば、上記のグラフでマーク0とマーク1のUserIDが異なる場合、ClickHouseはすべてのグラフがグラニュール0のURLが`'http://showtopics.html%3...'`以上であると仮定できません。しかし、上記のグラフでマーク0とマーク1が同じであれば（つまり、グラニュール0内のすべての行が同じUserIDを持つ場合）、ClickHouseはすべてのグラニュール0までのグラニュール内の行のURLが`'http://showtopics.html%3...'`以上であると仮定することができます。

  このことのクエリの実行パフォーマンスに対する影響は後で詳しく議論します。

### 主キーインデックスはグラニュールの選択に使用される

次に、主キーインデックスによるサポートを受けたクエリを実行できます。

以下はUserID 749927693のトップ10のクリックされたURLを計算します。

```sql
SELECT URL, count(URL) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```

応答は以下の通りです：

```response
┌─URL────────────────────────────┬─Count─┐
│ http://auto.ru/chatay-barana.. │   170 │
│ http://auto.ru/chatay-id=371...│    52 │
│ http://public_search           │    45 │
│ http://kovrik-medvedevushku-...│    36 │
│ http://forumal                 │    33 │
│ http://korablitz.ru/L_1OFFER...│    14 │
│ http://auto.ru/chatay-id=371...│    14 │
│ http://auto.ru/chatay-john-D...│    13 │
│ http://auto.ru/chatay-john-D...│    10 │
│ http://wot/html?page/23600_m...│     9 │
└────────────────────────────────┴───────┘

10 rows in set. Elapsed: 0.005 sec.
// highlight-next-line
Processed 8.19 thousand rows,
740.18 KB (1.53 million rows/s., 138.59 MB/s.)
```

ClickHouseクライアントの出力は、今度はフルスキャンを実行せずに、わずか8.19千行がClickHouseにストリーミングされたことを示しています。

<a href="https://clickhouse.com/docs/ja/operations/server-configuration-parameters/settings/#server_configuration_parameters-logger" target="_blank">トレースログが有効化されている場合</a>、ClickHouseサーバーログファイルは、主キーインデックスの1083 UserIDマークをバイナリサーチを用いて、クエリに一致する可能性のある行を含むグラニュールを特定する過程を示しています。この処理には19ステップが必要で、平均的な時間複雑度は`O(log2 n)`です：

```response
...Executor): Key condition: (column 0 in [749927693, 749927693])
// highlight-next-line
...Executor): Running binary search on index range for part all_1_9_2 (1083 marks)
...Executor): Found (LEFT) boundary mark: 176
...Executor): Found (RIGHT) boundary mark: 177
...Executor): Found continuous range in 19 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line
              1/1083 marks by primary key, 1 marks to read from 1 ranges
...Reading ...approx. 8192 rows starting from 1441792
```

サンプルトレースログでは、1のマークがクエリに一致する可能性のあるものとして選択されたことが示されています。

<details>
    <summary>
    トレースログの詳細
    </summary>
    <p>

マーク176が識別されました（‘found left boundary mark’は包含的で、‘found right boundary mark’は除外的です）。したがって、グラニュール176から始まる8192行全てがClickHouseにストリーミングされて、実際に`UserID`カラム値が`749927693`の行を見つけます。
</p>
</details>

このことは<a href="https://clickhouse.com/docs/ja/sql-reference/statements/explain/" target="_blank">EXPLAIN句</a>を使用して簡単に再現できます：

```sql
EXPLAIN indexes = 1
SELECT URL, count(URL) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```

応答は以下のようになります：

```response
┌─explain───────────────────────────────────────────────────────────────────────────────┐
│ Expression (Projection)                                                               │
│   Limit (preliminary LIMIT (without OFFSET))                                          │
│     Sorting (Sorting for ORDER BY)                                                    │
│       Expression (Before ORDER BY)                                                    │
│         Aggregating                                                                   │
│           Expression (Before GROUP BY)                                                │
│             Filter (WHERE)                                                            │
│               SettingQuotaAndLimits (Set limits and quota after reading from storage) │
│                 ReadFromMergeTree                                                     │
│                 Indexes:                                                              │
│                   PrimaryKey                                                          │
│                     Keys:                                                             │
│                       UserID                                                          │
│                     Condition: (UserID in [749927693, 749927693])                     │
│                     Parts: 1/1                                                        │
// highlight-next-line
│                     Granules: 1/1083                                                  │
└───────────────────────────────────────────────────────────────────────────────────────┘

16 rows in set. Elapsed: 0.003 sec.
```
クライアントの出力は、1083個のgranuleのうち1つが、UserIDカラム値749927693の行を含む可能性があると選択されたことを示しています。

:::note 結論
クエリが複合キーの一部であり最初のキーカラムでフィルタリングしている場合、ClickHouseはプロミナリキーインデックスマークに対してバイナリサーチアルゴリズムを実行しています。
:::

ClickHouseはそのスパース主キーインデックスを使用して簡単に（バイナリサーチを通じて）クエリにマッチする可能性がある行を含むgranuleを選択しています。

これはClickHouseクエリ実行の**最初のステージ（granule選択）**です。

クエリ実行の**第二ステージ（データ読み取り）**では、ClickHouseは選択されたgranuleを特定し、その全ての行をClickHouseエンジンにストリーミングして、実際にクエリに一致する行を見つけるプロセスを並行して実行します。

以下のセクションでは、この第二段階について更に詳しく説明します。

### マークファイルはgranuleの位置を特定するために使用される

以下の図は我々のテーブルの主キーインデックスファイルの一部を示しています。

<img src={require('./images/sparse-primary-indexes-04.png').default} class="image"/>

上述したように、インデックス内の1083 UserIDマークに対してバイナリサーチを行い、マーク176が特定されます。したがって、クエリに一致する可能性がある行を含む可能性があるのは、その対応するgranule176のみです。

<details>
    <summary>
    Granule選択の詳細
    </summary>
    <p>

上記の図は、mark 176がその関連するgranule 176の最低UserID値が749.927.693よりも小さい最初のインデックスエントリーであることを示しています。そして、その次のマーク（マーク177）のgranule 177が、この値よりも大きい最小UserID値であることを示しています。したがって、クエリに一致する可能性がある行を含む可能性があるのは、その対応するgranule 176のみです。
</p>
</details>

granule 176にUserIDカラム値`749.927.693`を含む行があることを確認するために（またはしないために）、そのgranule内の全ての8192行をClickHouseにストリーミングする必要があります。

これを達成するため、ClickHouseはgranule 176の物理的位置を知っておく必要があります。

ClickHouseでは、テーブルの全てのgranuleの物理位置がマークファイルに保存されます。データファイルと同様に、各テーブルカラムにつき1つのマークファイルがあります。

以下の図は、テーブルのUserID、URL、およびEventTimeカラムのgranuleの物理位置を保存する3つのマークファイルUserID.mrk, URL.mrk, およびEventTime.mrkを示しています。

<img src={require('./images/sparse-primary-indexes-05.png').default} class="image"/>

我々は以前に、主キーインデックスは0から始まる数値インデックスマークを含む未圧縮のフラットアレイファイル（primary.idx）であると述べました。

同様に、マークファイルもまた、0から始まる数値インデックスマークを含む未圧縮のフラットアレイファイル（*.mrk）です。

インデックスで選択されたgranuleに一致する行が含まれる可能性がある場合、ClickHouseはそのmark番号に基づいてarray lookupを行い、granuleの物理位置を取得することができます。

ある特定のカラムに対する各マークファイルエントリーは、次の形でオフセットの位置を提供しています：

- 最初のオフセット（上記の図で「ブロックオフセット」）は、選択されたgranuleを含む圧縮column data fileのブロックを位置付けるためのものです。この圧縮されたファイルブロックは、おそらくいくつかの圧縮granuleを含んでいます。見つかった圧縮されたブロックは、メモリに読み込まれるときに解凍されます。

- 第二のオフセット（上記の図の「granuleオフセット」）は、解凍されたブロックデータ内のgranuleの位置を提供します。

その後、指定の解凍されたgranuleのすべての8192行が、さらに処理のためにClickHouseにストリーミングされます。

:::note

- wide formatを使い、適応インデックス粒度が無効なテーブルでは（これが関連する場合は上記の図に示されているように.mrkファイルを使用）、各マークファイルエントリーでは2つの8バイトのアドレスを含むエントリーがあり、それらはgranuleの物理位置で全て同じサイズを持っています。

インデックスの粒度は[デフォルトで](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#index_granularity_bytes)適応的ですが、我々の例のテーブルでは適応インデックス粒度を無効化しました（このガイドのディスカッションを簡潔にし、図と結果を再現可能にするため）。このテーブルは、データサイズが[min_bytes_for_wide_part](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#min_bytes_for_wide_part)よりも大きいため、ワイドフォーマットを使用しています（self-managed clustersのデフォルトでは10 MBです）。

- wide formatおよび適応インデックス粒度を持つテーブルでは、.mrk2マークファイルを使用します。これは.mrkファイルと同様のエントリーを持ちますが、エントリーごとになんらかの追加の第3値：granuleに対応する行数が記録されているファイルです。

- compact formatを持つテーブルには、.mrk3マークファイルが使用されます。

:::


:::note マークファイルの理由

なぜ主キーインデックスがインデックスの物理的位置を直接保存していないか？

なぜなら、ClickHouseが設計された非常に大規模なスケールでは、ディスクとメモリ効率が非常に重要だからです。

主キーインデックスファイルはメインメモリに完全に収まる必要があります。

我々のサンプルクエリのために、ClickHouseは主キーインデックスを使用してあくまでクエリに一致する可能性があるgranuleを選択しました。クエリの更なる処理のために、その1つのgranuleのみに対して物理的位置が必要です。

さらに、このオフセット情報はUserIDおよびURLカラムにのみ必要です。

クエリで使用していないカラム（例：EventTimeカラム）に対してオフセット情報が不要です。

我々のサンプルクエリでは、UserIDデータファイル（UserID.bin）とURLデータファイル（URL.bin）それぞれのgranule 176のために2つの物理的位置オフセットが必要です。

マークファイルによる間接化のおかげで、全

1083個のgranuleの物理位置を主キーインデックス内に直接保存することを回避できます：したがって、メモリ内に不要な（使用されない可能性のある）データを持たずに済みます。

:::

下記の図と以下のテキストは、我々のサンプルクエリに対してClickHouseがどのようにUserID.binデータファイル内のgranule 176を特定するかを示しています。

<img src={require('./images/sparse-primary-indexes-06.png').default} class="image"/>

このガイドの冒頭で述べたことから、ClickHouseは他にはないgranuleを選択し、クエリに一致する行を含む可能性があるgranule 176を選択しました。

ClickHouseは、UserID.mrkマークファイル内でgranule 176の2つのオフセットを取得するためにインデックスから選択されたマーク番号（176）を使用して位置取得を行います。

示されているように、最初のオフセットは、UserID.binデータファイル内のgranule 176の圧縮バージョンを含む圧縮ファイルブロックを位置付けるためのものです。

見つかったファイルブロックがメモリに解凍されると、マークファイルから2つ目のオフセットを使用して、解凍されたデータ内のgranule 176を特定できます。

ClickHouseは我々のサンプルクエリ（UserIDが749.927.693であるインターネットユーザーのためのトップ10の最もクリックされたURL）を実行するために、観念的にUserID.binデータファイル内と平行してURL.binデータファイル内のgranule176を特定する必要があります。2つのgranuleは整列し、ClickHouseエンジンのさらなる処理のためにストリーミングされます。これは、全行の中から、まず最初にUserIDでフィルタリングされ、次にURL値ごとにグループ化され、最後に最も大きな10のURLグループを逆順に出力します。


## 複数の主キーインデックスを使用する

<a name="filtering-on-key-columns-after-the-first"></a>

### セカンダリキーカラムは効率的でない可能性があります

クエリがcompound keyの一部であり、最初のキーカラムでフィルタリングしている場合、[ClickHouseはそのキーカラムのインデックスマークに対してバイナリサーチアルゴリズムを走らせます](#the-primary-index-is-used-for-selecting-granules)。

ですが、クエリがcompound keyの一部であっても最初のキーカラムではないカラムでフィルタリングしている場合はどうなるでしょう？

:::note
クエリが明示的に最初のキーカラムでフィルタリングしていないが、セカンダリキーカラムでフィルタリングしているシナリオについて説明します。

クエリが最初のキーカラムと任意のキーカラム(または複数カラム)の両方でフィルタリングしている場合、ClickHouseは最初のキーカラムのインデックスマークに対してバイナリサーチを行います。
:::

<br/>
<br/>

<a name="query-on-url"></a>
クエリを使って、"http://public_search"をクリックしたユーザーのトップ10を計算します。

```sql
SELECT UserID, count(UserID) AS Count
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```

応答はこうなります： <a name="query-on-url-slow"></a>
```response
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.086 sec.
// highlight-next-line
Processed 8.81 million rows,
799.69 MB (102.11 million rows/s., 9.27 GB/s.)
```

クライアントの出力は、[URLカラムが複合主キーの一部であるにもかかわらず](#a-table-with-a-primary-key)、ClickHouseがほぼフルテーブルスキャンを行ったことを示しています! ClickHouseがテーブルの8.87百万行中8.81百万行を読み取っています。

[トレースログ](/docs/ja/operations/server-configuration-parameters/settings.md/#server_configuration_parameters-logger)が有効化されている場合、ClickHouseサーバーログファイルは、ClickHouseが<a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1444" target="_blank">汎用排除検索</a>を使用して、
基準された1083 URLインデックスマークに、URLカラム値が"http://public_search"を含む行が存在する可能性のあるgranuleを特定していることを示しています：

```response
...Executor): Key condition: (column 1 in ['http://public_search',
                                           'http://public_search'])
// highlight-next-line
...Executor): Used generic exclusion search over index for part all_1_9_2
              with 1537 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line
              1076/1083 marks by primary key, 1076 marks to read from 5 ranges
...Executor): Reading approx. 8814592 rows with 10 streams
```

サンプルトレースログでは、10761083 granularが選択されて、URL値に一致する行を持っている可能性があることが示されています。この結果、8.81百万行がClickHouseエンジンにストリーミング（並行して10ストリームを使用）され、実際に"URL:"が"http://public_search"である行を特定します。しかし、[後に示すように](#query-on-url-fast)、選択された1076-granuleの中から実際に該当する行を持っているのは39だけです。

複合主キー（UserID, URL）に基づいた主キーインデックスは、特定のUserID値を持つ行をフィルタリングするクエリを加速するのには非常に役立ちましたが、指定のURL値を持つ行をフィルタリングするクエリを加速するにはあまり役立ちません。

そのため、URLカラムが最初のキーカラムではなく、したがってClickHouseは汎用排除検索アルゴリズム（バイナリサーチではありません）をURLカラムのインデックスマークに対して使用し、その**アルゴリズムの効果はUserIDカラムとのカードイナリティの差に依存している**からです。

このことを示すために、汎用排除検索がどのように機能するのかいくつかの詳細を示します。

<a name="generic-exclusion-search-algorithm"></a>

### 汎用排除検索アルゴリズム

以下は、クエリがセカンダリカラムでgranuleを選択するとき、先行するキーカラムに低いカードイナリティまたはより高いカードイナリティを持つ場合に<a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1438" target="_blank">ClickHouse汎用排除検索アルゴリズム</a>がどのように機能するかを示しています。

両方のケースの例として、考えてみます。
- URLの値が"W3"である行を検索するクエリ。
- 簡略化された値を持つ我々の打ち込みテーブルの抽象的なバージョン。
- 同じ合成キー（UserID, URL）を持つ表インデックス。つまり、行は最初にUserID値でソートされています。同じUserID値を持つ行はURLでソートされます。
- 2のgranuleサイズ、つまり各granuleに2行を含みます。

我々は、以下の図に示すように、各granuleの最初の

テーブル行のキーカラム値をオレンジ色でマークしました。

**先行キーカラムが低いカードイナリティを持つ場合**<a name="generic-exclusion-search-fast"></a>

UserIDに低いカードイナリティがあると仮定します。この場合、大きな可能性があるUserID値が複数のテーブル行やgranule、すなわちインデックスマークに広がることになります。同じUserIDを持つインデックスマークについては、インデックスマーク内のURL値は昇順でソートされます（テーブル行が最初にUserIDでその後URLでソートされるため）。これにより以下で説明されるような効率的なフィルタリングが可能です：

<img src={require('./images/sparse-primary-indexes-07.png').default} class="image"/>

上記の図では我々の抽象的なサンプルデータに対してgranule選択プロセスに関する3つの異なるシナリオが存在します：

1.**URL値がW3より小さく、次のインデックスマークのもURL値がW3より小さい場合のインデックスマーク0**：は、マーク0と1に同じUserID値があるため除外されます。この排除条件は、granule0が完全にU1 UserID値で構成されていることを確認して、ClickHouseがgranule0の最大URL値がW3より小さいと仮定してgranuleを除外できることを保証します。
   
2. **URL値がW3以下で、次のインデックスマークのURL値がW3以上の場合のインデックスマーク1**：は選択されます。これはgranule1がURL W3を持つ行を持つ可能性があることを意味します。

3. **URL値がW3より大きいインデックスマーク2および3**：は、主キーインデックスのインデックスマークは、それぞれのgranuleに対するテーブル行の最初のキーカラム値を含んでおり、テーブル行がキーカラム値でディスク上でソートされているため、granule2および3がURL値W3を含むことができないため除外されます。

**先行キーカラムが高いカードイナリティを持つ場合**<a name="generic-exclusion-search-slow"></a>
ユーザーIDのカーディナリティが高い場合、同じユーザーIDの値が複数のテーブル行やグラニュールに分散している可能性は低いです。これは、インデックスマークのURL値が単調増加していないことを意味します:

<img src={require('./images/sparse-primary-indexes-08.png').default} class="image"/>

上の図で示されているように、URL値がW3より小さいすべてのマークが選択され、その関連するグラニュールの行がClickHouseエンジンにストリーミングされます。

これは、図中のすべてのインデックスマークが上述のシナリオ1に該当するにもかかわらず、「次のインデックスマークが現在のマークと同じユーザーID値を持つ」という除外前提条件を満たしていないため、除外できないからです。

例えば、インデックスマーク0を考えてみます。このマークの**URL値はW3より小さく、次のインデックスマークのURL値もW3より小さい**です。しかし、次のインデックスマーク1が現在のマーク0と同じユーザーID値を持っていないため、これを除外することは*できません*。

これは最終的に、ClickHouseがグラニュール0のURL値の最大値について仮定をすることを妨げます。代わりに、グラニュール0がURL値W3を持つ行を含む可能性があると考え、マーク0を選択せざるを得ません。

同じ状況はマーク1, 2, 3についても真です。

:::note 結論
<a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1444" target="_blank">ClickHouseが使用している一般的な除外検索アルゴリズム</a>は、コンパウンドキーの一部であるカラムをフィルタリングしているときに、二分探索アルゴリズムを使用する代わりに最も効果的です。ただし、前のキーのカラムのカーディナリティが低い(または低い場合)です。
:::

サンプルデータセットでは、両方のキーのカラム(UserID, URL)が同様に高いカーディナリティを持ち、説明されているように、URLカラムの前のキーのカラムが高い(または同様の)カーディナリティを持つとき、一般的な除外検索アルゴリズムはあまり効果的ではありません。

### データスキップインデックスについての注意

ユーザーIDとURLのカーディナリティが同様に高いため、[URLでクエリをフィルタリングする](#query-on-url)ことによってURLカラムに[二次データスキップインデックス](./skipping-indexes.md)を作成しても大きな恩恵はありません。

例えば、次の2つのステートメントは、テーブルのURLカラムに[minmax](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#primary-keys-and-indexes-in-queries)データスキップインデックスを作成し、データを挿入します:
```sql
ALTER TABLE hits_UserID_URL ADD INDEX url_skipping_index URL TYPE minmax GRANULARITY 4;
ALTER TABLE hits_UserID_URL MATERIALIZE INDEX url_skipping_index;
```
ClickHouseは4つの連続する[グラニュール](#data-is-organized-into-granules-for-parallel-data-processing)のグループごとに最小URL値と最大URL値を保存する追加のインデックスを作成しました（`ALTER TABLE`ステートメントの上の`GRANULARITY 4`句に注意してください）。

<img src={require('./images/sparse-primary-indexes-13a.png').default} class="image"/>

最初のインデックスエントリ（図中の「マーク0」）は[テーブルの最初の4つのグラニュールに属する行](#data-is-organized-into-granules-for-parallel-data-processing)の最小URL値と最大URL値を保存します。

2番目のインデックスエントリ（‘マーク1’）はテーブルの次の4つのグラニュールに属する行の最小URL値と最大URL値を保存します。以降同様です。

（ClickHouseはインデックスマークに関連するグラニュールのグループを特定するための[マークファイル](#mark-files-are-used-for-locating-granules)を作成しました。）

ユーザーIDとURLのカーディナリティが同様に高いため、我々の[URLでクエリをフィルタリングする](#query-on-url)際に、この二次データスキップインデックスはグラニュールを除外するのに役立ちません。

クエリが探している特定のURL値（すなわち 'http://public_search'）は、各グラニュールのグループでインデックスにより保存されている最小値と最大値の間にある可能性が高く、ClickHouseはグラニュールのグループを選択せざるを得ません（クエリと一致する行が含まれている可能性があるため）。

### 複数の主キーインデックスを使用する必要性

その結果、特定のURLを持つ行をフィルタリングするサンプルクエリの実行速度を大幅に上げるためには、そのクエリに最適化された主キーインデックスを使用する必要があります。

さらに特定のユーザーIDを持つ行をフィルタリングするサンプルクエリの良好なパフォーマンスを維持したい場合、複数の主キーインデックスを使用する必要があります。

以下にその方法を示します。

<a name="multiple-primary-indexes"></a>

### 追加の主キーインデックスを作成するためのオプション

以下の三つの選択肢のいずれかを使用して、特定のユーザーIDでフィルタリングするクエリと特定のURLでフィルタリングするクエリの両方を劇的に高速化したい場合、複数の主キーインデックスを使用する必要があります。

- 異なる主キーを持つ**第2のテーブル**を作成します。
- 既存のテーブルに**マテリアライズドビュー**を作成します。
- 既存のテーブルに**プロジェクション**を追加します。

すべての選択肢において、サンプルデータが新しいテーブルに複製され、テーブル主キーインデックスと行ソート順が再編成されます。

しかし、クエリと挿入ステートメントのルーティングに関して、ユーザーにとっての追加テーブルの透明性は各選択肢で異なります。

異なる主キーを持つ**第2のテーブル**を作成するとき、クエリはクエリに最適なテーブルバージョンに明示的に送信され、新しいデータは両方のテーブルに明示的に挿入されてテーブルを同期した状態に保つ必要があります：
<img src={require('./images/sparse-primary-indexes-09a.png').default} class="image"/>

**マテリアライズドビュー**を使用すると、追加テーブルは暗黙的に作成され、データは両方のテーブル間で自動的に同期されます：
<img src={require('./images/sparse-primary-indexes-09b.png').default} class="image"/>

そして、**プロジェクション**は最も透明性の高いオプションです。これは、データの変更に伴って暗黙的に（隠されて）新しいテーブルを自動的に同期させるだけでなく、ClickHouseがクエリに最も効果的なテーブルバージョンを自動的に選択するからです：
<img src={require('./images/sparse-primary-indexes-09c.png').default} class="image"/>

以下では、複数の主キーインデックスを作成し使用するための3つのオプションについて、詳細かつ実際の例を交えて説明します。

<a name="multiple-primary-indexes-via-secondary-tables"></a>

### オプション1: 第2のテーブル

<a name="secondary-table"></a>
元のテーブルと比較して主キーのキー列の順序を変更して新しい追加テーブルを作成します：

```sql
CREATE TABLE hits_URL_UserID
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
// highlight-next-line
PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime)
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, compress_primary_key = 0;
```

すべての元のテーブル[#やテーブル(#a-table-with-a-primary-key)]から8.87百万行を追加する：

```sql
INSERT INTO hits_URL_UserID
SELECT * from hits_UserID_URL;
```

レスポンスは以下のようになります：

```response
Ok.

0 rows in set. Elapsed: 2.898 sec. Processed 8.87 million rows, 838.84 MB (3.06 million rows/s., 289.46 MB/s.)
```

最後にテーブルを最適化します：
```sql
OPTIMIZE TABLE hits_URL_UserID FINAL;
```

主キーの列の順序を変更したことにより、挿入された行はこれまでの[#やテーブル(#a-table-with-a-primary-key)]と比較してディスク上に異なる辞書順に格納され、そのためテーブルの1083グラニュールも異なる値を持つようになります：
<img src={require('./images/sparse-primary-indexes-10.png').default} class="image"/>

これは生成された主キーです：
<img src={require('./images/sparse-primary-indexes-11.png').default} class="image"/>

これは、URLカラムでフィルタリングを行うサンプルクエリの実行を大幅に高速化するために使用されます。例えば、URL "http://public_search"を最も頻繁にクリックしたトップ10のユーザーを計算します：
```sql
SELECT UserID, count(UserID) AS Count
// highlight-next-line
FROM hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```

レスポンスは以下のとおりです：
<a name="query-on-url-fast"></a>

```response
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.017 sec.
// highlight-next-line
Processed 319.49 thousand rows,
11.38 MB (18.41 million rows/s., 655.75 MB/s.)
```

前述の[#や主キー列使用時フィルタリング[#filtering-on-key-columns-after-the-first](#filtering-on-key-columns-after-the-first)をほぼ行ったときと異なり、ClickHouseはそのクエリをより効果的に実行しました。

元のテーブルでの主キーによるインデックスを持つ状況では，ユーザーIDは最初でURLは2番目のキー列であったため，ClickHouseはインデックスマークに対して一般的な除外検索を実行していたが，ユーザーIDとURLのカーディナリティが同じかったため，それはあまり効果的でなかった．

URLが主キーの最初の列として考慮されると，ClickHouseはインデックスマークに対する<a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1452" target="_blank">バイナリサーチ</a>を実行しています。

ClickHouseサーバーログの対応するトレースログはそれを確認します：
```response
...Executor): Key condition: (column 0 in ['http://public_search',
                                           'http://public_search'])
// highlight-next-line
...Executor): Running binary search on index range for part all_1_9_2 (1083 marks)
...Executor): Found (LEFT) boundary mark: 644
...Executor): Found (RIGHT) boundary mark: 683
...Executor): Found continuous range in 19 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line
              39/1083 marks by primary key, 39 marks to read from 1 ranges
...Executor): Reading approx. 319488 rows with 2 streams
```
 ClickHouseは1076の汎用的除外検索を使用していたとき以下の39のインデックスマークだけを選択しました。

追加のテーブルは、URLでフィルタリングするサンプルクエリの実行速度を向上させるために最適化されています。

元のテーブルでの[#クエリのバッドパフォーマンス](#query-on-url-slow)と同様に、新しい追加のテーブルヒット_URL_ユーザーIDにおけるユーザーIDでフィルタリングするクエリの例はあまり効果的ではありません。ユーザーIDがテーブルの主キーの2番目のキー列であるため、ClickHouseはグラニュール選択のための一般的除外検索を使用しています。これがユーザーIDとURLというように同様に高いカーディナリティを持つ場合、あまり効果的でありません。この詳細については詳細を開いてください。
<details>
    <summary>
    ユーザーIDでフィルタリングするクエリはパフォーマンスが悪くなった<a name="query-on-userid-slow"></a>
    </summary>
    <p>

```sql
SELECT URL, count(URL) AS Count
FROM hits_URL_UserID
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```

レスポンスは：

```response
┌─URL────────────────────────────┬─Count─┐
│ http://auto.ru/chatay-barana.. │   170 │
│ http://auto.ru/chatay-id=371...│    52 │
│ http://public_search           │    45 │
│ http://kovrik-medvedevushku-...│    36 │
│ http://forumal                 │    33 │
│ http://korablitz.ru/L_1OFFER...│    14 │
│ http://auto.ru/chatay-id=371...│    14 │
│ http://auto.ru/chatay-john-D...│    13 │
│ http://auto.ru/chatay-john-D...│    10 │
│ http://wot/html?page/23600_m...│     9 │
└────────────────────────────────┴───────┘

10 rows in set. Elapsed: 0.024 sec.
// highlight-next-line
Processed 8.02 million rows,
73.04 MB (340.26 million rows/s., 3.10 GB/s.)
```

サーバーログ:
```response
...Executor): Key condition: (column 1 in [749927693, 749927693])
// highlight-next-line
...Executor): Used generic exclusion search over index for part all_1_9_2
              with 1453 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line
              980/1083 marks by primary key, 980 marks to read from 23 ranges
...Executor): Reading approx. 8028160 rows with 10 streams
```
</p>
</details>

我々は今、二つのテーブルを持っています。ユーザーIDでフィルタリングするクエリを加速するために最適化されたテーブルと、URLでフィルタリングするクエリを加速するために最適化されたテーブルです：
<img src={require('./images/sparse-primary-indexes-12a.png').default} class="image"/>

### オプション2: マテリアライズドビュー

既存のテーブルに[マテリアライズドビュー](/docs/ja/sql-reference/statements/create/view.md)を作成します。
```sql
CREATE MATERIALIZED VIEW mv_hits_URL_UserID
ENGINE = MergeTree()
PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime)
POPULATE
AS SELECT * FROM hits_UserID_URL;
```

レスポンスは以下のようになります：

```response
Ok.

0 rows in set. Elapsed: 2.935 sec. Processed 8.87 million rows, 838.84 MB (3.02 million rows/s., 285.84 MB/s.)
```

:::note
- ビューの主キーにおいて、元のテーブルと比較してキーのカラムの順序を入れ替えています。
- マテリアライズドビューは**暗黙的に作成されたテーブル**によってバックアップされ、その行の順序と主キーは与えられたプライマリキー定義に基づいています。
- 暗黙的に作成されたテーブルは`SHOW TABLES`クエリによって表示され、その名前は`.inner`で始まります。
- マテリアライズドビューのバックアップテーブルをまず明示的に作成してから、そのビューを`TO [db].[table]`[句](/docs/ja/sql-reference/statements/create/view.md)を使ってそれにターゲットさせることも可能です。
- `POPULATE`キーワードを使用して、ソーステーブル[ヒット_ユーザーID_URL](#a-table-with-a-primary-key)から暗黙的に作成されたテーブルに即座にすべての8.87百万行を挿入します。
- ソーステーブルヒット_ユーザーID_URLに新しい行が挿入されると、その行は暗黙的に作成されたテーブルにも自動的に挿入されます。
- 暗黙的に作成されたテーブルは、以前に明示的に作成した二次テーブルと同じ行順と主キーを持つことになります：

<img src={require('./images/sparse-primary-indexes-12b-1.png').default} class="image"/>

ClickHouseは[カラムデータファイル](#data-is-stored-on-disk-ordered-by-primary-key-columns) (*.bin)、[マークファイル](#mark-files-are-used-for-locating-granules) (*.mrk2)、およびインデックス(`primary.idx`)を、ClickHouseサーバーのデータディレクトリ内の特別なフォルダに格納しています：

<img src={require('./images/sparse-primary-indexes-12b-2.png').default} class="image"/>

:::


マテリアライズドビューのバックアップとして暗黙的に作成されたテーブル（およびその主キー）は、URLカラムでフィルタリングを行うサンプルクエリの実行を大幅に高速化するために現時点で使用されます：
```sql
SELECT UserID, count(UserID) AS Count
// highlight-next-line
FROM mv_hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```

レスポンスは以下の通りです：

```response
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.026 sec.
// highlight-next-line
Processed 335.87 thousand rows,
13.54 MB (12.91 million rows/s., 520.38 MB/s.)
```

効果的に言えば、暗黙的に作成されたテーブル（およびそのプライマリインデックス）は、以前に明示的に作成した二次テーブルと同一なので、クエリは明示的に作成したテーブルと同じ効果的な方法で実行されます。

ClickHouseサーバーログの対応するトレースログは、ClickHouseがインデックスマークにバイナリサーチ実行していることを確認します：

```response
...Executor): Key condition: (column 0 in ['http://public_search',
                                           'http://public_search'])
// highlight-next-line
...Executor): Running binary search on index range ...
...
...Executor): Selected 4/4 parts by partition key, 4 parts by primary key,
// highlight-next-line
              41/1083 marks by primary key, 41 marks to read from 4 ranges
...Executor): Reading approx. 335872 rows with 4 streams
```



### オプション3: プロジェクション

既存のテーブルにプロジェクションを追加：
```sql
ALTER TABLE hits_UserID_URL
    ADD PROJECTION prj_url_userid
    (
        SELECT *
        ORDER BY (URL, UserID)
    );
```

そしてプロジェクションをマテリアライズします：
```sql
ALTER TABLE hits_UserID_URL
    MATERIALIZE PROJECTION prj_url_userid;
```

:::note
- プロジェクションは行順と主キー定義に基づいている**隠れたテーブル**を作成する。
- 隠れたテーブルは`SHOW TABLES`クエリには表示されません。
- `MATERIALIZE`キーワードを使用して、ソーステーブル[#hitsUserID_URL](#a-table-with-a-primary-key)からすべての8.87百万行を即座に格納する。
- ソーステーブルの[#hits_USERID_URL](#a-table-with-a-primary-key)に新しい行が挿入されると、その行は隠れたテーブルにも自動的に挿入されます。
- 常にクエリは構文上でソーステーブル[#hits_USERID_URL](#a-table-with-a-primary-key)をターゲットにしており、行の順序と主キーのある隠れたテーブルがより効果的なクエリ実行を可能にする場合、そのクエリに対応するものです。
- しかし、ORDER BYがプロジェクションのORDER BYと一致する場合でも、プロジェクションはクエリのORDER BYをより効率的にすることはないことに注意してください (詳細は、https://github.com/ClickHouse/ClickHouse/issues/47333 を参照)。
- 事実上、暗黙に作成された隠れたテーブルは以前に明示的に作成した二次テーブルと同様の行順と主キーを持ちます：

<img src={require('./images/sparse-primary-indexes-12c-1.png').default} class="image"/>

ClickHouseは、ソーステーブルのデータファイル、マークファイル、および主キーと一緒に、特別なフォルダ（オレンジ色で表示されています）に隠れたテーブルの[カラムデータファイル](#data-is-stored-on-disk-ordered-by-primary-key-columns) (*.bin)、マークファイル (*.mrk2)、および主キー (primary.idx) ファイルを格納しています：

<img src={require('./images/sparse-primary-indexes-12c-2.png').default} class="image"/>
:::


仮想的な隠れたテーブルとその主キーはURLカラムでフィルタリングを行うサンプルクエリの実行スピードを劇的に上げるために暗黙的に使用されます。注意として、クエリは構文的にプロジェクションのソーステーブルをターゲットにしています。
```sql
SELECT UserID, count(UserID) AS Count
// highlight-next-line
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```

レスポンスは以下の通りです：

```response
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.029 sec.
// highlight-next-line
Processed 319.49 thousand rows, 1
1.38 MB (11.05 million rows/s., 393.58 MB/s.)
```

暗黙的に作成された隠れたテーブルとその主キーは、以前に明示的にテーブルを作成したものと同一であるため、クエリは明示的にテーブルを作成したときと同様に効果的に実行されます。

ClickHouseサーバーログの対応するトレースログが、ClickHouseがインデックスマークに対してバイナリサーチを行っていることを確認しました：


```response
...Executor): Key condition: (column 0 in ['http://public_search',
                                           'http://public_search'])
// highlight-next-line
...Executor): Running binary search on index range for part prj_url_userid (1083 marks)
...Executor): ...
// highlight-next-line
...Executor): Choose complete Normal projection prj_url_userid
...Executor): projection required columns: URL, UserID
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line
              39/1083 marks by primary key, 39 marks to read from 1 ranges
...Executor): Reading approx. 319488 rows with 2 streams
```

### まとめ

コンパウンドプライマリキーを持つ我々の[#テーブル](#a-table-with-a-primary-key)のプライマリインデックスが[ユーザーIDでフィルタリングするクエリ](#the-primary-index-is-used-for-selecting-granules)を加速するのに有用であった。 しかし、そのインデックスは、URLカラムがコンパウンドプライマリキーに加わっているにもかかわらず[URLでフィルタリングするクエリ](#guidelines-for-choosing-either-mergetree-or-replicatedmergetree)-の高速化には貢献しませんでした。

逆に:[URL, UserID](#secondary-table)のコンパウンドプライマリキーを持つ我々の[#テーブル](#a-table-with-a-primary-key)のプライマリインデックスは[URLでフィルタリングするクエリ](#query-on-url)を加速しましたが、[ユーザーIDでフィルタリングするクエリ](#the-primary-index-is-used-for-selecting-granules)-実行にあまり貢献しませんでした。

プライマリキーのカラムユーザーIDとURLのカーディナリティが似ているため、2つ目のキーのカラムがインデックスに含まれていても、[2番目のキーのカラムでフィルタリングを行うクエリ](#generic-exclusion-search-slow)はその恩恵を受けません。

したがって、プライマリインデックスから2番目のキーのカラムを削除すること（インデックスのメモリ使用量が少なくなる結果）や、 [複数のプライマリインデックス](#multiple-primary-indexes)を使用することは理にかなっています。

しかしながら、コンパウンドプライマリキーのキーのカラム群がカーディナリティの大きな違いがある場合、 [クエリにとって](#generic-exclusion-search-fast)カーディナリティにしたがってプライマリキーのカラムを昇順に並べることは利益になります。

キーのカラム間のカーディナリティ差が高ければ高いほど、それらのカラムがキー内で並べられる順序の影響が大きくなります。次のセクションでそれをデモンストレーションします。

## キーのカラムを効率的に並べ替える

<a name="test"></a>

コンパウンドプライマリキーではキーのカラムの順序は、以下の両方において大きく影響を与えます：
- クエリ内の二次キーのカラムに対するフィルタリングの効率
- テーブルのデータファイルの圧縮率

それを証明するため、我々はwebトラフィックのサンプルデータセットのバージョンを使用します。
各行がインターネットユーザー（`UserID`カラム）がURL（`URL`カラム）へのアクセスがbotトラフィックとしてマークされたかどうかを示す3つのカラムを持っています。

1つのコンパウンドプライマリキーを使用し、web分析クエリを高速化するためのすべての3つのカラムをキーのカラムとして使用します。以下が考えられるクエリ：
- 特定のURLへのトラフィックの何%がbotによるものか
- 指定のユーザーがbotかどうか（そのユーザーからのトラフィックの何%がbotと仮定されているか）
など

コンパウンドプライマリキーに使用しようとしている3つのカラムのカーディナリティを計算するためにこのクエリを使用します（TSVデータをアドホックにクエリするために[URLテーブル関数](/docs/ja/sql-reference/table-functions/url.md)を使用しています）。このクエリを`clickhouse client`で実行します：
```sql
SELECT
    formatReadableQuantity(uniq(URL)) AS cardinality_URL,
    formatReadableQuantity(uniq(UserID)) AS cardinality_UserID,
    formatReadableQuantity(uniq(IsRobot)) AS cardinality_IsRobot
FROM
(
    SELECT
        c11::UInt64 AS UserID,
        c15::String AS URL,
        c20::UInt8 AS IsRobot
    FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz')
    WHERE URL != ''
)
```
レスポンスは以下の通りです：
```response
┌─cardinality_URL─┬─cardinality_UserID─┬─cardinality_IsRobot─┐
│ 2.39 million    │ 119.08 thousand    │ 4.00                │
└─────────────────┴────────────────────┴─────────────────────┘

1 row in set. Elapsed: 118.334 sec. Processed 8.87 million rows, 15.88 GB (74.99 thousand rows/s., 134.21 MB/s.)
```

特に`URL`および`IsRobot`カラム間に非常に大きなカードナリティの違いがあります。したがって、このコンパウンドプライマリキーのカラムの順番は、そのカラムでフィルタリングするクエリの効率的な加速と、テーブルのカラムデータファイルの最適な圧縮比を達成するために重要です。

それを証明するため、我々はボットトラフィック分析データのために2つのテーブルバージョンを作ります：
- コンパウンドプライマリキーが`(URL, UserID, IsRobot)`で、キーのカラムをカードナリティの降順に並べた`hits_URL_UserID_IsRobot`テーブル
- コンパウンドプライマリキーが`(IsRobot, UserID, URL)`で、キーのカラムをカードナリティの昇順に並べた`hits-IsRobot_UserID_URL`テーブル

コンバウンドプライマリキー`(URL, UserID, IsRobot)`を持つ`hits_URL_UserID_IsRobot`テーブルを作成する：
```sql
CREATE TABLE hits_URL_UserID_IsRobot
(
    `UserID` UInt32,
    `URL` String,
    `IsRobot` UInt8
)
ENGINE = MergeTree
// highlight-next-line
PRIMARY KEY (URL, UserID, IsRobot);
```

そして、887百万行を挿入します：
```sql
INSERT INTO hits_URL_UserID_IsRobot SELECT
    intHash32(c11::UInt64) AS UserID,
    c15 AS URL,
    c20 AS IsRobot
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz')
WHERE URL != '';
```
レスポンスは以下のようになります：
```response
0 rows in set. Elapsed: 104.729 sec. Processed 8.87 million rows, 15.88 GB (84.73 thousand rows/s., 151.64 MB/s.)
```


次にコンパウンドプライマリキー`(IsRobot, UserID, URL)`を持つ`hits_IsRobot_UserID_URL`テーブルを作成します：
```sql
CREATE TABLE hits_IsRobot_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `IsRobot` UInt8
)
ENGINE = MergeTree
// highlight-next-line
PRIMARY KEY (IsRobot, UserID, URL);
```
そして、以前のテーブルに挿入した行と同じ8.87百万行を挿入します：

```sql
INSERT INTO hits_IsRobot_UserID_URL SELECT
    intHash32(c11::UInt64) AS UserID,
    c15 AS URL,
    c20 AS IsRobot
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz')
WHERE URL != '';
```
レスポンスは以下の通り：
```response
0 rows in set. Elapsed: 95.959 sec. Processed 8.87 million rows, 15.88 GB (92.48 thousand rows/s., 165.50 MB/s.)
```



### 二次キーのカラムでの効率的なフィルタリング

クエリがコンパウンドキーの一部のカラム、すなわち最初のキーのカラムでフィルタリングされている場合、ClickHouseは[#バイナリ検索アルゴリズム](#the-primary-index-is-used-for-selecting-granules)をそのキーのカラムのインデックスマークで実行します。

クエリがコンパウンドキーの一部のカラム（最初のキーのカラムでない）にのみフィルタリングされている場合、ClickHouseは[#一般的除外検索アルゴリズム](#secondary-key-columns-can-not-be-inefficient)をそのキーのカラムのインデックスマークで使用します。

2番目のケースでは、コンパウンドプライマリキーのキーのカラムの順序が一般的除外検索アルゴリズムの[#効率にとって重要です](https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1444)。

以下はカードナリティが降順となる`(URL, UserID, IsRobot)`のキーのカラムを並べ替えたテーブルでUserIDカラムをフィルタリングしているクエリです：
```sql
SELECT count(*)
FROM hits_URL_UserID_IsRobot
WHERE UserID = 112304
```
レスポンスは以下のとおりです：
```response
┌─count()─┐
│      73 │
└─────────┘

1 row in set. Elapsed: 0.026 sec.
// highlight-next-line
Processed 7.92 million rows,
31.67 MB (306.90 million rows/s., 1.23 GB/s.)
```

これも、先に作成した、カードナリティが昇順になるようにキーのカラムを並べた`(IsRobot, UserID, URL)`のテーブルで同じクエリ：
```sql
SELECT count(*)
FROM hits_IsRobot_UserID_URL
WHERE UserID = 112304
```
レスポンスは以下のとおりです：
```response
┌─count()─┐
│      73 │
└─────────┘

1 row in set. Elapsed: 0.003 sec.
// highlight-next-line
Processed 20.32 thousand rows,
81.28 KB (6.61 million rows/s., 26.44 MB/s.)
```

カードナリティを昇順に並べたテーブルの方が、クエリの実行が大幅に効果的で速いことが分かります。

その理由は、最も効果的に[一般的除外検索アルゴリズム](https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1444)が[グラニュール](#the-primary-index-is-used-for-selecting-granules)が選択される時の二次キーのカラムにおける、先行するキーのカラムのカードナリティが低い場合です。それを前述のセクションで詳しく説明しました。

### データファイルの最適な圧縮比

次のクエリは、上で作成した2つのテーブル間のユーザーIDカラムの圧縮比を比較します：

```sql
SELECT
    table AS Table,
    name AS Column,
    formatReadableSize(data_uncompressed_bytes) AS Uncompressed,
    formatReadableSize(data_compressed_bytes) AS Compressed,
    round(data_uncompressed_bytes / data_compressed_bytes, 0) AS Ratio
FROM system.columns
WHERE (table = 'hits_URL_UserID_IsRobot' OR table = 'hits_IsRobot_UserID_URL') AND (name = 'UserID')
ORDER BY Ratio ASC
```
これがレスポンスです：
```response
┌─Table───────────────────┬─Column─┬─Uncompressed─┬─Compressed─┬─Ratio─┐
│ hits_URL_UserID_IsRobot │ UserID │ 33.83 MiB    │ 11.24 MiB  │     3 │
│ hits_IsRobot_UserID_URL │ UserID │ 33.83 MiB    │ 877.47 KiB │    39 │
└─────────────────────────┴────────┴──────────────┴────────────┴───────┘

2 rows in set. Elapsed: 0.006 sec.
```
カードナリティを昇順に並べたテーブル`(IsRobot, UserID, URL)`では、`UserID`カラムの圧縮比が大幅に高いことが確認されます。

どちらのテーブルにも同じデータが保存されている（同じ8.87百万行を両方のテーブルに挿入しました）にもかかわらず、コンポーネントプライマリキーのキーのカラムの順序がテーブルの[カラムデータファイル](#data-is-stored-on-disk-ordered-by-primary-key-columns)に格納される圧縮データがどれだけのディスクスペースを要するかに大きな影響を与えます：
- カードナリティを降順に並べたコンパウンドプライマリキー`(URL, UserID, IsRobot)`を持つ`hits_URL_UserID_IsRobot`テーブルでは、データファイル`UserID.bin`のディスクスペースは**11.24 MiB**です。
- カードナリティを昇順に並べたコンパウンドプライマリキー`(IsRobot, UserID, URL)`を持つ`hits_IsRobot_UserID_URL`テーブルの場合、データファイル`UserID.bin`のディスクスペースはわずか**877.47 KiB**です。

ディスク上でのテーブルのカラムの良好な圧縮比を持つことは、ディスクスペースを節約するだけでなく、それによってそのカラムからデータを読み込まなければならないクエリ（特に分析クエリ）が高速化され、メインメモリのファイルキャッシュにディスクからデータを移動するためのi/oが少なくて済む。

次に例を示すことで、プライマリキーのカラムをカードナリティにしたがって昇順に並べることがテーブルのカラムの圧縮比にとって有利な理由を説明します。

以下の図は、プライマリキーのカラムをカードナリティにしたがって昇順に並べた場合の行をディスクに並べる順序を概略します：
<img src={require('./images/sparse-primary-indexes-14a.png').default} class="image"/>

テーブルの行データはプライマリキーのカラムでディスクに格納されることを[議論しました](#data-is-stored-on-disk-ordered-by-primary-key-columns)。

上記の方法では、そのテーブルの行はまず`cl`値でソートされ、同じ`cl`値を持つ行は`ch`値でソートされます。そしてカードナリティが低い最初のキーのカラム`cl`があるため、同じ`cl`値の行が存在する可能性が高い。したがって、`ch`値は（同じ`cl`値の行で）局所的にソートされています。

カラム内で、類似したデータが近隣に配置されると、データの圧縮が容易になります
一般的に、圧縮アルゴリズムはデータの長さに対する（より多くのデータが使用されるほど、圧縮のためにより良くなる）および局所性（より類似したデータであれば、より良い圧縮比率が得られる）を考慮します。

これに対して、以下の図は、プライマリキーのカラムをカードナリティにしたがって降順に並べた場合の行をディスクに並べる順序を概略します：
<img src={require('./images/sparse-primary-indexes-14b.png').default} class="image"/>

今度はテーブルの行がまず`ch`値を基に並べられ、同じ`ch`値を持つ行は`cl`値で最終的な順序が決定される。
ただし、最初のキーのカラム`ch`が非常に高いカードナリティを持っているため、同じ`ch`値を持つ行がほとんどない可能性がある。したがって、`cl`値が（同じ`ch`値を持つ行で）局所的にソートされる可能性は少ない。

したがって、`cl`値はおそらくランダムな順序であり、局所性が悪く圧縮比も悪い。

### サマリー

二次キーのカラムでの効率的なフィルタリングとテーブルのカラムデータファイルの圧縮比の両方において、プライマリキー内のカラムをそのカーディナリティに従って昇順に並べることが有益です。

### 関連コンテンツ
- Blog: [クリックハウスのクエリをスーパーチャージするには](https://clickhouse.com/blog/clickhouse-faster-queries-with-projections-and-primary-indexes)

## シングル行を効率的に特定する

場合によってはClickHouse上に構築したアプリケーションがClickHouseテーブルの特定の行を特定する必要があることがありますが、通常はClickHouseの[^best-uses-for-clickhouse]最適な使用方法ではありません。

そのための直感的な解決策は、各行に一意の値を持つ[UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier)カラムを使い、行の高速な検索の目的でそのカラムをプライマリキーのカラムとして使用することです。

最も高速な検索のためには、UUIDカラムはプライマリキーのカラムの最初に[する必要があります](#the-primary-index-is-used-for-selecting-granules) 。

プライマリキーまたはカードナリティが非常に高いカラムを含むコンパウンドプライマリキーのカラムを、そのプライマリキー列の後の低いカードナリティのカラムより前に持つことがあると、[テーブル内の他のカラムの圧縮比の劣化](#optimal-compression-ratio-of-data-files)につながります。

最速の検索と最適なデータ圧縮の妥協としては、UUIDを最後のキー列として持つコンパウンドプライマリキーを使用し、いくつかのテーブルのカラムで良好な圧縮比を確保しています。

### 具体的な例

具体的な例として、Alexey Milovidovが開発し[ブログに書いた例](https://clickhouse.com/blog/building-a-paste-service-with-clickhouse/) のhttps://pastila.nlが柔軟なペーストサービスがあります。

テキストエリアへの変更があると（例えば、テキストエリアでのタイピングによるキー入力のたびに）、データが自動でClickHouseテーブル行（変更1つにつき1行）に保存されます。

特定の行のバージョンを特定し取得する方法の1つとして、行に含まれる内容のハッシュをそのテーブル行のUUIDとして使用することが考えられます。

以下の図は、何を示すか：
- 内容変更時の行の挿入順序（例えば、テキストエリアにテキストを入力するキー入力による変更）
- `PRIMARY KEY (hash)`が使用されたときに挿入された行からのデータのディスク上での順序：

<img src={require('./images/sparse-primary-indexes-15a.png').default} class="image"/>

`hash`カラムがプライマリキーのカラムとして使用されているため、
- 特定の行を非常にすばやく[検索できます](#the-primary-index-is-used-for-selecting-granules)。 しかし
- テーブルの行（カラムデータ）は、（一意でランダムな）ハッシュ値によってディスク上の順序づけがされるため、contentカラムの値もランダム順で保存され、データの局所性がないためcontentカラムのデータファイルの圧縮比が最適化されていません。

データの圧縮比を大幅に改善し、特定の行の高速検索を実現するために、`pastila.nl`は特定の行を特定するために、2つのハッシュ（およびコンパウンドプライマリキー）を使用しています：
- ハッシュデータとは異なり、各データに対して固有のハッシュが設定されます。
- 小さなデータ変更で変化しない[局所性に敏感なハッシュ（フィンガープリント）](https://en.wikipedia.org/wiki/Locality-sensitive_hashing)

以下の図は、何を示すか：
- 内容変更時の行の挿入順序（例えば、テキストエリアにテキストを入力するキー入力による変更）
- コンパウンド`PRIMARY KEY (fingerprint, hash)`が使用されたときに挿入された行からのデータのディスク上での順序：

<img src={require('./images/sparse-primary-indexes-15b.png').default} class="image"/>

行は`fingerprint`によってまず順序づけられ、同じfingerprintを持つ行では`hash`によって最終的な順序が決まります。

僅かなデータ変更のみで同じfingerprintが生成されるため、似たデータがcontentカラムのディスク上で隣り合って保存されます。圧縮アルゴリズムは一般にデータの局所性の恩恵を受け（データが似ていれば似ているほど圧縮比が良くなる）、contentカラムの圧縮比に非常に有効です。

妥協としては、2つのフィールド（`fingerprint` および `hash`）を使用して特定の行を検索し、コンパウンド`PRIMARY KEY (fingerprint, hash)`によってプライマリインデックスを最適に利用することです。
