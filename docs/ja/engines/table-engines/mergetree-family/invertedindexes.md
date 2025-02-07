---
slug: /ja/engines/table-engines/mergetree-family/invertedindexes
sidebar_label: テキスト検索インデックス
description: テキスト内の検索語を迅速に見つけます。
keywords: [フルテキスト検索, テキスト検索, インデックス, インデックス]
---

# フルテキスト検索を使用したテキスト検索インデックス [エクスペリメンタル]

フルテキストインデックスは、[二次インデックス](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#available-types-of-indices)のエクスペリメンタルなタイプであり、[String](/docs/ja/sql-reference/data-types/string.md)または[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)カラムの高速テキスト検索機能を提供します。フルテキストインデックスの主なアイデアは、「用語」からこれらの用語を含む行へのマッピングを保持することです。「用語」は文字列カラムのトークン化されたセルです。例えば、文字列セル「I will be a little late」は、デフォルトで6つの用語「I」、「will」、「be」、「a」、「little」、「late」にトークン化されます。別のトークン化手法としてはn-gramがあります。例えば、3-gramトークン化の結果は、「I w」、「wi」、「wil」、「ill」、「ll」、「l b」、「 b」など21の用語になります。入力文字列が細かくトークン化されるほど、生成されるフルテキストインデックスは大きくなりますが、より有用になります。

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/O_MnyUkrIq8"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

:::note
フルテキストインデックスはエクスペリメンタルであり、まだ本番環境で使用するべきではありません。こちらは今後、後方互換性のない方法で変更される可能性があります。例えば、そのDDL/DQL構文やパフォーマンス/圧縮特性に関してです。
:::

## 使い方

フルテキストインデックスを使用するには、まず設定でそれらを有効にします:

```sql
SET allow_experimental_full_text_index = true;
```

フルテキストインデックスは次のような構文で文字列カラムに定義できます。

``` sql
CREATE TABLE tab
(
    `key` UInt64,
    `str` String,
    INDEX inv_idx(str) TYPE full_text(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
```

:::note
初期のClickHouseバージョンでは、対応するインデックスタイプ名は`inverted`でした。
:::

ここで`N`はトークナイザを指定します：

- `full_text(0)`（短くして`full_text()`でも可）はトークナイザを「トークン」に設定し、スペースで文字列を分割します。
- `full_text(N)`で`N`が2から8の間の場合、トークナイザを「ngrams(N)」に設定します。

ポスティングリストごとの最大行数は2番目のパラメータで指定できます。このパラメータは、巨大なポスティングリストファイルの生成を防ぐために使用できます。以下のバリエーションがあります：

- `full_text(ngrams, max_rows_per_postings_list)`: 指定されたmax_rows_per_postings_listを使用（0でない場合）
- `full_text(ngrams, 0)`: ポスティングリストごとの最大行数に制限なし
- `full_text(ngrams)`: デフォルトの最大行数64Kを使用

スキッピングインデックスの一種として、フルテキストインデックスは、テーブル作成後にカラムに追加または削除できます：

``` sql
ALTER TABLE tab DROP INDEX inv_idx;
ALTER TABLE tab ADD INDEX inv_idx(s) TYPE full_text(2);
```

インデックスを使用するために特別な関数や構文は必要ありません。典型的な文字列検索述語は自動的にインデックスを活用します。例として以下を考慮してください：

```sql
INSERT INTO tab(key, str) values (1, 'Hello World');
SELECT * from tab WHERE str == 'Hello World';
SELECT * from tab WHERE str IN ('Hello', 'World');
SELECT * from tab WHERE str LIKE '%Hello%';
SELECT * from tab WHERE multiSearchAny(str, ['Hello', 'World']);
SELECT * from tab WHERE hasToken(str, 'Hello');
```

フルテキストインデックスは、`Array(String)`、`Array(FixedString)`、`Map(String)`、`Map(String)`タイプのカラムでも機能します。

他の二次インデックスと同様に、各カラムパートは独自のフルテキストインデックスを持ちます。さらに、各フルテキストインデックスは内部的に「セグメント」に分割されています。セグメントの存在とサイズは一般的にユーザーに対して透明ですが、セグメントサイズはインデックス構築時のメモリ消費を決定します（例えば、2つのパートが結合されたとき）。設定パラメータ「max_digestion_size_per_segment」（デフォルトでは256 MB）は、新しいセグメントが作成される前に使用される基盤となるカラムから消費されるデータの量を制御します。パラメータを増やすとインデックス構築の中間メモリ消費が増えますが、検索パフォーマンスを改善し、平均的にクエリを評価するためにチェックするセグメント数が少なくなります。

## Hacker Newsデータセットのフルテキスト検索

大量のテキストを持つ大規模データセットでのフルテキストインデックスのパフォーマンス改善を見てみましょう。人気のあるHacker Newsウェブサイトのコメント28.7M行を使用します。フルテキストインデックスを持たないテーブルは以下のとおりです：

```sql
CREATE TABLE hackernews (
    id UInt64,
    deleted UInt8,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    children Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32
)
ENGINE = MergeTree
ORDER BY (type, author);
```

28.7M行はS3にあるParquetファイルに入っています - それらを`hackernews`テーブルに挿入しましょう：

```sql
INSERT INTO hackernews
	SELECT * FROM s3Cluster(
        'default',
        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet',
        'Parquet',
        '
    id UInt64,
    deleted UInt8,
    type String,
    by String,
    time DateTime,
    text String,
	dead UInt8,
	parent UInt64,
	poll UInt64,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32');
```

`comment`カラムに「ClickHouse」という用語（とその大文字小文字のバリエーション）を検索する以下のシンプルな検索を考えてみましょう：

```sql
SELECT count()
FROM hackernews
WHERE hasToken(lower(comment), 'clickhouse');
```

クエリの実行には3秒かかります：

```response
┌─count()─┐
│    1145 │
└─────────┘

1 row in set. Elapsed: 3.001 sec. Processed 28.74 million rows, 9.75 GB (9.58 million rows/s., 3.25 GB/s.)
```

`ALTER TABLE`を使用して`comment`カラムの小文字版にフルテキストインデックスを追加し、それをマテリアライズします（時間がかかるかもしれません - マテリアライズするのを待ちます）：

```sql
ALTER TABLE hackernews
     ADD INDEX comment_lowercase(lower(comment)) TYPE full_text;

ALTER TABLE hackernews MATERIALIZE INDEX comment_lowercase;
```

同じクエリを実行すると...

```sql
SELECT count()
FROM hackernews
WHERE hasToken(lower(comment), 'clickhouse')
```

...クエリ実行時間が4倍速くなります：

```response
┌─count()─┐
│    1145 │
└─────────┘

1 row in set. Elapsed: 0.747 sec. Processed 4.49 million rows, 1.77 GB (6.01 million rows/s., 2.37 GB/s.)
```

また、複数の用語のうち一つまたは全てを検索することもできます。例えば、ORやAND条件です：

```sql
-- 複数のOR条件
SELECT count(*)
FROM hackernews
WHERE multiSearchAny(lower(comment), ['oltp', 'olap']);

-- 複数のAND条件
SELECT count(*)
FROM hackernews
WHERE hasToken(lower(comment), 'avx') AND hasToken(lower(comment), 'sve');
```

:::note
他の二次インデックスと異なり、フルテキストインデックスは（現状では）グラニュールIDではなく行番号（行ID）にマップされています。このデザインの理由はパフォーマンスによるものです。実際、ユーザーは複数の用語を一度に検索することが多いです。例えば、フィルタ条件`WHERE s LIKE '%little%' OR s LIKE '%big%'`は、「little」および「big」の行IDリストの合計を形成して、フルテキストインデックスを直接使用して評価できます。これは、インデックス作成時に指定された`GRANULARITY`パラメータに意味がないことを意味し、将来的に構文から削除される可能性があります。
:::

## 関連コンテンツ

- ブログ: [ClickHouseでのインバーテッドインデックスの紹介](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
