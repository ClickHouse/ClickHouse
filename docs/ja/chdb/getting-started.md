---
title: chDBの使い始め
sidebar_label: 使い始め
slug: /ja/chdb/getting-started
description: chDBはClickHouseによってパワードされたプロセス内SQL OLAPエンジンです
keywords: [chdb, embedded, clickhouse-lite, in-process, in process]
---

# chDBの使い始め

このガイドでは、chDBのPythonバリアントをセットアップしていきます。まず、S3上のJSONファイルにクエリを実行し、それを基にchDBでテーブルを作成し、データに対するクエリを行います。さらに、クエリ結果をApache ArrowやPandasなど異なるフォーマットで取得する方法を学び、最終的にはPandas DataFrameに対してクエリを実行する方法を紹介します。

## セットアップ

まずは仮想環境を作成します:

```bash
python -m venv .venv
source .venv/bin/activate
```

次にchDBをインストールします。バージョン2.0.3以上であることを確認してください:

```bash
pip install "chdb>=2.0.2"
```

そして[ipython](https://ipython.org/)をインストールします:

```bash
pip install ipython
```

このガイドの残りでは`ipython`を使ってコマンドを実行します。以下のコマンドで`ipython`を起動できます:

```bash
ipython
```

さらに、PandasとApache Arrowを使用するので、それらのライブラリもインストールします:

```bash
pip install pandas pyarrow
```

## S3上にあるJSONファイルにクエリ

次に、S3バケットに保存されているJSONファイルに対してクエリを行う方法を見てみましょう。[YouTubeの非表示データセット](https://clickhouse.com/docs/ja/getting-started/example-datasets/youtube-dislikes)には、2021年までのYouTube動画の非表示数が4億以上含まれています。このデータセットのJSONファイルの1つを使っていきます。

chdbをインポートします:

```python
import chdb
```

次のクエリで、JSONファイルの構造を調べることができます:

```python
chdb.query(
  """
  DESCRIBE s3(
    's3://clickhouse-public-datasets/youtube/original/files/' ||
    'youtubedislikes_20211127161229_18654868.1637897329_vid.json.zst',
    'JSONLines'
  )
  SETTINGS describe_compact_output=1
  """
)
```

```text
"id","Nullable(String)"
"fetch_date","Nullable(String)"
"upload_date","Nullable(String)"
"title","Nullable(String)"
"uploader_id","Nullable(String)"
"uploader","Nullable(String)"
"uploader_sub_count","Nullable(Int64)"
"is_age_limit","Nullable(Bool)"
"view_count","Nullable(Int64)"
"like_count","Nullable(Int64)"
"dislike_count","Nullable(Int64)"
"is_crawlable","Nullable(Bool)"
"is_live_content","Nullable(Bool)"
"has_subtitles","Nullable(Bool)"
"is_ads_enabled","Nullable(Bool)"
"is_comments_enabled","Nullable(Bool)"
"description","Nullable(String)"
"rich_metadata","Array(Tuple(
    call Nullable(String),
    content Nullable(String),
    subtitle Nullable(String),
    title Nullable(String),
    url Nullable(String)))"
"super_titles","Array(Tuple(
    text Nullable(String),
    url Nullable(String)))"
"uploader_badges","Nullable(String)"
"video_badges","Nullable(String)"
```

また、そのファイル内の行数をカウントすることもできます:

```python
chdb.query(
  """
  SELECT count()
  FROM s3(
    's3://clickhouse-public-datasets/youtube/original/files/' ||
    'youtubedislikes_20211127161229_18654868.1637897329_vid.json.zst',
    'JSONLines'
  )"""
)
```

```text
336432
```

このファイルには30万以上のレコードが含まれています。

chDBはまだクエリパラメータの受け渡しをサポートしていませんが、パスを取り出してf-Stringを使って渡すことができます。

```python
path = 's3://clickhouse-public-datasets/youtube/original/files/youtubedislikes_20211127161229_18654868.1637897329_vid.json.zst'
```

```python
chdb.query(
  f"""
  SELECT count()
  FROM s3('{path}','JSONLines')
  """
)
```

:::warning
これはプログラム内で定義された変数を使用する限りでは問題ありませんが、ユーザーから提供された入力で行うと、クエリがSQLインジェクションに対して脆弱になります。
:::

## 出力フォーマットの設定

デフォルトの出力フォーマットは`CSV`ですが、`output_format`パラメータを使って変更できます。chDBはClickHouseのデータフォーマットに加え、`DataFrame`など[chDB独自のフォーマット](data-formats.md)をサポートしており、Pandas DataFrameを返します:

```python
result = chdb.query(
  f"""
  SELECT is_ads_enabled, count()
  FROM s3('{path}','JSONLines')
  GROUP BY ALL
  """,
  output_format="DataFrame"
)

print(type(result))
print(result)
```

```text
<class 'pandas.core.frame.DataFrame'>
   is_ads_enabled  count()
0           False   301125
1            True    35307
```

また、Apache Arrowテーブルを取得したい場合は:

```python
result = chdb.query(
  f"""
  SELECT is_live_content, count()
  FROM s3('{path}','JSONLines')
  GROUP BY ALL
  """,
  output_format="ArrowTable"
)

print(type(result))
print(result)
```

```text
<class 'pyarrow.lib.Table'>
pyarrow.Table
is_live_content: bool
count(): uint64 not null
----
is_live_content: [[false,true]]
count(): [[315746,20686]]
```

## JSONファイルからテーブルの作成

次に、chDBでテーブルを作成する方法を見てみましょう。これを行うには、異なるAPIを使用する必要がありますので、まずそれをインポートします:

```python
from chdb import session as chs
```

次に、セッションを初期化します。セッションをディスクに保存したい場合は、ディレクトリ名を指定する必要があります。これを空にすると、データベースはメモリ上に配置され、Pythonのプロセスを終了すると消去されます。

```python
sess = chs.Session("gettingStarted.chdb")
```

次に、データベースを作成します:

```python
sess.query("CREATE DATABASE IF NOT EXISTS youtube")
```

次に、JSONファイルのスキーマに基づいて`dislikes`テーブルを`CREATE...EMPTY AS`技法を使って作成します。カラムの型をすべて`Nullable`にしないように[`schema_inference_make_columns_nullable`](/docs/ja/operations/settings/formats/#schema_inference_make_columns_nullable)設定を使用します。

```python
sess.query(f"""
  CREATE TABLE youtube.dislikes
  ORDER BY fetch_date 
  EMPTY AS 
  SELECT * 
  FROM s3('{path}','JSONLines')
  SETTINGS schema_inference_make_columns_nullable=0
  """
)
```

その後、`DESCRIBE`句を使ってスキーマを確認できます:

```python
sess.query(f"""
   DESCRIBE youtube.dislikes
   SETTINGS describe_compact_output=1
   """
)
```

```text
"id","String"
"fetch_date","String"
"upload_date","String"
"title","String"
"uploader_id","String"
"uploader","String"
"uploader_sub_count","Int64"
"is_age_limit","Bool"
"view_count","Int64"
"like_count","Int64"
"dislike_count","Int64"
"is_crawlable","Bool"
"is_live_content","Bool"
"has_subtitles","Bool"
"is_ads_enabled","Bool"
"is_comments_enabled","Bool"
"description","String"
"rich_metadata","Array(Tuple(
    call String,
    content String,
    subtitle String,
    title String,
    url String))"
"super_titles","Array(Tuple(
    text String,
    url String))"
"uploader_badges","String"
"video_badges","String"
```

次に、そのテーブルにデータを挿入します:

```python
sess.query(f"""
  INSERT INTO youtube.dislikes
  SELECT * 
  FROM s3('{path}','JSONLines')
  SETTINGS schema_inference_make_columns_nullable=0
  """
)
```

これらのステップを1回の操作で行うこともできます。`CREATE...AS`技法を使って異なるテーブルを作成してみます:

```python
sess.query(f"""
  CREATE TABLE youtube.dislikes2
  ORDER BY fetch_date 
  AS 
  SELECT * 
  FROM s3('{path}','JSONLines')
  SETTINGS schema_inference_make_columns_nullable=0
  """
)
```

## テーブルへのクエリ

最後に、テーブルにクエリを実行します:

```sql
df = sess.query("""
  SELECT uploader, sum(view_count) AS viewCount, sum(like_count) AS likeCount, sum(dislike_count) AS dislikeCount
  FROM youtube.dislikes
  GROUP BY ALL
  ORDER BY viewCount DESC
  LIMIT 10
  """,
  "DataFrame"
)
df
```

```text
                             uploader  viewCount  likeCount  dislikeCount
0                             Jeremih  139066569     812602         37842
1                     TheKillersMusic  109313116     529361         11931
2  LetsGoMartin- Canciones Infantiles  104747788     236615        141467
3                    Xiaoying Cuisine   54458335    1031525         37049
4                                Adri   47404537     279033         36583
5                  Diana and Roma IND   43829341     182334        148740
6                      ChuChuTV Tamil   39244854     244614        213772
7                            Cheez-It   35342270        108            27
8                            Anime Uz   33375618    1270673         60013
9                    RC Cars OFF Road   31952962     101503         49489
```

例えば、次にDataFrameにlikesとdislikesの比率を計算するカラムを追加する場合、次のコードを書くことができます:

```python
df["likeDislikeRatio"] = df["likeCount"] / df["dislikeCount"]
```

## Pandas DataFrameへのクエリ

次に、そのDataFrameに対してchDBからクエリを実行できます:

```python
chdb.query(
  """
  SELECT uploader, likeDislikeRatio
  FROM Python(df)
  """,
  output_format="DataFrame"
)
```

```text
                             uploader  likeDislikeRatio
0                             Jeremih         21.473548
1                     TheKillersMusic         44.368536
2  LetsGoMartin- Canciones Infantiles          1.672581
3                    Xiaoying Cuisine         27.842182
4                                Adri          7.627395
5                  Diana and Roma IND          1.225857
6                      ChuChuTV Tamil          1.144275
7                            Cheez-It          4.000000
8                            Anime Uz         21.173296
9                    RC Cars OFF Road          2.051021
```

Pandas DataFrameへのクエリについての詳細は[Querying Pandas developer guide](guides/querying-pandas.md)で詳しく説明されています。

## 次のステップ

このガイドでchDBの概要が理解できたでしょうか。さらなる学習には、以下のデベロッパーガイドをご参照ください:

* [Pandas DataFrameへのクエリ](guides/querying-pandas.md)
* [Apache Arrowへのクエリ](guides/querying-apache-arrow.md)
* [JupySQLでのchDBの使用](guides/jupysql.md)
* [既存のclickhouse-localデータベースでのchDBの使用](guides/clickhouse-local.md)
