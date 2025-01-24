---
slug: /ja/engines/table-engines/integrations/mongodb
sidebar_position: 135
sidebar_label: MongoDB
---

# MongoDB

MongoDB エンジンはリモート [MongoDB](https://www.mongodb.com/) コレクションからデータを読み取るための読み取り専用テーブルエンジンです。

MongoDB v3.6+ サーバーのみがサポートされています。
[シードリスト(`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list)はまだサポートされていません。

:::note
トラブルが発生した場合は、問題を報告し、[従来の実装](../../../operations/server-configuration-parameters/settings.md#use_legacy_mongodb_integration)を試してみてください。ただし、これは非推奨であり、次のリリースで削除される予定ですのでご注意ください。
:::

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password [, options]);
```

**エンジンパラメータ**

- `host:port` — MongoDB サーバーのアドレス。

- `database` — リモートデータベース名。

- `collection` — リモートコレクション名。

- `user` — MongoDB ユーザー。

- `password` — ユーザーパスワード。

- `options` — MongoDB 接続文字列オプション（省略可能なパラメータ）。

:::tip
MongoDB Atlas クラウド提供の接続 URL は 'Atlas SQL' オプションから取得できます。シードリスト(`mongodb**+srv**`)はまだサポートされていませんが、将来のリリースで追加される予定です。
:::

また、URI を簡単に渡すこともできます：

``` sql
ENGINE = MongoDB(uri, collection);
```

**エンジンパラメータ**

- `uri` — MongoDB サーバーの接続 URI 

- `collection` — リモートコレクション名。

## 型のマッピング

| MongoDB            | ClickHouse                                                            |
|--------------------|-----------------------------------------------------------------------|
| bool, int32, int64 | *任意の数値型*, String                                                |
| double             | Float64, String                                                       |
| date               | Date, Date32, DateTime, DateTime64, String                            |
| string             | String, UUID                                                          |
| document           | String(as JSON)                                                       |
| array              | Array, String(as JSON)                                                |
| oid                | String                                                                |
| binary             | カラムにある場合は String、配列やドキュメントにある場合は base64 エンコードされた String|
| *その他*           | String                                                                |

キーが MongoDB ドキュメントに存在しない場合（例えば、カラム名が一致しない場合）、デフォルト値または `NULL`（カラムが nullable の場合）が挿入されます。

## サポートされている句

単純な式のクエリのみがサポートされています（例：`WHERE field = <constant> ORDER BY field2 LIMIT <constant>`）。こうした式は MongoDB クエリ言語に変換され、サーバー側で実行されます。
これらの制限をすべて無効にしたい場合は、[mongodb_throw_on_unsupported_query](../../../operations/settings/settings.md#mongodb_throw_on_unsupported_query)を使用してください。その場合、ClickHouse はベストエフォートでクエリを変換しようとしますが、フルテーブルスキャンや ClickHouse 側での処理につながる可能性があります。

:::note
リテラルの型を明示的に指定した方が良いです。これは Mongo が厳密型のフィルターを要求するためです。\
例えば `Date` でフィルターしたい場合：

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```

これは機能しません。なぜなら Mongo は文字列を `Date` にキャストしないためです。したがって、手動でキャストする必要があります：

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```

これは `Date`、`Date32`、`DateTime`、`Bool`、`UUID` に適用されます。

:::

## 使用例 {#usage-example}

MongoDB に [sample_mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix) データセットがロードされていると仮定します。

MongoDB コレクションからデータを読み取れる ClickHouse でテーブルを作成します：

``` sql
CREATE TABLE sample_mflix_table
(
    _id String,
    title String,
    plot String,
    genres Array(String),
    directors Array(String),
    writers Array(String),
    released Date,
    imdb String,
    year String,
) ENGINE = MongoDB('mongodb://<USERNAME>:<PASSWORD>@atlas-sql-6634be87cefd3876070caf96-98lxs.a.query.mongodb.net/sample_mflix?ssl=true&authSource=admin', 'movies');
```

クエリ:

``` sql
SELECT count() FROM sample_mflix_table
```

``` text
   ┌─count()─┐
1. │   21349 │
   └─────────┘
```

```SQL
-- JSONExtractString は MongoDB にはプッシュダウンできません
SET mongodb_throw_on_unsupported_query = 0;

-- 評価が 7.5 を超える 'Back to the Future' シリーズの続編を見つける
SELECT title, plot, genres, directors, released FROM sample_mflix_table
WHERE title IN ('Back to the Future', 'Back to the Future Part II', 'Back to the Future Part III')
    AND toFloat32(JSONExtractString(imdb, 'rating')) > 7.5
ORDER BY year
FORMAT Vertical;
```

```text
Row 1:
──────
title:     Back to the Future
plot:      A young man is accidentally sent 30 years into the past in a time-traveling DeLorean invented by his friend, Dr. Emmett Brown, and must make sure his high-school-age parents unite in order to save his own existence.
genres:    ['Adventure','Comedy','Sci-Fi']
directors: ['Robert Zemeckis']
released:  1985-07-03

Row 2:
──────
title:     Back to the Future Part II
plot:      After visiting 2015, Marty McFly must repeat his visit to 1955 to prevent disastrous changes to 1985... without interfering with his first trip.
genres:    ['Action','Adventure','Comedy']
directors: ['Robert Zemeckis']
released:  1989-11-22
```

```SQL
-- Cormac McCarthy の本に基づいたトップ3の映画を探す
SELECT title, toFloat32(JSONExtractString(imdb, 'rating')) as rating
FROM sample_mflix_table
WHERE arrayExists(x -> x like 'Cormac McCarthy%', writers)
ORDER BY rating DESC
LIMIT 3;
```

```text
   ┌─title──────────────────┬─rating─┐
1. │ No Country for Old Men │    8.1 │
2. │ The Sunset Limited     │    7.4 │
3. │ The Road               │    7.3 │
   └────────────────────────┴────────┘
```

## トラブルシューティング
DEBUG レベルのログで生成された MongoDB クエリを見ることができます。

実装の詳細は [mongocxx](https://github.com/mongodb/mongo-cxx-driver) および [mongoc](https://github.com/mongodb/mongo-c-driver) のドキュメントに記載されています。
