---
description: 'MongoDB エンジンは、リモートのコレクションからデータを読み取れる読み取り専用のテーブルエンジンです。'
sidebar_label: 'MongoDB'
sidebar_position: 135
slug: /engines/table-engines/integrations/mongodb
title: 'MongoDB テーブルエンジン'
doc_type: 'リファレンス'
---

MongoDB エンジンは、リモートの [MongoDB](https://www.mongodb.com/) コレクションからデータを読み取れる読み取り専用のテーブルエンジンです。

MongoDB v3.6+ サーバーのみをサポートしています。
[シードリスト(`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list) はまだサポートされていません。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password[, options[, oid_columns]]);
```

**エンジンパラメータ**

| Parameter     | Description                                                                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host:port`   | MongoDBサーバーのアドレス。                                                                                                                                                        |
| `database`    | リモートデータベース名。                                                                                                                                                             |
| `collection`  | リモートコレクション名。                                                                                                                                                             |
| `user`        | MongoDBユーザー。                                                                                                                                                             |
| `password`    | ユーザーのパスワード。                                                                                                                                                              |
| `options`     | 任意。URL形式の文字列として指定する、MongoDB接続文字列の[options](https://www.mongodb.com/docs/manual/reference/connection-string-options/#connection-options)。例: `'authSource=admin&ssl=true'` |
| `oid_columns` | WHERE句で `oid` として扱うカラムのカンマ区切りリスト。デフォルトは `_id` です。                                                                                                                        |

:::tip
MongoDB Atlas のクラウド提供を使用している場合、接続URLは &#39;Atlas SQL&#39; オプションから取得できます。
シードリスト(`mongodb**+srv**`) はまだサポートされていませんが、今後のリリースで追加される予定です。
:::

または、URI を渡すこともできます。

```sql
ENGINE = MongoDB(uri, collection[, oid_columns]);
```

**エンジンパラメータ**

| Parameter     | Description                                     |
| ------------- | ----------------------------------------------- |
| `uri`         | MongoDB サーバーの接続 URI。                            |
| `collection`  | リモートコレクションの名前。                                  |
| `oid_columns` | WHERE 句で `oid` として扱うカラムのカンマ区切りリスト。デフォルトは `_id`。 |

<div id="types-mappings">
  ## 型のマッピング
</div>

| MongoDB                 | ClickHouse                                          |
| ----------------------- | --------------------------------------------------- |
| bool, int32, int64      | *Decimals を除く任意の数値型*、Boolean、String                 |
| double                  | Float64、String                                      |
| date                    | Date、Date32、DateTime、DateTime64、String              |
| string                  | String、*正しくフォーマットされていれば任意の数値型 (Decimals を除く)&#x20;* |
| document                | String (JSON として)                                   |
| array                   | Array、String (JSON として)                             |
| oid                     | String                                              |
| binary                  | カラム内では String、配列または document 内では base64 エンコードされた文字列 |
| uuid (binary subtype 4) | UUID                                                |
| *その他*                   | String                                              |

MongoDB の document にキーが見つからない場合 (たとえばカラム名が一致しない場合) 、デフォルト値、またはカラムが Nullable の場合は `NULL` が挿入されます。

<div id="oid">
  ### OID
</div>

`String` を WHERE 句で `oid` として扱いたい場合は、テーブルエンジンの最後の引数にそのカラム名を指定するだけです。
これは、MongoDB では `_id` カラムがデフォルトで `oid` 型になっているため、`_id` カラムでレコードをクエリする際に必要になることがあります。
テーブル内の `_id` フィールドが `uuid` など別の型である場合は、空の `oid_columns` を指定する必要があります。そうしないと、このパラメータのデフォルト値である `_id` が使われます。

```javascript
db.sample_oid.insertMany([
    {"another_oid_column": ObjectId()},
]);

db.sample_oid.find();
[
    {
        "_id": {"$oid": "67bf6cc44ebc466d33d42fb2"},
        "another_oid_column": {"$oid": "67bf6cc40000000000ea41b1"}
    }
]
```

デフォルトでは、`_id` のみが `oid` カラムとして扱われます。

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid');

SELECT count() FROM sample_oid WHERE _id = '67bf6cc44ebc466d33d42fb2'; --will output 1.
SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; --will output 0
```

この場合、出力は `0` になります。これは、ClickHouse が `another_oid_column` を `oid` 型として認識していないためです。では、これを修正しましょう。

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid', '_id,another_oid_column');

-- or

CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('host', 'db', 'sample_oid', 'user', 'pass', '', '_id,another_oid_column');

SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; -- will output 1 now
```

<div id="supported-clauses">
  ## サポートされる句
</div>

サポートされるのは、単純な式を含むクエリのみです (たとえば、`WHERE field = <constant> ORDER BY field2 LIMIT <constant>`) 。
このような式は MongoDB クエリ言語に変換され、サーバー側で実行されます。
[mongodb&#95;throw&#95;on&#95;unsupported&#95;query](../../../operations/settings/settings.md#mongodb_throw_on_unsupported_query) を使用すると、これらの制限をすべて無効にできます。
その場合、ClickHouse はベストエフォートでクエリの変換を試みますが、フルテーブルスキャンや ClickHouse 側での処理が発生する可能性があります。

:::note
Mongo では厳密な型付きフィルターが必要になるため、リテラルの型は常に明示的に指定することをおすすめします。
たとえば、`Date` でフィルタリングする場合:

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```

Mongo は文字列を `Date` にキャストしないため、この方法は機能しません。そのため、手動でキャストする必要があります。

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```

これは `Date`、`Date32`、`DateTime`、`Bool`、`UUID` に適用されます。

:::

<div id="usage-example">
  ## 使用例
</div>

MongoDB に [sample&#95;mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix) データセットがロードされていることを前提とします

MongoDB のコレクションからデータを読み取れる ClickHouse のテーブルを作成します:

```sql title="Query"
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
    year String
) ENGINE = MongoDB('mongodb://<USERNAME>:<PASSWORD>@atlas-sql-6634be87cefd3876070caf96-98lxs.a.query.mongodb.net/sample_mflix?ssl=true&authSource=admin', 'movies');
```

```sql title="Query"
SELECT count() FROM sample_mflix_table
```

```text title="Response"
   ┌─count()─┐
1. │   21349 │
   └─────────┘
```

```sql title="Query"
-- JSONExtractString cannot be pushed down to MongoDB
SET mongodb_throw_on_unsupported_query = 0;

-- Find all 'Back to the Future' sequels with rating > 7.5
SELECT title, plot, genres, directors, released FROM sample_mflix_table
WHERE title IN ('Back to the Future', 'Back to the Future Part II', 'Back to the Future Part III')
    AND toFloat32(JSONExtractString(imdb, 'rating')) > 7.5
ORDER BY year
FORMAT Vertical;
```

```text title="Response"
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

```sql title="Query"
-- Find top 3 movies based on Cormac McCarthy's books
SELECT title, toFloat32(JSONExtractString(imdb, 'rating')) AS rating
FROM sample_mflix_table
WHERE arrayExists(x -> x LIKE 'Cormac McCarthy%', writers)
ORDER BY rating DESC
LIMIT 3;
```

```text title="Response"
   ┌─title──────────────────┬─rating─┐
1. │ No Country for Old Men │    8.1 │
2. │ The Sunset Limited     │    7.4 │
3. │ The Road               │    7.3 │
   └────────────────────────┴────────┘
```

<div id="troubleshooting">
  ## トラブルシューティング
</div>

生成された MongoDB クエリは、DEBUG レベルのログで確認できます。

実装の詳細については、[mongocxx](https://github.com/mongodb/mongo-cxx-driver) と [mongoc](https://github.com/mongodb/mongo-c-driver) のドキュメントを参照してください。