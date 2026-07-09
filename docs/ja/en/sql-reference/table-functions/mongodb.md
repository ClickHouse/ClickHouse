---
description: 'リモートの MongoDB サーバーに保存されているデータに対して、`SELECT` クエリを実行できます。'
sidebar_label: 'mongodb'
sidebar_position: 135
slug: /sql-reference/table-functions/mongodb
title: 'mongodb'
doc_type: 'reference'
---

リモートの MongoDB サーバーに保存されているデータに対して、`SELECT` クエリを実行できます。

<div id="syntax">
  ## 構文
</div>

```sql
mongodb(host:port, database, collection, user, password, structure[, options[, oid_columns]]);
mongodb(uri, collection, structure[, oid_columns]);
mongodb(named_collection_name[, <arg>=<value>...]);
```

<div id="arguments">
  ## 引数
</div>

| 引数            | 説明                                                |
| ------------- | ------------------------------------------------- |
| `host:port`   | MongoDBサーバーのアドレス。                                 |
| `database`    | リモートデータベース名。                                      |
| `collection`  | リモートコレクション名。                                      |
| `user`        | MongoDBユーザー。                                      |
| `password`    | ユーザーのパスワード。                                       |
| `structure`   | この関数が返すClickHouseテーブルのスキーマ。                       |
| `options`     | MongoDB接続文字列のオプション (省略可能なパラメータ) 。                 |
| `oid_columns` | WHERE句で `oid` として扱うカラムのカンマ区切りリスト。デフォルトは `_id` です。 |

:::tip
MongoDB Atlas のクラウドサービスを使用している場合は、次のオプションを追加してください:

```ini
'connectTimeoutMS=10000&ssl=true&authSource=admin'
```

:::

URI を使用して接続することもできます。

```sql
mongodb(uri, collection, structure[, oid_columns])
```

| 引数            | 説明                                                |
| ------------- | ------------------------------------------------- |
| `uri`         | 接続文字列。                                            |
| `collection`  | リモートのコレクション名。                                     |
| `structure`   | この関数が返す ClickHouse テーブルのスキーマ。                     |
| `oid_columns` | WHERE句で `oid` として扱うカラムのカンマ区切りリスト。デフォルトは `_id` です。 |
| :::           |                                                   |

名前付きコレクションを使用して引数を渡すこともできます:

```sql
mongodb(_named_collection_[, host][, port][, database][, collection][, user][, password][, structure][, options][, oid_columns])
-- or
mongodb(_named_collection_[, uri][, structure][, oid_columns])
```

<div id="returned_value">
  ## 戻り値
</div>

元のMongoDBテーブルと同じカラムを持つテーブルオブジェクト。

<div id="examples">
  ## 例
</div>

`test` という名前の MongoDB データベースに `my_collection` という名前のコレクションがあり、いくつかのドキュメントを挿入するとします。

```sql
db.createUser({user:"test_user",pwd:"password",roles:[{role:"readWrite",db:"test"}]})

db.createCollection("my_collection")

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.9", command: "check-cpu-usage -w 75 -c 90" }
)

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.4", command: "system-check"}
)
```

`mongodb` テーブル関数を使用して、コレクションに対してクエリを実行してみましょう：

```sql
SELECT * FROM mongodb(
    '127.0.0.1:27017',
    'test',
    'my_collection',
    'test_user',
    'password',
    'log_type String, host String, command String',
    'connectTimeoutMS=10000'
)
```

または:

```sql
SELECT * FROM mongodb(
    'mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
    'my_collection',
    'log_type String, host String, command String'
)
```

または:

```sql
CREATE NAMED COLLECTION mongo_creds AS
       uri='mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
       collection='default_collection';

SELECT * FROM mongodb(
        mongo_creds,
        collection = 'my_collection',
        structure = 'log_type String, host String, command String'
)
```

<div id="related">
  ## 関連
</div>

* [`MongoDB` テーブルエンジン](/ja/engines/table-engines/integrations/mongodb.md)
* [`MongoDB` を Dictionary ソースとして使う](../statements/create/dictionary/sources/mongodb.md)