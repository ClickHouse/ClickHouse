---
slug: /ja/sql-reference/table-functions/mongodb
sidebar_position: 135
sidebar_label: mongodb
---

# mongodb

リモートのMongoDBサーバーに保存されているデータに対して`SELECT`クエリを実行することができます。

**構文**

``` sql
mongodb(host:port, database, collection, user, password, structure [, options])
```

**引数**

- `host:port` — MongoDBサーバーのアドレス。

- `database` — リモートデータベース名。

- `collection` — リモートコレクション名。

- `user` — MongoDBユーザー。

- `password` — ユーザーパスワード。

- `structure` - この関数から返されるClickHouseテーブルのスキーマ。

- `options` - MongoDB接続文字列オプション（オプションのパラメータ）。

:::tip
MongoDB Atlasクラウドを使用している場合、次のオプションを追加してください：

```
'connectTimeoutMS=10000&ssl=true&authSource=admin'
```

:::

また、URIで接続することもできます：
``` sql
mongodb(uri, collection, structure)
```
**引数**

- `uri` — 接続文字列。

- `collection` — リモートコレクション名。

- `structure` — この関数から返されるClickHouseテーブルのスキーマ。

**返される値**

MongoDBのオリジナルテーブルと同じカラムを持つテーブルオブジェクト。


**例**

MongoDBデータベース`test`に定義されているコレクション`my_collection`があり、そこにいくつかのドキュメントを挿入するとします：

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

このコレクションを`mongodb`テーブル関数を使ってクエリしてみましょう：

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

または：

```sql
SELECT * FROM mongodb(
    'mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
    'my_collection',
    'log_type String, host String, command String'
)
```

**関連項目**

- [MongoDBテーブルエンジン](/docs/ja/engines/table-engines/integrations/mongodb.md)
- [DictionaryソースとしてのMongoDBの利用](/docs/ja/sql-reference/dictionaries/index.md#mongodb)
