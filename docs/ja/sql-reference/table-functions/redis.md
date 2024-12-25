---
slug: /ja/sql-reference/table-functions/redis
sidebar_position: 170
sidebar_label: redis
---

# redis

このテーブル関数は、ClickHouseを[Redis](https://redis.io/)と統合するためのものです。

**構文**

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

**引数**

- `host:port` — Redisサーバーのアドレス。ポートを指定しない場合はデフォルトでRedisのポート6379が使用されます。

- `key` — カラムリスト内の任意のカラム名。

- `structure` — この関数により返されるClickHouseテーブルのスキーマ。

- `db_index` — Redisのデータベースインデックスで、範囲は0から15です。デフォルトは0です。

- `password` — ユーザーのパスワード。デフォルトは空文字列です。

- `pool_size` — Redisの最大接続プールサイズ。デフォルトは16です。

- `primary`は指定する必要があり、主キーには1つのカラムのみをサポートします。主キーはRedisキーとしてバイナリシリアライズされます。

- 主キー以外のカラムは、対応する順序でRedisの値としてバイナリシリアライズされます。

- キーが等しいかフィルタリングされているクエリは、Redisからのマルチキー検索に最適化されます。フィルタリングキーなしのクエリは、全テーブルスキャンが行われるため、重い操作になります。

現在、`redis`テーブル関数には[名前付きコレクション](/docs/ja/operations/named-collections.md)はサポートされていません。

**返される値**

Redisキーとしてのキーを持つテーブルオブジェクト。他のカラムはRedis値としてパッケージ化されます。

## 使用例 {#usage-example}

Redisから読み込む:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Redisに挿入する:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

**参照**

- [`Redis`テーブルエンジン](/docs/ja/engines/table-engines/integrations/redis.md)
- [Dictionaryソースとしてredisを使用する](/docs/ja/sql-reference/dictionaries/index.md#redis)
