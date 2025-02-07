---
slug: /ja/engines/table-engines/integrations/redis
sidebar_position: 175
sidebar_label: Redis
---

# Redis

このエンジンは、ClickHouseを[Redis](https://redis.io/)と統合することを可能にします。Redisはkvモデルを採用しているため、`where k=xx` や `where k in (xx, xx)` のように、特定の方法でクエリを行うことを強くお勧めします。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**エンジンパラメータ**

- `host:port` — Redisサーバーのアドレスです。ポートを無視できます。デフォルトはRedisのポート6379が使用されます。
- `db_index` — Redisのデータベースインデックス範囲は0から15までです。デフォルトは0です。
- `password` — ユーザーのパスワードです。デフォルトは空の文字列です。
- `pool_size` — Redisの最大接続プールサイズです。デフォルトは16です。
- `primary_key_name` - カラムリスト内の任意のカラム名です。

:::note シリアル化
`PRIMARY KEY` は1つのカラムのみをサポートします。主キーはRedisキーとしてバイナリでシリアル化されます。
主キー以外のカラムはRedis値として対応する順序でバイナリでシリアル化されます。
:::

引数は[named collections](/docs/ja/operations/named-collections.md)を使用して渡すこともできます。この場合、`host` と `port` は別々に指定する必要があります。この方法は本番環境で推奨されます。現在、Redisにnamed collectionsを使用して渡されるすべてのパラメータは必須です。

:::note フィルタリング
`key equals` や `in filtering` を使用したクエリは、Redisからのマルチキー検索に最適化されます。フィルタリングキーなしのクエリでは、すべてのテーブルをスキャンすることになりますが、これは重い操作です。
:::

## 使用例 {#usage-example}

`Redis`エンジンを使用してClickHouseにテーブルを作成する例:

``` sql
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

または[named collections](/docs/ja/operations/named-collections.md)を使用する場合:

```
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>s0</db_index>
    </redis_creds>
</named_collections>
```

```sql
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

挿入:

```sql
INSERT INTO redis_table Values('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

クエリ:

``` sql
SELECT COUNT(*) FROM redis_table;
```

``` text
┌─count()─┐
│       2 │
└─────────┘
```

``` sql
SELECT * FROM redis_table WHERE key='1';
```

```text
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

``` sql
SELECT * FROM redis_table WHERE v1=2;
```

```text
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

更新:

主キーは更新できないことに注意してください。

```sql
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

削除:

```sql
ALTER TABLE redis_table DELETE WHERE key='1';
```

削除:

Redisデータベースを非同期的にフラッシュします。また、`Truncate`はSYNCモードをサポートしています。

```sql
TRUNCATE TABLE redis_table SYNC;
```

結合:

他のテーブルと結合します。

```
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

## 制限 {#limitations}

Redisエンジンは`where k > xx`のようなスキャンクエリもサポートしますが、いくつかの制限があります:
1. スキャンクエリは、再ハッシング中に非常に稀に重複したキーを生成する場合があります。[Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269)の詳細を参照してください。
2. スキャン中にキーが作成または削除される可能性があり、結果として得られるデータセットは有効な時点を表すことができません。
