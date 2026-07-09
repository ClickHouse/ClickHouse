---
description: 'このエンジンを使用すると、ClickHouse を Redis と統合できます。'
sidebar_label: 'Redis'
sidebar_position: 175
slug: /engines/table-engines/integrations/redis
title: 'Redis テーブルエンジン'
doc_type: 'guide'
---

このエンジンを使用すると、ClickHouse を [Redis](https://redis.io/) と統合できます。Redis はキー・バリュー型のモデルを採用しているため、`where k=xx` や `where k in (xx, xx)` のようなポイントクエリのみを行うことを強く推奨します。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**エンジンパラメータ**

* `host:port` — Redis サーバーのアドレスです。ポートは省略可能で、その場合はデフォルトの Redis ポート 6379 が使用されます。
* `db_index` — Redis DB のインデックスです。範囲は 0 から 15 で、デフォルトは 0 です。
* `password` — ユーザーパスワードです。デフォルトは空文字列です。
* `pool_size` — Redis の最大接続プールサイズです。デフォルトは 16 です。
* `primary_key_name` - カラム一覧内の任意のカラム名です。

:::note シリアライゼーション
`PRIMARY KEY` でサポートされるのは 1 つのカラムのみです。主キーは Redis のキーとしてバイナリ形式でシリアライズされます。
主キー以外のカラムは、対応する順序で Redis の値としてバイナリ形式でシリアライズされます。
:::

引数は [named collections](/ja/operations/named-collections.md) を使用して渡すこともできます。この場合、`host` と `port` は別々に指定する必要があります。この方法は本番環境での利用に推奨されます。現時点では、named collections を使用して Redis に渡すすべてのパラメータが必須です。

:::note フィルタリング
`key equals` または `in` によるフィルタリングを含むクエリは、Redis に対する複数キーのルックアップに最適化されます。フィルタリングキーを指定しないクエリではフルテーブルスキャンが発生し、これは負荷の高い操作です。
:::

<div id="usage-example">
  ## 使用例
</div>

通常の引数を使用して、`Redis` エンジンを使った ClickHouse のテーブルを作成します：

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

または、[named collections](/ja/operations/named-collections.md) を使用します：

```xml
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>0</db_index>
    </redis_creds>
</named_collections>
```

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

インサート:

```sql title="Query"
INSERT INTO redis_table VALUES('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

```sql title="Query"
SELECT COUNT(*) FROM redis_table;
```

```text title="Response"
┌─count()─┐
│       2 │
└─────────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE key='1';
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE v1=2;
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

更新：

主キーは更新できない点に注意してください。

```sql title="Query"
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

削除：

```sql title="Query"
ALTER TABLE redis_table DELETE WHERE key='1';
```

Truncate:

Redis DB を非同期でフラッシュします。`Truncate` は SYNC モードにも対応しています。

```sql title="Query"
TRUNCATE TABLE redis_table SYNC;
```

結合:

他のテーブルとの結合。

```sql title="Query"
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

<div id="limitations">
  ## 制限事項
</div>

Redis エンジンは、`where k > xx` のようなスキャンクエリもサポートしていますが、いくつか制限があります。

1. スキャンクエリでは、リハッシュ中のごくまれなケースで、同じキーが重複して返されることがあります。詳細は [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269) を参照してください。
2. スキャン中にキーが作成・削除される可能性があるため、結果として得られるデータセットは、ある時点の整合した状態を表すものにはなりません。