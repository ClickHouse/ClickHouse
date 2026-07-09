---
description: 'このテーブル関数を使用すると、ClickHouse を Redis と統合できます。'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

このテーブル関数を使用すると、ClickHouse を [Redis](https://redis.io/) と統合できます。

<div id="syntax">
  ## 構文
</div>

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

<div id="arguments">
  ## 引数
</div>

| 引数          | 説明                                                                      |
| ----------- | ----------------------------------------------------------------------- |
| `host:port` | Redis サーバーのアドレスです。ポートは省略でき、省略した場合はデフォルトの Redis ポート 6379 が使用されます。        |
| `key`       | カラムリスト内の任意のカラム名です。                                                      |
| `structure` | この関数が返す ClickHouseテーブルのスキーマです。                                          |
| `db_index`  | Redis DB のインデックスです。範囲は 0 から 15 で、デフォルトは 0 です。                           |
| `password`  | ユーザーパスワードです。デフォルトは空文字列です。                                               |
| `pool_size` | Redis の最大接続プールサイズです。デフォルトは 16 です。                                       |
| `primary`   | 指定は必須です。主キーとしてサポートされるのは 1 つのカラムのみです。主キーは Redis のキーとしてバイナリ形式でシリアライズされます。 |

* 主キー以外のカラムは、対応する順序で Redis の値としてバイナリ形式でシリアライズされます。
* `key` に対する `equals` または `in` 条件でのフィルタリングを含むクエリは、Redis に対する複数キーのルックアップに最適化されます。`key` によるフィルタリングがないクエリではフルテーブルスキャンが発生し、これは負荷の高い操作です。

[Named collections](/ja/operations/named-collections.md) は、現時点では `redis` テーブル関数ではサポートされていません。

<div id="returned_value">
  ## 戻り値
</div>

キーをRedisキーとし、その他のカラムをまとめてRedisの値にしたテーブルオブジェクト。

<div id="usage-example">
  ## 使用例
</div>

Redis から読み込む:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Redisへの挿入:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

<div id="related">
  ## 関連
</div>

* [Redis テーブルエンジン](/ja/engines/table-engines/integrations/redis.md)
* [Dictionary ソースとして Redis を使用する](/ja/sql-reference/statements/create/dictionary/sources/redis)