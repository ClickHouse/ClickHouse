---
slug: /ja/engines/table-engines/integrations/embedded-rocksdb
sidebar_position: 50
sidebar_label: EmbeddedRocksDB
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# EmbeddedRocksDB エンジン

<CloudNotSupportedBadge />

このエンジンは、ClickHouseを[RocksDB](http://rocksdb.org/)と統合することを可能にします。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

エンジンパラメータ：

- `ttl` - 値の有効期限（TTL）。TTLは秒単位で指定されます。TTLが0の場合、通常のRocksDBインスタンスが使用されます（TTLなし）。
- `rocksdb_dir` - 既存のRocksDBのディレクトリのパス、または作成されたRocksDBの目的地のパス。指定された`rocksdb_dir`でテーブルをオープンします。
- `read_only` - `read_only`がtrueに設定されている場合、読み取り専用モードが使用されます。TTLのあるストレージでは、手動および自動いずれのコンパクションもトリガーされないため、期限切れのエントリは削除されません。
- `primary_key_name` – カラムリストの任意のカラム名。
- `主キー`は必ず指定され、一つのカラムのみをサポートします。主キーはバイナリで`rocksdb key`としてシリアライズされます。
- 主キー以外のカラムは、対応する順序でバイナリで`rocksdb`値としてシリアライズされます。
- `equals`または`in`フィルタリングを使用したクエリは、`rocksdb`からのマルチキー検索に最適化されます。

エンジン設定：

- `optimize_for_bulk_insert` – テーブルはバルクインサートに最適化されています（インサートパイプラインはSSTファイルを作成し、memtablesへの書き込みの代わりにrocksdbデータベースにインポートします）。デフォルト値: `1`。
- `bulk_insert_block_size` - バルクインサートによって作成されるSSTファイルの最小サイズ（行単位）；デフォルト値: `1048449`。

例：

``` sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

## メトリクス

`system.rocksdb`テーブルも存在し、rocksdbの統計を公開しています：

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

## 設定

構成を使用して任意の[rocksdbオプション](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map)を変更することもできます：

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

デフォルトで簡易近似カウント最適化はオフになっており、`count()`クエリのパフォーマンスに影響を与える可能性があります。この最適化を有効にするには、`optimize_trivial_approximate_count_query = 1`を設定してください。また、この設定はEmbeddedRocksDBエンジンの`system.tables`にも影響を与え、`total_rows`および`total_bytes`の近似値を表示するために設定をオンにしてください。

## 対応操作 {#supported-operations}

### インサート

新しい行が`EmbeddedRocksDB`に挿入されると、キーが既に存在する場合は値が更新され、そうでなければ新しいキーが作成されます。

例：

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

### 削除

`DELETE`クエリや`TRUNCATE`を使用して行を削除できます。

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

### 更新

`ALTER TABLE`クエリを使用して値を更新できます。主キーは更新できません。

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

### ジョイン

EmbeddedRocksDBテーブルとの特別な`direct`ジョインがサポートされています。このダイレクトジョインは、メモリにハッシュテーブルを形成せず、EmbeddedRocksDBからデータを直接アクセスします。

大規模なジョインの場合、ダイレクトジョインによりメモリ使用量が大幅に低減されることがあります。なぜならハッシュテーブルが作成されないからです。

ダイレクトジョインを有効にするには：

```sql
SET join_algorithm = 'direct, hash'
```

:::tip
`join_algorithm`が`direct, hash`に設定されている場合、可能な場合はダイレクトジョインが使用され、それ以外はハッシュが使用されます。
:::

#### 例

##### EmbeddedRocksDBテーブルを作成し、データを挿入する：

```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) as key,
        [key, key+1] as value,
        ('val2' || toString(key)) as value2
    FROM numbers_mt(10);
```

##### テーブル`t2`を作成し、`rdb`テーブルとジョインするためにデータを挿入：

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

##### ジョインアルゴリズムを`direct`に設定：

```sql
SET join_algorithm = 'direct'
```

##### INNER JOIN：
```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```
```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

### ジョインに関する詳細情報
- [`join_algorithm`設定](/docs/ja/operations/settings/settings.md#join_algorithm)
- [JOIN句](/docs/ja/sql-reference/statements/select/join.md)
