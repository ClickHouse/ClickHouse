---
description: 'このエンジンにより、ClickHouse を RocksDB と統合できます'
sidebar_label: 'EmbeddedRocksDB'
sidebar_position: 50
slug: /engines/table-engines/integrations/embedded-rocksdb
title: 'EmbeddedRocksDB テーブルエンジン'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="embeddedrocksdb-table-engine">
  # EmbeddedRocksDB テーブルエンジン
</div>

<CloudNotSupportedBadge />

このエンジンでは、ClickHouse を [RocksDB](http://rocksdb.org/) と統合できます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

エンジンパラメータ:

* `ttl` - 値の有効期限 (TTL) です。TTL は秒単位で指定します。TTL が 0 の場合は、通常の RocksDB インスタンスが使用されます (TTL なし) 。
* `rocksdb_dir` - 既存の RocksDB のディレクトリへのパス、または新しく作成される RocksDB の保存先パスです。テーブルは指定した `rocksdb_dir` で開かれます。
* `read_only` - `read_only` が true に設定されている場合、読み取り専用モードが使用されます。TTL を持つストレージでは compaction はトリガーされず (手動・自動のいずれでも) 、期限切れのエントリは削除されません。
* `primary_key_name` – カラム一覧内の任意のカラム名です。
* `primary key` は必須で、主キーとして指定できるのは 1 つのカラムのみです。主キーは `rocksdb key` としてバイナリ形式でシリアライズされます。
* 主キー以外のカラムは、対応する順序で `rocksdb` value としてバイナリ形式でシリアライズされます。
* キーに対する `equals` または `in` フィルタリングを含むクエリは、`rocksdb` からの複数キーのルックアップ向けに最適化されます。

エンジン設定:

* `optimize_for_bulk_insert` – テーブルは一括 insert 向けに最適化されます (insert pipeline は memtable に書き込む代わりに SST ファイルを作成し、rocksdb database に import します) 。デフォルト値: `1`。
* `bulk_insert_block_size` - 一括 insertion で作成される SST ファイルの最小サイズ (行数ベース) です。デフォルト値: `1048449`。

例:

```sql
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

<div id="metrics">
  ## メトリクス
</div>

RocksDB の統計情報を公開する `system.rocksdb` テーブルもあります。

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

<div id="configuration">
  ## 設定
</div>

config を使用して、任意の [RocksDB オプション](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) を変更することもできます。

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

デフォルトでは、単純な近似カウント最適化は無効になっており、`count()` クエリのパフォーマンスに影響する可能性があります。この最適化を有効にするには、`optimize_trivial_approximate_count_query = 1` を設定してください。また、この設定は EmbeddedRocksDB engine の `system.tables` にも影響し、有効にすると `total_rows` と `total_bytes` の概算値を確認できます。

<div id="supported-operations">
  ## サポートされる操作
</div>

<div id="inserts">
  ### 挿入
</div>

`EmbeddedRocksDB` に新しい行が挿入される際、キーがすでに存在する場合は値が更新され、存在しない場合は新しいキーが作成されます。

例:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### 削除
</div>

行は`DELETE`クエリまたは`TRUNCATE`で削除できます。

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

<div id="updates">
  ### 更新
</div>

`ALTER TABLE` クエリを使用して値を更新できます。主キーは更新できません。

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="joins">
  ### JOIN
</div>

EmbeddedRocksDB テーブルでは、特殊な `direct` join をサポートしています。
この `direct` join では、メモリ上に hash table を構築せず、
EmbeddedRocksDB から直接データにアクセスします。

大規模な join では、hash table を作成しないため、
`direct` join を使うことで memory usage を大幅に抑えられる場合があります。

`direct` join を有効にするには:

```sql
SET join_algorithm = 'direct, hash'
```

:::tip
`join_algorithm` が `direct, hash` に設定されている場合、可能であれば direct JOIN が使用され、それ以外の場合は hash が使用されます。
:::

<div id="example">
  #### 例
</div>

<div id="create-and-populate-an-embeddedrocksdb-table">
  ##### EmbeddedRocksDB テーブルを作成してデータを挿入する
</div>

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
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

<div id="create-and-populate-a-table-to-join-with-table-rdb">
  ##### table `rdb` と結合するためのテーブルを作成し、データを挿入する
</div>

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

<div id="set-the-join-algorithm-to-direct">
  ##### JOIN アルゴリズムを `direct` に設定する
</div>

```sql
SET join_algorithm = 'direct'
```

<div id="an-inner-join">
  ##### INNER JOIN
</div>

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

<div id="more-information-on-joins">
  ### JOIN の詳細情報
</div>

* [`join_algorithm` 設定](/ja/operations/settings/settings.md#join_algorithm)
* [JOIN 句](/ja/sql-reference/statements/select/join.md)