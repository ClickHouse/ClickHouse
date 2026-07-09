---
description: 'このエンジンは、Amazon S3、Azure、HDFS、およびローカルに保存された既存の Apache Paimon
  テーブルに対する読み取り専用のインテグレーションを提供します。'
sidebar_label: 'Paimon'
sidebar_position: 95
slug: /engines/table-engines/integrations/paimon
title: 'Paimon テーブルエンジン'
doc_type: 'reference'
---

このエンジンは、Amazon S3、Azure、HDFS、およびローカルに保存された既存の Apache [Paimon](https://paimon.apache.org/) テーブルに対する読み取り専用のインテグレーションを提供します。
スナップショット読み取り、インクリメンタル読み取り、およびこのエンジンで提供される基本的なパーティションプルーニングをサポートしています。

<div id="create-table">
  ## テーブルの作成
</div>

Paimon テーブルは、ストレージ内にあらかじめ存在している必要がある点に注意してください。このコマンドでは、新しいテーブルを作成するための DDL パラメータは指定できません。
`Paimon*` テーブルの作成は `allow_experimental_paimon_storage_engine` で制御されており (既定では無効) 、`CREATE TABLE` を実行する前にこれを有効にしてください。

```sql
SET allow_experimental_paimon_storage_engine = 1;

CREATE TABLE paimon_table_s3
    ENGINE = PaimonS3(url,  [, access_key_id, secret_access_key] [,format] [,compression])

CREATE TABLE paimon_table_azure
    ENGINE = PaimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

CREATE TABLE paimon_table_hdfs
    ENGINE = PaimonHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE paimon_table_local
    ENGINE = PaimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## エンジン引数
</div>

各引数の説明は、それぞれ `S3`、`AzureBlobStorage`、`HDFS`、`File` エンジンの引数の説明に対応しています。
`format` は、Paimon テーブル のデータファイルのフォーマットを表します。

エンジンパラメータは [Named Collections](../../../operations/named-collections.md) を使用して指定できます

<div id="example">
  ### 例
</div>

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

named collections を使用する場合:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3(paimon_conf, filename = 'test_table')
```

<div id="capabilities">
  ## 機能
</div>

* 最新のテーブルスナップショットからスナップショット読み取りを行います。
* 有効にすると、コミット済みスナップショット ID に基づくインクリメンタル読み取りを行います。
* `use_paimon_partition_pruning` を有効にすると、パーティションプルーニングを行います。
* 設定されている場合、必要に応じてバックグラウンドでメタデータを refresh します。
* Atomic/Replicated データベースを使用している場合、テーブル UUID は安定しており、Keeper パスで `{uuid}` マクロを使用できます。

<div id="settings">
  ## 設定
</div>

このエンジンは、対応するオブジェクトストレージエンジンと同じ設定を使用し、さらに Paimon 固有の設定が追加されています。

* `allow_experimental_paimon_storage_engine` — `Paimon`、`PaimonS3`、`PaimonAzure`、`PaimonHDFS`、`PaimonLocal` テーブルエンジンの作成を有効にします。デフォルト: `0` (無効) 。
* `paimon_incremental_read` — インクリメンタル読み取りモードを有効にします。
* `paimon_metadata_refresh_interval_sec` — バックグラウンドでのメタデータのリフレッシュ間隔 (秒) 。0 より大きい値に設定すると、バックグラウンドタスクがオブジェクトストレージから最新のスナップショットとスキーマを定期的に取得します。デフォルト: 30。
* `paimon_keeper_path` — インクリメンタル読み取り状態用の Keeper パス。設定は必須で、テーブルごとに一意である必要があります。`{database}`、`{table}`、`{uuid}` などのマクロをサポートします。
* `paimon_replica_name` — インクリメンタル読み取り状態用のレプリカ名。設定は必須で、レプリカごとに一意である必要があります。`{replica}` などのマクロをサポートします。

<div id="incremental-read-examples">
  ## インクリメンタル読み取りの例
</div>

Keeper stateを使用したインクリメンタル読み取り:

```sql
CREATE TABLE paimon_inc
ENGINE = PaimonS3(paimon_conf, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/{database}/{uuid}',
    paimon_replica_name = '{replica}';
```

<div id="query-level-settings-for-incremental-read">
  ### インクリメンタル読み取りのクエリレベル設定
</div>

以下の設定は **クエリレベル** のものです (`CREATE TABLE` ではなく、`SELECT ... SETTINGS` で指定します) 。これらは、インクリメンタル読み取りのクエリごとの動作を制御します。

* `paimon_target_snapshot_id` — 指定したスナップショットの差分のみを読み取ります。Keeper 内のコミット済みウォーターマークは進まないため、同じスナップショットを何度でも再読み取りできます。デフォルト: `-1` (無効) 。
* `max_consume_snapshots` — 1 回のインクリメンタル読み取りで消費するスナップショットの最大数。ソースに未読のスナップショットが多数蓄積されている場合、この設定により 1 クエリあたりで消費する数を制限し、バッチサイズを制御できます。`0` は無制限を意味します。デフォルト: `0`。

**対象スナップショットの読み取り** — 現在のウォーターマークに関係なく、常にスナップショット 1 の差分を返します。

```sql
SELECT count()
FROM paimon_inc
SETTINGS paimon_target_snapshot_id = 1;
```

**バッチごとのスナップショット数を制限する** — 新しいスナップショットが3つ未処理の場合、1回のクエリで消費するのは最大2つまでにします。

```sql
SELECT count()
FROM paimon_inc
SETTINGS max_consume_snapshots = 2;
```

<div id="paimon-to-mergetree-via-refresh-mv">
  ## リフレッシャブルmaterialized view経由でPaimonからMergeTreeへ
</div>

`APPEND` モードのリフレッシャブルmaterialized viewを使用すると、PaimonテーブルからMergeTreeテーブルへデータを継続的に同期するエンドツーエンドのパイプラインを構築できます。各リフレッシュサイクルでは、Paimonから新しいインクリメンタルデータのみを読み取り、それを宛先テーブルに追加します。

**ステップ 1 — インクリメンタル読み取りとメタデータのリフレッシュを有効にして、Paimonソーステーブルを作成します。**

以下の例では `PaimonLocal` を使用しています。ストレージバックエンドに応じて、engineを `PaimonS3`、`PaimonAzure`、`PaimonHDFS`、または自動検出を行う `Paimon` engine に置き換えてください。

```sql
SET allow_experimental_paimon_storage_engine = 1;

-- Local storage
CREATE TABLE paimon_mv_source
ENGINE = PaimonLocal('/path/to/paimon/table')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;

-- S3 storage (the `Paimon` engine defaults to the S3 implementation when no `disk` is specified)
CREATE TABLE paimon_mv_source
ENGINE = Paimon('http://minio:9000/bucket/path/to/table', 'access_key', 'secret_key')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;
```

`paimon_metadata_refresh_interval_sec` は、バックグラウンドでのメタデータのリフレッシュ間隔を秒単位で設定します。0 より大きい値を指定すると、バックグラウンドタスクがオブジェクトストレージから最新のスナップショットとスキーマを定期的に取得するため、クエリによってメタデータ更新がトリガーされるのを待たずに、MV のリフレッシュサイクルで新たにコミットされたデータを認識できます。デフォルトは 30 です。オブジェクトストレージや Keeper の I/O が過剰にならないよう、多数のテーブルで使用する場合は注意してください。

**ステップ 2 — MergeTree の宛先テーブルを作成する (スキーマは Paimon テーブルから複製) :**

```sql
CREATE TABLE paimon_mv_dest AS paimon_mv_source
ENGINE = MergeTree()
ORDER BY tuple();
```

**ステップ 3 — リフレッシャブルmaterialized viewを作成します:**

```sql
CREATE MATERIALIZED VIEW paimon_mv
REFRESH EVERY 10 SECOND
APPEND
TO paimon_mv_dest
AS SELECT * FROM paimon_mv_source;
```

10秒ごとに MV が発火し、`SELECT * FROM paimon_mv_source` を実行します。これにより、前回のコミット済みスナップショット以降に追加された行のみが返され、それらが `paimon_mv_dest` に追記されます。

**クリーンアップ:**

```sql
SYSTEM STOP VIEW paimon_mv;
DROP VIEW IF EXISTS paimon_mv SYNC;
DROP TABLE IF EXISTS paimon_mv_dest SYNC;
DROP TABLE IF EXISTS paimon_mv_source SYNC;
```

:::note
バックグラウンド更新によってDDL操作がブロックされるのを防ぐため、MVを削除する前に停止してください。
:::

<div id="limitations">
  ## 制限事項
</div>

* インクリメンタル読み取りを使用するには、Keeper (ZooKeeper) が設定されている必要があります。
* インクリメンタル読み取りでは、`paimon_keeper_path` を設定し、テーブルごとに一意にする必要があります。
* `paimon_replica_name` は、同じ Keeper パス内でレプリカごとに一意である必要があります。
* インクリメンタル読み取りでは at-most-once 配信が使用されます。コミット済みスナップショットは、データが実際に消費される前、データファイルの収集時に進められます。ファイル収集後にクエリが失敗した場合、スキップされたスナップショットは再試行しても再読み取りされません。
* このテーブルエンジンは読み取り専用であり、データの変更はサポートされていません。
* インクリメンタル読み取りは、Paimon ソース内の過去データの削除には対応していません。アップストリームの Paimon データが削除または更新されても、ClickHouse の MergeTree 宛先テーブルにすでに書き込まれている対応する行は自動的には削除されません。古いデータをクリーンアップするには、MergeTree テーブルに対して `ALTER TABLE ... DELETE` を手動で実行する必要があります。

<div id="aliases">
  ## 別名
</div>

`Paimon` テーブルエンジンは、`disk` 設定からストレージバックエンドを自動検出し、それに応じて `PaimonS3`、`PaimonAzure`、または `PaimonLocal` を使い分けます。`disk` が指定されていない場合は、デフォルトで `PaimonS3` 実装が使用されます。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルへのパス。型: `LowCardinality(String)`。
* `_file` — ファイル名。型: `LowCardinality(String)`。
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`。ファイルサイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`。時刻が不明な場合、値は `NULL` です。
* `_etag` — ファイルの etag。型: `LowCardinality(String)`。etag が不明な場合、値は `NULL` です。

<div id="data-types-supported">
  ## サポートされているデータ型
</div>

| Paimon データ型                       | ClickHouse データ型           |
| --------------------------------- | ------------------------- |
| BOOLEAN                           | Int8                      |
| TINYINT                           | Int8                      |
| SMALLINT                          | Int16                     |
| INTEGER                           | Int32                     |
| BIGINT                            | Int64                     |
| FLOAT                             | Float32                   |
| DOUBLE                            | Float64                   |
| STRING,VARCHAR,BYTES,VARBINARY    | String                    |
| DATE                              | Date                      |
| TIME(p),TIME                      | Time(&#39;UTC&#39;)       |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | DateTime64                |
| TIMESTAMP(p)                      | DateTime64(&#39;UTC&#39;) |
| CHAR                              | FixedString(1)            |
| BINARY(n)                         | FixedString(n)            |
| DECIMAL(P,S)                      | Decimal(P,S)              |
| ARRAY                             | Array                     |
| MAP                               | Map                       |

<div id="partition-supported">
  ## サポートされるパーティション
</div>

Paimon のパーティションキーでサポートされるデータ型:

* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`