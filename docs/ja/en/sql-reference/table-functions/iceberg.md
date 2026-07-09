---
description: 'Amazon S3、Azure、HDFS、またはローカルに保存された Apache Iceberg テーブルに対する読み取り専用のテーブル形式インターフェイスを提供します。'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

Amazon S3、Azure、HDFS、またはローカルに保存された Apache [Iceberg](https://iceberg.apache.org/) テーブルに対する読み取り専用のテーブル形式インターフェイスを提供します。

<div id="syntax">
  ## 構文
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## 引数
</div>

引数の説明は、それぞれテーブル関数 `s3`、`azureBlobStorage`、`HDFS`、`file` の引数の説明と同じです。
`format` は、Iceberg テーブル内のデータファイルのフォーマットを表します。

`icebergS3` では、ClickHouse Cloud でロールベースのアクセスに使用する `role_arn` を渡すために、省略可能な `extra_credentials` パラメータを使用できます。設定手順については、[Secure S3](/ja/cloud/data-sources/secure-s3) を参照してください。

<div id="returned-value">
  ### 戻り値
</div>

指定された Iceberg テーブル内のデータを読み取るための、指定した構造を持つテーブルです。

<div id="example">
  ### 例
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse は現在、`icebergS3`、`icebergAzure`、`icebergHDFS`、`icebergLocal` のテーブル関数と、`IcebergS3`、`icebergAzure`、`IcebergHDFS`、`IcebergLocal` のテーブルエンジンを通じて、Iceberg フォーマットの v1 および v2 の読み取りをサポートしています。
:::

<div id="defining-a-named-collection">
  ## 名前付きコレクションの定義
</div>

以下は、URL と認証情報を保存するための名前付きコレクションを設定する例です。

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## データカタログの使用
</div>

Iceberg テーブルは、[REST Catalog](https://iceberg.apache.org/rest-catalog-spec/)、[AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html)、[Unity Catalog](https://www.unitycatalog.io/) など、さまざまなデータカタログで使用できます。

:::important
カタログを使用する場合、ほとんどのユーザーは `DataLakeCatalog` データベースエンジンを使用することになるでしょう。これは ClickHouse をカタログに接続し、テーブルを検出するためのものです。個々のテーブルを `IcebergS3` テーブルエンジンで手動作成する代わりに、このデータベースエンジンを使用できます。
:::

これらを使用するには、`IcebergS3` エンジンでテーブルを作成し、必要な設定を指定します。

たとえば、MinIO ストレージで REST Catalog を使用する場合:

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

または、S3 と AWS Glue Data Catalog を使用する場合:

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## スキーマ進化
</div>

現時点では、CH を使用して、時間の経過とともにスキーマが変化した Iceberg テーブルを読み込むことができます。現在は、カラムの追加・削除や順序の変更が行われたテーブルの読み取りをサポートしています。また、値が必須のカラムを NULL を許可するカラムに変更することもできます。さらに、単純型に対する許可された型変換、具体的には次のものをサポートしています。  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

現時点では、ネストされた構造や、配列およびマップ内の要素の型を変更することはできません。

<div id="partition-pruning">
  ## パーティションプルーニング
</div>

ClickHouse は、Iceberg テーブルに対する SELECT クエリでパーティションプルーニングをサポートしており、関連のないデータファイルをスキップすることでクエリパフォーマンスを最適化できます。パーティションプルーニングを有効にするには、`use_iceberg_partition_pruning = 1` を設定します。Iceberg のパーティション化とパーティションプルーニングの詳細については、https://iceberg.apache.org/spec/#partitioning を参照してください。

<div id="time-travel">
  ## タイムトラベル
</div>

ClickHouse は Iceberg テーブルでのタイムトラベルをサポートしており、特定のタイムスタンプまたはスナップショット ID を指定して、過去のデータをクエリできます。

<div id="deleted-rows">
  ## 削除済みの行を含むテーブルの処理
</div>

現在、[position deletes](https://iceberg.apache.org/spec/#position-delete-files) に対応した Iceberg テーブルのみがサポートされています。

以下の削除方法は**サポートされていません**。

* [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files)
* [Deletion vectors](https://iceberg.apache.org/spec/#deletion-vectors) (導入バージョン v3)

<div id="basic-usage">
  ### 基本的な使い方
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

注: 同一のクエリで `iceberg_timestamp_ms` と `iceberg_snapshot_id` の両方を指定することはできません。

<div id="important-considerations">
  ### 重要な考慮事項
</div>

* **スナップショット** は通常、次の場合に作成されます。

* 新しいデータがテーブルに書き込まれたとき

* 何らかのデータ コンパクション が実行されたとき

* **スキーマの変更では通常、スナップショットは作成されません** - そのため、スキーマ進化が行われたテーブルで タイムトラベル を使用する際には、重要な挙動が生じます。

<div id="example-scenarios">
  ### シナリオの例
</div>

CH はまだ Iceberg テーブルへの書き込みに対応していないため、すべてのシナリオは Spark を使って記述しています。

<div id="scenario-1">
  #### シナリオ 1: 新しいスナップショットがない場合のスキーマ変更
</div>

次の操作の流れを考えてみましょう。

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

異なるタイムスタンプでのクエリ結果:

* ts1 &amp; ts2: 元の2つのカラムのみが表示されます
* ts3: 3つすべてのカラムが表示され、最初の行の price は NULL になります

<div id="scenario-2">
  #### シナリオ 2: 履歴上のスキーマと現在のスキーマの違い
</div>

現在時点でタイムトラベルクエリを実行すると、現在のテーブルとは異なるスキーマが表示されることがあります。

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

これは、`ALTER TABLE` では新しいスナップショットが作成されず、現在のテーブルについては Spark がスナップショットではなく最新のメタデータファイルから `schema_id` の値を取得するために発生します。

<div id="scenario-3">
  #### シナリオ 3: 過去と現在のスキーマの差異
</div>

もう1つは、タイムトラベルでは、データが一度も書き込まれていない時点のテーブルの状態は取得できないことです:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

ClickHouse では、この動作は Spark と一貫しています。Spark の Select クエリを ClickHouse の Select クエリに置き換えて考えると、同じように動作します。

<div id="metadata-file-resolution">
  ## メタデータファイルの解決
</div>

ClickHouseで`iceberg`テーブル関数を使用する場合、システムはIcebergテーブル構造を記述した適切なmetadata.jsonファイルを特定する必要があります。この解決プロセスは次のように行われます:

<div id="candidate-search">
  ### 候補の検索 (優先順)
</div>

1. **直接パスの指定**:
   *`iceberg_metadata_file_path` を設定すると、システムはこの正確なパスを、Iceberg テーブルのディレクトリパスと組み合わせて使用します。

* この設定が指定されている場合、他のすべての解決設定は無視されます。

2. **テーブル UUID の照合**:
   *`iceberg_metadata_table_uuid` が指定されている場合、システムは次のように動作します:
   *`metadata` ディレクトリ内の `.metadata.json` ファイルのみを対象にします
   *指定した UUID と一致する `table-uuid` フィールドを含むファイルに絞り込みます (大文字と小文字を区別しない)

3. **デフォルト検索**:
   *上記いずれの設定も指定されていない場合、`metadata` ディレクトリ内のすべての `.metadata.json` ファイルが候補になります

<div id="most-recent-file">
  ### 最新のファイルを選択する
</div>

上記のルールを使って候補のファイルを特定した後、システムはどのファイルが最新かを判定します。

* `iceberg_recent_metadata_file_by_last_updated_ms_field` が有効な場合:

* `last-updated-ms` の値が最も大きいファイルが選択されます

* それ以外の場合:

* バージョン番号が最も大きいファイルが選択されます

* (バージョンは、`V.metadata.json` または `V-uuid.metadata.json` 形式のファイル名では `V` として表されます)

**注**: ここで言及している設定はすべてテーブル関数の設定であり (グローバル設定やクエリレベルの設定ではありません) 、以下のように指定する必要があります。

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**注意**: 通常、メタデータの解決は Iceberg カタログが担いますが、ClickHouse の `iceberg` テーブル関数は、S3 に保存されたファイルを Iceberg テーブルとして直接解釈します。そのため、これらの解決ルールを理解することが重要です。

<div id="metadata-cache">
  ## メタデータキャッシュ
</div>

`Iceberg` テーブルエンジンとテーブル関数は、マニフェストファイル、マニフェストリスト、メタデータ JSON の情報を保持するメタデータキャッシュをサポートしています。キャッシュはメモリ上に保存されます。この機能は設定 `use_iceberg_metadata_files_cache` で制御されており、デフォルトで有効です。

<div id="aliases">
  ## 別名
</div>

テーブル関数 `iceberg` は現在、`icebergS3` の別名です。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`.
* `_file` — ファイル名。型: `LowCardinality(String)`.
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`. ファイルサイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`. 時刻が不明な場合、値は `NULL` です。
* `_etag` — ファイルの etag。型: `LowCardinality(String)`. etag が不明な場合、値は `NULL` です。

<div id="writes-into-iceberg-table">
  ## Iceberg テーブル への書き込み
</div>

バージョン 25.7 以降、ClickHouse はユーザーの Iceberg テーブル の変更をサポートしています。

現在、これは実験的な機能であるため、まず有効にする必要があります:

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### テーブルの作成
</div>

独自の空の Iceberg テーブルを作成するには、読み取り時と同じコマンドを使用しますが、スキーマは明示的に指定します。
書き込みでは、Parquet、Avro、ORC など、Iceberg 仕様のすべてのデータフォーマットをサポートしています。

<div id="example">
  ### 例
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

注: バージョンヒントファイルを作成するには、`iceberg_use_version_hint` 設定を有効にします。
metadata.json ファイルを圧縮する場合は、`iceberg_metadata_compression_method` 設定でコーデック名を指定します。

<div id="writes-inserts">
  ### INSERT
</div>

新しいテーブルを作成したら、通常のClickHouse構文でデータを挿入できます。

<div id="example">
  ### 例
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

ClickHouse は、merge-on-read フォーマットで余分な行を削除することにも対応しています。
このクエリは、position delete files を含む新しいスナップショットを作成します。

<div id="example">
  ### 例
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### スキーマ進化
</div>

ClickHouse では、単純型 (Tuple、Array、Map 以外) のカラムを簡単に追加、削除、変更、またはリネームできます。

<div id="example">
  ### 例
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### コンパクション
</div>

ClickHouse は Iceberg テーブルのコンパクションをサポートしています。現在は、メタデータを更新しながら position delete files を data files にマージできます。以前の snapshot ID とタイムスタンプは変更されないため、タイムトラベル機能も引き続き同じ値で使用できます。

使用方法:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### スナップショットの失効
</div>

Icebergテーブルでは、`INSERT`、`DELETE`、または`UPDATE` を行うたびにスナップショットが蓄積されます。時間の経過とともに、これによりスナップショットや関連するデータファイルが大量に増えることがあります。`expire_snapshots` コマンドは、古いスナップショットを削除し、保持されているいずれのスナップショットからも参照されなくなったデータファイルをクリーンアップします。

**構文:**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

デフォルトでは、どのスナップショットを保持するかは [保持ポリシー](#iceberg-snapshot-retention-policy) (テーブルプロパティ `min-snapshots-to-keep`、`max-snapshot-age-ms`、および ref ごとの override) によって決まります。`snapshot_ids` を指定すると、保持ポリシーは適用されず、列挙したスナップショットだけが期限切れの対象として扱われます。

**引数:**

* `'timestamp'` (位置指定) または `expire_before = 'timestamp'` — **サーバーのタイムゾーン**で解釈される datetime 文字列 (例: `'2024-06-01 00:00:00'`) 。安全弁として機能し、`timestamp-ms` がこの値以上のスナップショットは、保持ポリシー上は期限切れになる場合でも保護されます。`snapshot_ids` と組み合わせることもでき、その場合は、指定した timestamp 以降の列挙済みスナップショットは期限切れになりません。
* `retention_period = '<duration>'` — この呼び出しに限り、テーブルレベルの `history.expire.max-snapshot-age-ms` を上書きします。この期間より古いスナップショット (現在時刻からの経過時間で判定) が期限切れ候補になります。値は、1 つ以上の `{number}{unit}` の組を連結した duration 文字列です。サポートされる単位: `y` (365日) 、`w` (7日) 、`d` (24時間) 、`h` (60分) 、`m` (60秒) 、`s` (1秒) 、`ms` (1ミリ秒) 。単位は組み合わせ可能です。例: `'3d'`、`'12h'`、`'1d12h30m'`、`'500ms'`。
* `retain_last = N` — この呼び出しに限り、テーブルレベルの `history.expire.min-snapshots-to-keep` を上書きします。古さに関係なく、少なくとも `N` 個のスナップショットが常に保持されます。
* `snapshot_ids = [id1, id2, ...]` — 列挙したスナップショット ID だけを期限切れにします (current snapshot、ブランチ、またはタグから参照されているスナップショットを除く) 。このモードでは保持ポリシーは完全に適用されず、`retention_period` または `retain_last` と組み合わせることはできません。
* `dry_run = 1` — 実際に新しいメタデータを書き込んだりファイルを削除したりせずに、何が期限切れになるかを計算してメトリクスを返します。

:::note
`retention_period` と `retain_last` が上書きするのは、**テーブルレベル** の保持に関するデフォルト値だけです。Iceberg テーブルプロパティ (例: `refs.<branch>.min-snapshots-to-keep`) で設定された ref ごとの保持 override (ブランチ/タグ) は上書きされず、常にテーブルメタデータで指定されたとおりに適用されます。
:::

**例:**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**出力:**

このコマンドは、2 つのカラム (`metric_name String`、`metric_value Int64`) を持つテーブルを返します。各メトリクスに対して 1 行が含まれます。メトリクス名は [Iceberg spec](https://iceberg.apache.org/docs/latest/spark-procedures/#output) に従います。

| metric&#95;name                       | 説明                                 |
| ------------------------------------- | ---------------------------------- |
| `deleted_data_files_count`            | 削除されたデータファイル数                      |
| `deleted_position_delete_files_count` | 削除された position deleteファイル数         |
| `deleted_equality_delete_files_count` | 削除された equality deleteファイル数         |
| `deleted_manifest_files_count`        | 削除されたマニフェストファイル数                   |
| `deleted_manifest_lists_count`        | 削除されたマニフェストリストファイル数                |
| `deleted_statistics_files_count`      | 削除された statisticsファイル数 (現時点では常に 0)  |
| `dry_run`                             | ドライランモードでは `1`、通常実行では `0`          |

このコマンドでは、次の手順を実行します。

1. 保持ポリシー (以下を参照) を評価し、保持対象とする必要があるスナップショットを決定します
2. timestamp 引数が指定されている場合は、その timestamp 以降のすべてのスナップショットも追加で保護します
3. ポリシーによって保持されず、timestamp fuse によっても保護されないスナップショットを期限切れにします
4. 期限切れとなったスナップショットにのみ関連付けられているファイルを特定します
5. 通常モードでは、期限切れのスナップショットを除外した新しいメタデータを生成します
6. 通常モードでは、到達不能になったマニフェストリスト、マニフェストファイル、データファイルを物理的に削除します
7. `dry_run = 1` モードでは、手順 5 と 6 をスキップし、算出されたメトリクスのみを返します

<div id="iceberg-snapshot-retention-policy">
  #### スナップショット保持ポリシー
</div>

`expire_snapshots` コマンドは、[Iceberg のスナップショット保持ポリシー](https://iceberg.apache.org/spec/#snapshot-retention-policy) に従います。保持設定は、Iceberg テーブルのプロパティと参照ごとのオーバーライドで構成します。

| プロパティ                                  | スコープ | デフォルト                                                                      | 説明                                            |
| -------------------------------------- | ---- | -------------------------------------------------------------------------- | --------------------------------------------- |
| `history.expire.min-snapshots-to-keep` | テーブル | `iceberg_expire_default_min_snapshots_to_keep` (default `1`)               | 各ブランチの祖先チェーンで保持するスナップショットの最小数                 |
| `history.expire.max-snapshot-age-ms`   | テーブル | `iceberg_expire_default_max_snapshot_age_ms` (default `432000000`, 5 days) | ブランチで保持するスナップショットの最大経過時間 (ms)                 |
| `history.expire.max-ref-age-ms`        | テーブル | `iceberg_expire_default_max_ref_age_ms` (default `∞`)                      | スナップショット参照 (ブランチまたはタグ) 自体を削除するまでの最大経過時間 (ms)  |

各スナップショット参照 (Iceberg のメタデータ内の `refs`) では、参照ごとのフィールド `min-snapshots-to-keep`、`max-snapshot-age-ms`、`max-ref-age-ms` を使って、これらの設定をオーバーライドできます。

**保持の評価:**

* **各ブランチ** (`main` を含む) について: ブランチの head から祖先チェーンをたどります。以下のいずれかの条件が true である間、スナップショットは保持されます。
  * そのスナップショットが、チェーン内の先頭から `min-snapshots-to-keep` 個以内に含まれている
  * そのスナップショットの経過時間が `max-snapshot-age-ms` 以内である (つまり、`now - timestamp-ms <= max-snapshot-age-ms`)
* **タグ**について: タグ付けされたスナップショットは、タグが `max-ref-age-ms` を超えない限り保持され、超えた場合はタグ参照が削除されます
* **`main` 以外の参照**で、経過時間が `max-ref-age-ms` を超えたものは完全に削除されます (`main` ブランチが削除されることはありません)
* 存在しないスナップショットを指す**孤立した参照**は、警告を出して削除されます
* **現在のスナップショットは常に保持されます**。保持設定にかかわらず保持されます

**必要な権限:**

`ALTER TABLE EXECUTE` 権限が必要です。これは ClickHouse のアクセス制御階層において `ALTER TABLE` の子権限です。個別に付与することも、親権限経由で付与することもできます。

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* サポートされるのは Iceberg format version 2 のテーブルのみです (v1 のスナップショットでは `manifest-list` が保証されないため、クリーンアップ対象のファイルを安全に特定するにはこれが必要です)
* current snapshot は、指定したタイムスタンプより古い場合でも常に保持されます
* `allow_insert_into_iceberg` 設定を有効にする必要があります
* `allow_experimental_expire_snapshots` 設定を有効にする必要があります
* ClickHouse がメタデータを更新する際は、カタログ自体の認可 (REST カタログ認証、AWS Glue IAM など) も独立して適用されます
  :::

<div id="iceberg-remove-orphan-files">
  ### 孤立ファイルを削除する
</div>

孤立ファイルとは、Iceberg テーブルのメタデータ内のどのスナップショットからも参照されていないストレージ上のファイルです。これらは、書き込みの失敗、コンパクション後の不完全なクリーンアップ、操作の中断によって蓄積し、ストレージ使用量の無制限な増加を引き起こします。`remove_orphan_files` コマンドは、こうした孤立ファイルを特定して削除します。

**構文:**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**パラメータ:**

| Parameter    | Type                 | Default                                                | Description                                                                   |
| ------------ | -------------------- | ------------------------------------------------------ | ----------------------------------------------------------------------------- |
| `older_than` | `String` (timestamp) | 3日前 (`iceberg_orphan_files_older_than_seconds` で設定可能)  | 最終更新時刻がこのタイムスタンプより古いファイルのみを、孤立ファイルの候補として扱います。進行中の書き込みで作成中のファイルを削除しないための安全策です。 |
| `location`   | `String`             | テーブルの格納場所                                              | スキャン対象を、テーブルの格納場所配下にある特定のサブディレクトリ (例: `'data/'` または `'metadata/'`) に限定します。    |
| `dry_run`    | `UInt64`             | `0`                                                    | `1` の場合、孤立ファイルを特定し、実際には何も削除せずに結果の要約を返します。                                     |

**例:**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**出力:**

このコマンドは、カテゴリごとの削除済みファイル数 (`dry&#95;run` モードでは削除対象となるファイル数) を示す `metric_name` と `metric_value` のカラムを含むテーブルを返します。ファイルカテゴリは、ファイル名の命名規則に基づくベストエフォートのヒューリスティクスで分類されます。特定のパターンに一致しないファイルは、デフォルトで `deleted_data_files_count` に分類されます。

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**設定:**

| Setting                                   | Type     | Default           | Description                                 |
| ----------------------------------------- | -------- | ----------------- | ------------------------------------------- |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`           | この機能を有効にするためのゲート設定 (実験的機能) 。                |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 days) | 引数を省略した場合に使用される、秒単位のデフォルトの `older_than` 閾値。 |

:::note

* **Iceberg format version 2 以上が必要です。** バージョン 1 のテーブルは、到達可能なファイルセットを安全に判定するために必要な、スナップショット内の `manifest-list` ポインタを持たないため拒否されます。v1 テーブルでこのコマンドを実行すると、`BAD_ARGUMENTS` エラーが返されます。
* `allow_insert_into_iceberg` と `allow_iceberg_remove_orphan_files` の両方の設定を有効にする必要があります
* 期限切れのスナップショットだけが参照しているファイルを先にクリーンアップできるよう、`remove_orphan_files` の前に `expire_snapshots` を実行することを推奨します
* 削除前に孤立ファイルを確認するには `dry_run = 1` を使用します
* `older_than` 閾値は、進行中の書き込みに含まれるファイルが削除されるのを防ぎます。デフォルトの 3 日という閾値には十分な安全マージンがあります
  :::

<div id="see-also">
  ## 関連項目
</div>

* [Iceberg エンジン](/ja/engines/table-engines/integrations/iceberg.md)
* [Iceberg クラスターテーブル関数](/ja/sql-reference/table-functions/icebergCluster.md)