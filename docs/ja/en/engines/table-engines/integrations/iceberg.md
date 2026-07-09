---
description: 'このエンジンは、Amazon S3、Azure、HDFS、およびローカルに保存された既存の Apache Iceberg
  テーブルに対する読み取り専用のインテグレーションを提供します。'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Iceberg テーブルエンジン'
doc_type: 'reference'
---

:::warning
ClickHouse で Iceberg データを扱うには、[Iceberg テーブル関数](/ja/sql-reference/table-functions/iceberg.md) を使用することを推奨します。現在、Iceberg テーブル関数は十分な機能を備えており、Iceberg テーブルに対して部分的な読み取り専用インターフェイスを提供します。

Iceberg テーブルエンジンも利用できますが、いくつかの制限があります。ClickHouse はもともと、外部でスキーマが変更されるテーブルのサポートを前提として設計されていないため、これが Iceberg テーブルエンジンの動作に影響することがあります。その結果、通常のテーブルでは利用できる一部の機能が使えない場合や、正しく動作しない場合があります。特に、古いアナライザを使用している場合にその傾向が強くなります。

最適な互換性を得るため、Iceberg テーブルエンジンのサポート改善を進めている間は、Iceberg テーブル関数を使用することをお勧めします。
:::

このエンジンは、Amazon S3、Azure、HDFS、およびローカルに保存された既存の Apache [Iceberg](https://iceberg.apache.org/) テーブルに対する読み取り専用のインテグレーションを提供します。

<div id="create-table">
  ## テーブルの作成
</div>

Icebergテーブルはストレージ内にあらかじめ存在している必要がある点に注意してください。このコマンドでは、新しいテーブルを作成するためのDDLパラメータは指定できません。

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## エンジン引数
</div>

引数の説明は、それぞれエンジン `S3`、`AzureBlobStorage`、`HDFS`、`File` の引数の説明に対応しています。
`format` は、Icebergテーブルのデータファイルのフォーマットを表します。

`IcebergS3` では、オプションの `extra_credentials` パラメータを使用して、ClickHouse Cloud でロールベースアクセスを行うための `role_arn` を渡すことができます。設定手順については、[Secure S3](/ja/cloud/data-sources/secure-s3) を参照してください。

エンジンパラメータは [Named Collections](../../../operations/named-collections.md) を使用して指定できます

<div id="example">
  ### 例
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

named collections を使用する場合:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## 別名
</div>

`Iceberg` テーブルエンジンは、`disk` 設定からストレージバックエンドを自動検出し、それに応じて `IcebergS3`、`IcebergAzure`、または `IcebergLocal` を使い分けます。`disk` が指定されていない場合は、デフォルトで `IcebergS3` 実装が使用されます。

<div id="data-types">
  ## データ型
</div>

以下の表は、スキーマ推論時に (読み取り時に) Iceberg のデータ型が ClickHouse のデータ型にどのようにマッピングされるかを示しています。

<div id="primitive-types">
  ### プリミティブ型
</div>

| Iceberg 型          | ClickHouse 型           | 注記                                |
| ------------------ | ---------------------- | --------------------------------- |
| `boolean`          | `Bool`                 |                                   |
| `int`              | `Int32`                |                                   |
| `long`, `bigint`   | `Int64`                |                                   |
| `float`            | `Float32`              |                                   |
| `double`           | `Float64`              |                                   |
| `date`             | `Date32`               |                                   |
| `time`             | `Int64`                | 午前0時からのマイクロ秒数                     |
| `timestamp`        | `DateTime64(6)`        | マイクロ秒、タイムゾーンなし                    |
| `timestamptz`      | `DateTime64(6, 'UTC')` | マイクロ秒、UTC タイムゾーン                  |
| `timestamp_ns`     | `DateTime64(9)`        | ナノ秒、タイムゾーンなし (Iceberg v3 以降のみ)    |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | ナノ秒、UTC タイムゾーン (Iceberg v3 以降のみ)  |
| `string`, `binary` | `String`               |                                   |
| `uuid`             | `UUID`                 |                                   |
| `fixed(N)`         | `FixedString(N)`       |                                   |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                   |

<div id="complex-types">
  ### 複合型
</div>

| Iceberg 型 | ClickHouse 型 |
| --------- | ------------ |
| `list`    | `Array`      |
| `map`     | `Map`        |
| `struct`  | `Tuple`      |

<div id="schema-evolution">
  ## スキーマ進化
</div>

ClickHouse は、時間の経過とともにスキーマが進化した Iceberg テーブルの読み取りをサポートしています。これには、カラムの追加、削除、並べ替えが行われたテーブルや、必須から Nullable に変更されたカラムを含むテーブルが含まれます。さらに、以下の型変換がサポートされています。

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

現在のところ、ネストされた構造や、Array および Map 内の要素の型を変更することはできません。

動的スキーマ推論で作成した後にスキーマが変更されたテーブルを読み取るには、テーブル作成時に allow&#95;dynamic&#95;metadata&#95;for&#95;data&#95;lakes = true を設定してください。

<div id="partition-pruning">
  ## パーティションプルーニング
</div>

ClickHouse は、Iceberg テーブルに対する SELECT クエリでパーティションプルーニングをサポートしており、無関係なデータファイルをスキップすることでクエリパフォーマンスの最適化に役立ちます。パーティションプルーニングを有効にするには、`use_iceberg_partition_pruning = 1` を設定します。Iceberg のパーティションプルーニングの詳細については、https://iceberg.apache.org/spec/#partitioning を参照してください

<div id="time-travel">
  ## タイムトラベル
</div>

ClickHouse は Iceberg テーブルでのタイムトラベルをサポートしており、特定のタイムスタンプまたはスナップショット ID を指定して履歴データをクエリできます。

<div id="deleted-rows">
  ## 削除された行を含むテーブルの処理
</div>

ClickHouse は、以下の削除方式を使用する Iceberg テーブルの読み取りに対応しています。

* [Position deletes](https://iceberg.apache.org/spec/#position-delete-files)
* [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files) (バージョン 25.8+ 以降でサポート)

以下の削除方式は **サポートされていません**。

* [Deletion vectors](https://iceberg.apache.org/spec/#deletion-vectors) (v3 で導入)

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

注: 同じクエリ内で `iceberg_timestamp_ms` パラメータと `iceberg_snapshot_id` パラメータの両方を指定することはできません。

<div id="important-considerations">
  ### 重要な考慮事項
</div>

* **スナップショット** は通常、次のタイミングで作成されます:
  * 新しいデータがテーブルに書き込まれたとき
  * 何らかのデータ compaction が実行されたとき

* **スキーマ変更では通常、スナップショットは作成されません** - このため、スキーマ進化が発生したテーブルでタイムトラベルを使用する場合に、重要な挙動が生じます。

<div id="example-scenarios">
  ### 例のシナリオ
</div>

CH はまだ Icebergテーブルへの書き込みをサポートしていないため、すべてのシナリオは Spark で記述しています。

<div id="scenario-1">
  #### シナリオ 1: 新しいスナップショットを伴わないスキーマ変更
</div>

次の一連の操作を考えてみましょう：

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

* ts1 と ts2: 元の 2 つのカラムのみが表示されます
* ts3: 3 つのカラムがすべて表示され、1 行目の `price` は NULL になります

<div id="scenario-2">
  #### シナリオ 2: 過去と現在のスキーマの違い
</div>

現在時点でタイムトラベルクエリを実行すると、現在のテーブルとは異なるスキーマが表示されることがあります:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

これは、`ALTER TABLE` では新しいスナップショットは作成されず、現在のテーブルでは Spark がスナップショットではなく最新のメタデータファイルから `schema_id` の値を取得するためです。

<div id="scenario-3">
  #### シナリオ 3: 過去と現在のスキーマの違い
</div>

2 つ目は、タイムトラベルでは、そのテーブルにまだデータが一度も書き込まれていない時点の状態は取得できないという点です:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

ClickHouse では、挙動は Spark と同様です。Spark の Select クエリを ClickHouse の Select クエリに置き換えて考えると、同じように動作します。

<div id="metadata-file-resolution">
  ## メタデータファイルの解決
</div>

ClickHouseで`Iceberg` テーブルエンジンを使用する場合、システムはIcebergテーブルの構造を記述した適切なmetadata.jsonファイルを見つける必要があります。この解決処理は次のように行われます。

<div id="candidate-search">
  ### 候補の検索
</div>

1. **パスの直接指定**:

* `iceberg_metadata_file_path` を設定すると、システムはこの正確な path を Icebergテーブルの directory path と組み合わせて使用します。
* この設定が指定されている場合、他のすべての解決関連の設定は無視されます。

2. **table UUID の照合**:

* `iceberg_metadata_table_uuid` が指定されている場合、システムは次のように動作します:
  * `metadata` directory 内の `.metadata.json` ファイルのみを対象とします
  * 指定した UUID と一致する `table-uuid` フィールドを含むファイルに絞り込みます (大文字と小文字は区別しません)

3. **デフォルトの検索**:

* 上記いずれの設定も指定されていない場合、`metadata` directory 内のすべての `.metadata.json` ファイルが候補になります

<div id="most-recent-file">
  ### 最新のファイルの選択
</div>

上記のルールを使って候補ファイルを特定した後、システムはどのファイルが最新かを判定します。

* `iceberg_recent_metadata_file_by_last_updated_ms_field` が有効な場合:
  * `last-updated-ms` の値が最も大きいファイルが選択されます

* それ以外の場合:
  * バージョン番号が最も大きいファイルが選択されます
  * (バージョンは、`V.metadata.json` または `V-uuid.metadata.json` 形式のファイル名では `V` として表されます)

**注**: ここで挙げた設定は、明示的に別途指定がない限り、すべてエンジンレベルの設定です。以下に示すように、テーブル作成時に指定する必要があります。

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**注**: Icebergカタログは通常メタデータの解決を担いますが、ClickHouse の `Iceberg` テーブルエンジンは S3 に保存されたファイルを Icebergテーブルとして直接解釈するため、これらの解決ルールを理解しておくことが重要です。

<div id="data-cache">
  ## データキャッシュ
</div>

`Iceberg` テーブルエンジンおよびテーブル関数では、`S3`、`AzureBlobStorage`、`HDFS` ストレージと同様にデータキャッシュをサポートしています。詳細は[こちら](../../../engines/table-engines/integrations/s3.md#data-cache)を参照してください。

<div id="metadata-cache">
  ## メタデータキャッシュ
</div>

`Iceberg` テーブルエンジンとテーブル関数は、マニフェストファイル、マニフェストリスト、メタデータJSONの情報を保持するメタデータキャッシュをサポートしています。キャッシュはメモリ上に保持されます。この機能は設定 `use_iceberg_metadata_files_cache` で制御されており、デフォルトで有効です。

<div id="async-metadata-prefetch">
  ## 非同期メタデータプリフェッチ
</div>

非同期メタデータプリフェッチは、`Iceberg` テーブルの作成時に `iceberg_metadata_async_prefetch_period_ms` を設定することで有効にできます。0 (デフォルト) に設定されている場合、またはメタデータキャッシュが有効でない場合は、非同期プリフェッチは無効になります。
この機能を有効にするには、0 以外のミリ秒単位の値を指定する必要があります。これはプリフェッチサイクル間の間隔を表します。

有効にすると、サーバーはリモートカタログを一覧して新しいメタデータのバージョンを検出するバックグラウンド処理を定期的に実行します。次に、それを解析し、スナップショット を再帰的にたどりながら、アクティブなマニフェストリストファイルとマニフェストファイルを取得します。
メタデータキャッシュにすでに存在するファイルは、再度ダウンロードされません。各プリフェッチサイクルの終了時には、最新のメタデータスナップショットがメタデータキャッシュで利用可能になります。

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

読み取り操作時の非同期メタデータプリフェッチを最大限活用するには、`iceberg_metadata_staleness_ms` パラメータをクエリまたはセッションのパラメータとして指定する必要があります。デフォルトでは (0、つまり未指定) 、各クエリのコンテキストで、サーバーはリモートカタログから最新のメタデータを取得します。
メタデータの古さに対する許容値を指定すると、サーバーはリモートカタログを呼び出さずに、キャッシュされたバージョンのメタデータスナップショットを使用できます。キャッシュ内にメタデータのバージョンがあり、それが指定された古さのウィンドウ内にダウンロードされたものであれば、それがクエリの処理に使用されます。
それ以外の場合は、リモートカタログから最新バージョンが取得されます。

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**注意**: 非同期メタデータプリフェッチは `ICEBERG_SCEDULE_POOL` で実行されます。これは、アクティブな `Iceberg` テーブルに対するバックグラウンド操作用のサーバー側スレッドプールです。このスレッドプールのサイズは、サーバー設定パラメーター `iceberg_background_schedule_pool_size` (デフォルトは 10) で制御されます。

**注意**: 非同期プリフェッチが有効な場合、現在は、メタデータキャッシュのサイズが、すべてのアクティブなテーブルの最新のメタデータスナップショット全体を保持するのに十分であることを前提としています。

<div id="see-also">
  ## 関連項目
</div>

* [Iceberg テーブル関数](/ja/sql-reference/table-functions/iceberg.md)