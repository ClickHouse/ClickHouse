---
sidebar_label: dlt
keywords: [clickhouse, dlt, connect, integrate, etl, data integration]
slug: /ja/integrations/dlt 
description: dlt統合を使用してClickHouseにデータをロードする
---

# ClickHouseにdltを接続する

<a href="https://dlthub.com/docs/intro" target="_blank">dlt</a>は、さまざまな混雑したデータソースから整理されたライブのデータセットにデータをロードするために、Pythonスクリプトに追加できるオープンソースライブラリです。



## ClickHouseにdltをインストールする

### ClickHouseの依存関係を伴う`dlt`ライブラリをインストールするには:
```bash
pip install "dlt[clickhouse]" 
```

## セットアップガイド

### 1. dltプロジェクトを初期化する

以下のように新しい`dlt`プロジェクトを初期化します:
```bash
dlt init chess clickhouse
```

:::note
このコマンドはsourceとしてchessを、destinationとしてClickHouseを持つパイプラインを初期化します。
:::

上記のコマンドにより、`.dlt/secrets.toml`やClickHouse用のrequirementsファイルなどいくつかのファイルとディレクトリが生成されます。次のように実行してrequirementsファイルに指定されている必要な依存関係をインストールできます:
```bash
pip install -r requirements.txt
```

または`pip install dlt[clickhouse]`で、ClickHouseを目的地として動作させるための`dlt`ライブラリと必要な依存関係をインストールします。

### 2. ClickHouseデータベースをセットアップする

ClickHouseにデータをロードするには、ClickHouseデータベースを作成する必要があります。次のような大まかな手順を行います:

1. 既存のClickHouseデータベースを使用するか、新しいものを作成します。

2. 新しいデータベースを作成するには、`clickhouse-client`コマンドラインツールやお好みのSQLクライアントを使用してClickHouseサーバーに接続します。

3. 次のSQLコマンドを実行して、新しいデータベース、ユーザーを作成し、必要な権限を付与します:

```bash
CREATE DATABASE IF NOT EXISTS dlt;
CREATE USER dlt IDENTIFIED WITH sha256_password BY 'Dlt*12345789234567';
GRANT CREATE, ALTER, SELECT, DELETE, DROP, TRUNCATE, OPTIMIZE, SHOW, INSERT, dictGet ON dlt.* TO dlt;
GRANT SELECT ON INFORMATION_SCHEMA.COLUMNS TO dlt;
GRANT CREATE TEMPORARY TABLE, S3 ON *.* TO dlt;
```

### 3. 認証情報を追加する

次に、`.dlt/secrets.toml`ファイル内にClickHouseの認証情報を以下のように設定します:

```bash
[destination.clickhouse.credentials]
database = "dlt"                         # 作成したデータベース名
username = "dlt"                         # ClickHouseユーザー名。通常のデフォルトは"default"
password = "Dlt*12345789234567"          # ClickHouseのパスワードがあれば
host = "localhost"                       # ClickHouseサーバーのホスト
port = 9000                              # ClickHouse HTTPポート。デフォルトは9000
http_port = 8443                         # ClickHouseサーバーのHTTPインターフェイスに接続するポート。デフォルトは8443
secure = 1                               # HTTPSを使用している場合は1、それ以外は0
dataset_table_separator = "___"          # データセットテーブル名のセパレータ
```

:::note
HTTP_PORT
`http_port`パラメータは、ClickHouseサーバーのHTTPインターフェイスに接続する際に使用するポート番号を指定します。これは、ネイティブTCPプロトコルで使用されるデフォルトポート9000とは異なります。

外部ステージングを使用していない場合（つまり、パイプラインでstagingパラメータを設定していない場合）、`http_port`を設定する必要があります。これは、dltの組み込みClickHouseローカルストレージステージングが<a href="https://github.com/ClickHouse/clickhouse-connect">clickhouse content</a>ライブラリを使用しており、ClickHouseとの通信はHTTP経由で行われるためです。

ClickHouseサーバーが`http_port`で指定されたポートでHTTP接続を受け入れるように設定されていることを確認してください。例えば、`http_port = 8443`を設定した場合、ClickHouseはポート8443でHTTPリクエストを待ち受けることになります。外部ステージングを使用している場合は、`http_port`パラメータを省略することができます。clickhouse-connectはこの場合使用されないからです。
:::

`clickhouse-driver`ライブラリで使用されるものに似たデータベース接続文字列を渡すことができます。上記の認証情報は次のようになります:

```bash
# tomlファイルのセクションが始まる前に、ファイルの上部に保管してください。
destination.clickhouse.credentials="clickhouse://dlt:Dlt*12345789234567@localhost:9000/dlt?secure=1"
```

## Write Disposition

全ての[Write Disposition](https://dlthub.com/docs/general-usage/incremental-loading#choosing-a-write-disposition)がサポートされています。

dltライブラリにおけるWrite Dispositionは、データがデスティネーションにどのように書き込まれるべきかを定義します。3種類のWrite Dispositionがあります:

**Replace**: このディスポジションは、リソースからのデータでデスティネーションのデータを置き換えます。すべてのクラスやオブジェクトを削除し、データをロードする前にスキーマを再作成します。詳細は<a href="https://dlthub.com/docs/general-usage/full-loading">こちら</a>をご覧ください。

**Merge**: このWrite Dispositionは、リソースからのデータをデスティネーション内のデータとマージします。`merge`ディスポジションの場合、リソースのために`primary_key`を指定する必要があります。詳細は<a href="https://dlthub.com/docs/general-usage/incremental-loading">こちら</a>をご覧ください。

**Append**: これはデフォルトのディスポジションです。デスティネーションの既存のデータにデータを追加し、`primary_key`フィールドを無視します。

## Data Loading
データは、データソースに応じて最も効率的な方法でClickHouseにロードされます:

- ローカルファイルの場合、`clickhouse-connect`ライブラリを使用して、`INSERT`コマンドを用いて直接ファイルをClickHouseテーブルにロードします。
- `S3`、`Google Cloud Storage`、`Azure Blob Storage`のようなリモートストレージにあるファイルの場合、ClickHouseのテーブル関数（s3、gcs、azureBlobStorageなど）を使用してファイルを読み込み、データをテーブルに挿入します。

## Datasets

`ClickHouse`は1つのデータベースで複数のデータセットをサポートしませんが、`dlt`は複数の理由からデータセットに依存しています。`ClickHouse`を`dlt`で動作させるためには、`ClickHouse`データベース内で`dlt`によって生成されるテーブル名には、データセット名が接頭辞としてつけられ、設定可能な`dataset_table_separator`で区切られます。さらに、データを含まない特別なセンチネルテーブルが作成され、`dlt`がどの仮想データセットがすでに`ClickHouse`のデスティネーションに存在するかを認識できるようにします。

## Supported File Formats

- <a href="https://dlthub.com/docs/dlt-ecosystem/file-formats/jsonl">jsonl</a>は、直接ロードとステージングの両方で使用するための推奨フォーマットです。
- <a href="https://dlthub.com/docs/dlt-ecosystem/file-formats/parquet">parquet</a>は、直接ロードとステージングの両方でサポートされています。

`clickhouse`デスティネーションには、デフォルトのsqlデスティネーションからいくつかの特定の逸脱があります:

1. `ClickHouse`にはエクスペリメンタルな`object`データ型がありますが、これが予測不可能なことがあるため、dltのClickHouseデスティネーションは複雑なデータ型をテキストカラムにロードします。この機能が必要な場合は、私たちのSlackコミュニティにご連絡ください。追加検討します。
2. `ClickHouse`は`time`データ型をサポートしていません。時間は`text`カラムにロードされます。
3. `ClickHouse`は`binary`データ型をサポートしていません。代わりに、バイナリーデータは`text`カラムにロードされます。`jsonl`からロードする際には、バイナリーデータはbase64文字列になり、parquetからロードする際には`binary`オブジェクトが`text`に変換されます。
5. `ClickHouse`では、データが入っているテーブルに対して非NULLのカラムを追加することを許可しています。
6. `ClickHouse`はある条件下でfloatまたはdoubleデータ型を使用する際に丸め誤差を生じる可能性があります。丸め誤差を許容できない場合は、decimalデータ型を使用するようにしてください。例えば、ローダーファイルフォーマットがjsonlに設定されている場合に値12.7001をdoubleカラムにロードすると、予測可能に丸め誤差が生じます。

## Supported Column Hints
ClickHouseは以下の<a href="https://dlthub.com/docs/general-usage/schema#tables-and-columns">カラムヒント</a>をサポートしています:

- `primary_key` - カラムが主キーの一部であることを示します。複数のカラムがこのヒントを持つことで複合主キーを作成できます。

## テーブルエンジン
デフォルトでは、ClickHouseで`ReplicatedMergeTree`テーブルエンジンを使用してテーブルが作成されます。`table_engine_type`を使用してclickhouseアダプターで代替のテーブルエンジンを指定できます:

```bash
from dlt.destinations.adapters import clickhouse_adapter


@dlt.resource()
def my_resource():
  ...

clickhouse_adapter(my_resource, table_engine_type="merge_tree")
```

サポートされている値は以下の通りです:

- `merge_tree` - `MergeTree`エンジンを使用してテーブルを作成
- `replicated_merge_tree` (デフォルト) - `ReplicatedMergeTree`エンジンを使用してテーブルを作成

## ステージングサポート

ClickHouseはAmazon S3、Google Cloud Storage、Azure Blob Storageをファイルステージングのデスティネーションとしてサポートしています。

`dlt`はParquetまたはJSONLファイルをステージングロケーションにアップロードし、ClickHouseのテーブル関数を使用してステージングされたファイルから直接データをロードします。

ステージングデスティネーションの認証情報の設定方法については、ファイルシステムドキュメントを参照してください:

- <a href="https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#aws-s3">Amazon S3</a>
- <a href="https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#google-storage">Google Cloud Storage</a>
- <a href="https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#azure-blob-storage">Azure Blob Storage</a>

ステージングを有効にしてパイプラインを実行するには:

```bash
pipeline = dlt.pipeline(
  pipeline_name='chess_pipeline',
  destination='clickhouse',
  staging='filesystem',  # ステージングを有効にするために追加
  dataset_name='chess_data'
)
```

### Google Cloud Storageをステージングエリアとして使用する
dltはGoogle Cloud Storage（GCS）をClickHouseにデータをロードする際のステージングエリアとして使用することをサポートしています。これはClickHouseの<a href="https://clickhouse.com/docs/ja/sql-reference/table-functions/gcs">GCSテーブル関数</a>によって自動的に処理されます。

ClickHouseのGCSテーブル関数は、ハッシュベースのメッセージ認証コード（HMAC）キーを使用した認証のみをサポートしています。これを可能にするため、GCSはAmazon S3 APIをエミュレートするS3互換モードを提供しています。ClickHouseはこれを活用して、S3統合を通じてGCSバケットにアクセスできるようにしています。

dltでのHMAC認証を使用したGCSステージングのセットアップ方法:

1. <a href="https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create">Google Cloudのガイド</a>に従って、GCSサービスアカウントのHMACキーを作成します。

2. dltプロジェクトのClickHouseデスティネーション設定`config.toml`に、サービスアカウントの`client_email`、 `project_id`、`private_key`と共にHMACキーを設定します:

```bash
[destination.filesystem]
bucket_url = "gs://dlt-ci"

[destination.filesystem.credentials]
project_id = "a-cool-project"
client_email = "my-service-account@a-cool-project.iam.gserviceaccount.com"
private_key = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkaslkdjflasjnkdcopauihj...wEiEx7y+mx\nNffxQBqVVej2n/D93xY99pM=\n-----END PRIVATE KEY-----\n"

[destination.clickhouse.credentials]
database = "dlt"
username = "dlt"
password = "Dlt*12345789234567"
host = "localhost"
port = 9440
secure = 1
gcp_access_key_id = "JFJ$$*f2058024835jFffsadf"
gcp_secret_access_key = "DFJdwslf2hf57)%$02jaflsedjfasoi"
```

注: HMACキー( `gcp_access_key_id`及び`gcp_secret_access_key`)に加えて、`client_email`、`project_id`、`private_key`を`[destination.filesystem.credentials]`の下に提供する必要があります。これは、GCSステージングサポートが一時的なワークアラウンドとして実装されており、まだ最適化されていないためです。

dltはこれらの認証情報をClickHouseに渡し、ClickHouseが認証とGCSへのアクセスを処理します。

ClickHouseのdltデスティネーションにおけるGCSステージング設定を簡素化し、改善するための作業が進行中です。適切なGCSステージングサポートは、以下のGitHubの問題として追跡されています:

- ファイルシステムデスティネーションを<a href="https://github.com/dlt-hub/dlt/issues/1272">GCSのS3互換モードで動作させる</a>
- Google Cloud Storageステージングエリア<a href="https://github.com/dlt-hub/dlt/issues/1181">サポート</a>

### dbtサポート
<a href="https://dlthub.com/docs/dlt-ecosystem/transformations/dbt/">dbt</a>との統合は、一般的にdbt-clickhouseを通じてサポートされています。

### `dlt`の状態の同期
このデスティネーションは<a href="https://dlthub.com/docs/general-usage/state#syncing-state-with-destination">dlt</a>の状態同期を完全にサポートしています。
