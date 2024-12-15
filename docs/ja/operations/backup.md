---
slug: /ja/operations/backup
description: 人為的ミスを効果的に軽減するために、データのバックアップと復元の戦略を慎重に準備する必要があります。
---

# バックアップと復元

- [ローカルディスクへのバックアップ](#backup-to-a-local-disk)
- [S3エンドポイントを使用するためのバックアップ/リストアの設定](#configuring-backuprestore-to-use-an-s3-endpoint)
- [S3ディスクを使用したバックアップ/リストア](#backuprestore-using-an-s3-disk)
- [代替案](#alternatives)

## コマンド概要

```bash
 BACKUP|RESTORE
  TABLE [db.]table_name [AS [db.]table_name_in_backup]
    [PARTITION[S] partition_expr [,...]] |
  DICTIONARY [db.]dictionary_name [AS [db.]name_in_backup] |
  DATABASE database_name [AS database_name_in_backup]
    [EXCEPT TABLES ...] |
  TEMPORARY TABLE table_name [AS table_name_in_backup] |
  VIEW view_name [AS view_name_in_backup]
  ALL TEMPORARY TABLES [EXCEPT ...] |
  ALL [EXCEPT ...] } [,...]
  [ON CLUSTER 'cluster_name']
  TO|FROM File('<path>/<filename>') | Disk('<disk_name>', '<path>/') | S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>')
  [SETTINGS base_backup = File('<path>/<filename>') | Disk(...) | S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>')]

```

:::note ALL
ClickHouseのバージョン23.4以前では、`ALL`は`RESTORE`コマンドにのみ適用されました。
:::

## 背景

[レプリケーション](../engines/table-engines/mergetree-family/replication.md)はハードウェアの障害からデータを保護しますが、人為的ミスに対しては保護しません。たとえば、データの誤削除や、不適切なテーブルの削除、または間違ったクラスター上での削除、ソフトウェアのバグによる誤ったデータ処理やデータ破損が含まれます。これらのミスは多くの場合、すべてのレプリカに影響します。ClickHouseは、一部の誤りを防ぐための組み込みのセーフガードを持っています。たとえば、デフォルトでは、[50GBを超えるデータを含むMergeTreeのようなエンジンを持つテーブルを簡単に削除できない](server-configuration-parameters/settings.md#max-table-size-to-drop)ようにすることができます。しかし、これらのセーフガードではすべてのケースをカバーしきれず、回避される可能性があります。

人為的ミスを効果的に軽減するために、**事前に**データのバックアップと復元の戦略を慎重に準備する必要があります。

企業ごとに利用可能なリソースやビジネス要件が異なるため、すべての状況に合うClickHouseのバックアップと復元の万能な解決策は存在しません。1GBのデータに対して機能するものが、数十ペタバイトには効かない可能性があります。それぞれに独自の利点と欠点があるさまざまなアプローチがあります。以下でこれについて説明します。さまざまな欠点を補うために、1つのアプローチだけでなくいくつかのアプローチを使用するのが良い考えです。

:::note
何かをバックアップしたが復元を試したことがない場合、実際に必要な時に復元が正常に動作しない可能性があります（少なくともビジネスが許容できる以上に時間がかかります）。したがって、どのバックアップアプローチを選択するにしても、復元プロセスも自動化し、定期的に予備のClickHouseクラスター上で実行することを確認してください。
:::

## ローカルディスクへのバックアップ

### バックアップ先の設定

以下の例では、バックアップの宛先が`Disk('backups', '1.zip')`のように指定されていることがわかります。目的地を準備するには、/etc/clickhouse-server/config.d/backup_disk.xmlにバックアップ先を指定するファイルを追加します。たとえば、このファイルは`backups`という名前のディスクを定義し、そのディスクを**backups > allowed_disk**リストに追加します:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
<!--highlight-next-line -->
            <backups>
                <type>local</type>
                <path>/backups/</path>
            </backups>
        </disks>
    </storage_configuration>
<!--highlight-start -->
    <backups>
        <allowed_disk>backups</allowed_disk>
        <allowed_path>/backups/</allowed_path>
    </backups>
<!--highlight-end -->
</clickhouse>
```

### パラメータ

バックアップはフルまたは増分バックアップで行うことができ、テーブル（マテリアライズドビュー、プロジェクション、およびDictionaryを含む）やデータベースを含めることができます。バックアップは同期（デフォルト）または非同期にすることができ、圧縮することもできます。バックアップにはパスワード保護を設定できます。

BACKUPおよびRESTOREステートメントは、データベースおよびテーブル名のリスト、宛先（またはソース）、オプションと設定を取ります:
- バックアップの宛先、または復元のソース。これは以前に定義されたディスクに基づいています。たとえば`Disk('backups', 'filename.zip')`
- ASYNC: バックアップまたは復元を非同期に行う
- PARTITIONS: 復元するパーティションのリスト
- SETTINGS:
    - `id`: バックアップまたは復元操作のIDを指定し、指定しない場合はランダムに生成されます。既に同じ`id`で実行されている操作がある場合、例外が発生します。
    - [`compression_method`](/docs/ja/sql-reference/statements/create/table.md/#column-compression-codecs)およびcompression_level
    - ディスク上のファイルの`password`
    - `base_backup`: このソースの前回のバックアップの宛先。たとえば、`Disk('backups', '1.zip')`
    - `use_same_s3_credentials_for_base_backup`: ベースバックアップをS3に対してクエリの資格情報を継承するかどうか。`S3`でのみ機能。
    - `use_same_password_for_base_backup`: ベースバックアップアーカイブがクエリのパスワードを継承するかどうか。
    - `structure_only`: 有効にすると、テーブルのデータなしでCREATEステートメントのみをバックアップまたは復元できます
    - `storage_policy`: 復元されるテーブルのストレージポリシー。参照：[データストレージ用の複数のブロックデバイスの使用](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)。この設定は`RESTORE`コマンドにのみ適用されます。指定されたストレージポリシーは`MergeTree`ファミリーのエンジンを持つテーブルにのみ適用されます。
    - `s3_storage_class`: S3バックアップに使用されるストレージクラス。たとえば、`STANDARD`
    - `azure_attempt_to_create_container`: Azure Blob Storageを使用する際、指定されたコンテナが存在しない場合に作成を試みるかどうか。デフォルトはtrueです。

### 使用例

テーブルをバックアップし、その後復元する:
```
BACKUP TABLE test.table TO Disk('backups', '1.zip')
```

対応する復元:
```
RESTORE TABLE test.table FROM Disk('backups', '1.zip')
```

:::note
上記の`RESTORE`はテーブル`test.table`にデータが含まれている場合に失敗します。復元をテストするためには、テーブルを削除する必要があります。または、`allow_non_empty_tables=true`という設定を使用してください:
```
RESTORE TABLE test.table FROM Disk('backups', '1.zip')
SETTINGS allow_non_empty_tables=true
```
:::

テーブルを新しい名前で復元またはバックアップすることができます:
```
RESTORE TABLE test.table AS test.table2 FROM Disk('backups', '1.zip')
```

```
BACKUP TABLE test.table3 AS test.table4 TO Disk('backups', '2.zip')
```

### 増分バックアップ

増分バックアップは、`base_backup`を指定することで取得できます。
:::note
増分バックアップはベースバックアップに依存しています。増分バックアップから復元するためには、ベースバックアップを利用可能に保つ必要があります。
:::

新しいデータを増分で保存します。`base_backup`の設定により、以前のバックアップからのデータが`Disk('backups', 'd.zip')`から`Disk('backups', 'incremental-a.zip')`に保存されます:
```
BACKUP TABLE test.table TO Disk('backups', 'incremental-a.zip')
  SETTINGS base_backup = Disk('backups', 'd.zip')
```

増分バックアップとベースバックアップからのすべてのデータを新しいテーブル`test.table2`に復元します:
```
RESTORE TABLE test.table AS test.table2
  FROM Disk('backups', 'incremental-a.zip');
```

### バックアップにパスワードを設定する

ディスクに書き込まれたバックアップには、ファイルにパスワードを適用することができます:
```
BACKUP TABLE test.table
  TO Disk('backups', 'password-protected.zip')
  SETTINGS password='qwerty'
```

復元:
```
RESTORE TABLE test.table
  FROM Disk('backups', 'password-protected.zip')
  SETTINGS password='qwerty'
```

### 圧縮設定

圧縮方法またはレベルを指定したい場合:
```
BACKUP TABLE test.table
  TO Disk('backups', 'filename.zip')
  SETTINGS compression_method='lzma', compression_level=3
```

### 特定のパーティションを復元する
テーブルに関連付けられた特定のパーティションを復元する必要がある場合、これを指定することができます。バックアップからパーティション1と4を復元するには:
```
RESTORE TABLE test.table PARTITIONS '2', '3'
  FROM Disk('backups', 'filename.zip')
```

### tarアーカイブとしてのバックアップ

バックアップはzipと同様にtarアーカイブとして保存することもできます。ただし、パスワードはサポートされていません。

tarとしてバックアップを書く:
```
BACKUP TABLE test.table TO Disk('backups', '1.tar')
```

対応する復元:
```
RESTORE TABLE test.table FROM Disk('backups', '1.tar')
```

圧縮方法を変更するには、バックアップ名に適切なファイルサフィックスを付ける必要があります。gzipを使用してtarアーカイブを圧縮するには:
```
BACKUP TABLE test.table TO Disk('backups', '1.tar.gz')
```

サポートされている圧縮ファイルサフィックスは`tar.gz`、`.tgz`、`tar.bz2`、`tar.lzma`、`.tar.zst`、`.tzst`および`.tar.xz`です。

### バックアップのステータスを確認する

バックアップコマンドは`id`および`status`を返し、その`id`を使用してバックアップのステータスを取得できます。これは、長時間のASYNCバックアップの進行状況を確認するのに非常に便利です。以下の例では、既存のバックアップファイルを上書きしようとしたときに発生した失敗を示しています:
```sql
BACKUP TABLE helloworld.my_first_table TO Disk('backups', '1.zip') ASYNC
```
```response
┌─id───────────────────────────────────┬─status──────────┐
│ 7678b0b3-f519-4e6e-811f-5a0781a4eb52 │ CREATING_BACKUP │
└──────────────────────────────────────┴─────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

```
SELECT
    *
FROM system.backups
where id='7678b0b3-f519-4e6e-811f-5a0781a4eb52'
FORMAT Vertical
```
```response
Row 1:
──────
id:                7678b0b3-f519-4e6e-811f-5a0781a4eb52
name:              Disk('backups', '1.zip')
#highlight-next-line
status:            BACKUP_FAILED
num_files:         0
uncompressed_size: 0
compressed_size:   0
#highlight-next-line
error:             Code: 598. DB::Exception: Backup Disk('backups', '1.zip') already exists. (BACKUP_ALREADY_EXISTS) (version 22.8.2.11 (official build))
start_time:        2022-08-30 09:21:46
end_time:          2022-08-30 09:21:46

1 row in set. Elapsed: 0.002 sec.
```

`system.backups`テーブルと共に、すべてのバックアップおよびリストア操作はシステムログテーブル[backup_log](../operations/system-tables/backup_log.md)でも追跡されます:
```
SELECT *
FROM system.backup_log
WHERE id = '7678b0b3-f519-4e6e-811f-5a0781a4eb52'
ORDER BY event_time_microseconds ASC
FORMAT Vertical
```
```response
Row 1:
──────
event_date:              2023-08-18
event_time_microseconds: 2023-08-18 11:13:43.097414
id:                      7678b0b3-f519-4e6e-811f-5a0781a4eb52
name:                    Disk('backups', '1.zip')
status:                  CREATING_BACKUP
error:
start_time:              2023-08-18 11:13:43
end_time:                1970-01-01 03:00:00
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

Row 2:
──────
event_date:              2023-08-18
event_time_microseconds: 2023-08-18 11:13:43.174782
id:                      7678b0b3-f519-4e6e-811f-5a0781a4eb52
name:                    Disk('backups', '1.zip')
status:                  BACKUP_FAILED
#highlight-next-line
error:                   Code: 598. DB::Exception: Backup Disk('backups', '1.zip') already exists. (BACKUP_ALREADY_EXISTS) (version 23.8.1.1)
start_time:              2023-08-18 11:13:43
end_time:                2023-08-18 11:13:43
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

2 rows in set. Elapsed: 0.075 sec.
```

## S3エンドポイントを使用するためのBACKUP/RESTOREの設定

S3バケットにバックアップを書くためには、次の情報が必要です:
- S3エンドポイント,
  たとえば `https://mars-doc-test.s3.amazonaws.com/backup-S3/`
- アクセスキーID,
  たとえば `ABC123`
- シークレットアクセスキー,
  たとえば `Abc+123`

:::note
S3バケットの作成については[ClickHouseディスクとしてのS3オブジェクトストレージの使用](/docs/ja/integrations/data-ingestion/s3/index.md#configuring-s3-for-clickhouse-use)でカバーされていますが、ポリシーを保存した後にこのドキュメントに戻るだけでよく、ClickHouseをS3バケットで使用するように設定する必要はありません。
:::

バックアップの宛先は次のように指定されます:
```
S3('<S3エンドポイント>/<ディレクトリ>', '<アクセスキーID>', '<シークレットアクセスキー>')
```

```sql
CREATE TABLE data
(
    `key` Int,
    `value` String,
    `array` Array(String)
)
ENGINE = MergeTree
ORDER BY tuple()
```

```sql
INSERT INTO data SELECT *
FROM generateRandom('key Int, value String, array Array(String)')
LIMIT 1000
```

### ベース（初期）バックアップの作成

増分バックアップには開始するための_ベース_バックアップが必要です。この例を後でベースバックアップとして使用します。S3の宛先の最初のパラメータはS3エンドポイントで、その後はこのバックアップ用のバケット内のディレクトリです。この例ではディレクトリを`my_backup`としています。

```sql
BACKUP TABLE data TO S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_backup', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ de442b75-a66c-4a3c-a193-f76f278c70f3 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```

### データを追加する

増分バックアップは、ベースバックアップと現在のテーブル内容との間の差分で構成されます。増分バックアップを取る前にデータをさらに追加します:

```sql
INSERT INTO data SELECT *
FROM generateRandom('key Int, value String, array Array(String)')
LIMIT 100
```
### 増分バックアップを取る

このバックアップコマンドはベースバックアップと似ていますが、`SETTINGS base_backup`とベースバックアップの場所を追加します。増分バックアップの宛先はベースと同じディレクトリではなく、バケット内の異なるターゲットディレクトリであることに注意してください。ベースバックアップは`my_backup`にあり、増分バックアップは`my_incremental`に書き込まれます:
```sql
BACKUP TABLE data TO S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_incremental', 'ABC123', 'Abc+123') SETTINGS base_backup = S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_backup', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ f6cd3900-850f-41c9-94f1-0c4df33ea528 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```
### 増分バックアップから復元する

このコマンドは、増分バックアップを新しいテーブル`data3`に復元します。増分バックアップが復元されるとき、ベースバックアップも同様に含まれることに注意してください。復元する際には増分バックアップのみを指定します:
```sql
RESTORE TABLE data AS data3 FROM S3('https://mars-doc-test.s3.amazonaws.com/backup-S3/my_incremental', 'ABC123', 'Abc+123')
```

```response
┌─id───────────────────────────────────┬─status───┐
│ ff0c8c39-7dff-4324-a241-000796de11ca │ RESTORED │
└──────────────────────────────────────┴──────────┘
```

### 行数を確認する

元のテーブル`data`には最初に1,000行、次に100行のデータが挿入され、合計1,100行となります。復元されたテーブルが1,100行あることを確認します:
```sql
SELECT count()
FROM data3
```
```response
┌─count()─┐
│    1100 │
└─────────┘
```

### 内容を確認する
これにより、元のテーブル`data`と復元されたテーブル`data3`の内容が比較されます:
```sql
SELECT throwIf((
        SELECT groupArray(tuple(*))
        FROM data
    ) != (
        SELECT groupArray(tuple(*))
        FROM data3
    ), 'Data does not match after BACKUP/RESTORE')
```
## S3ディスクを使用してのバックアップ/復元

ClickHouseのストレージ設定でS3ディスクを設定することにより、S3への`BACKUP`/`RESTORE`を行うことも可能です。以下のようにディスクを設定するために、/etc/clickhouse-server/config.dにファイルを追加します:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_plain>
                <type>s3_plain</type>
                <endpoint></endpoint>
                <access_key_id></access_key_id>
                <secret_access_key></secret_access_key>
            </s3_plain>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3_plain</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>

    <backups>
        <allowed_disk>s3_plain</allowed_disk>
    </backups>
</clickhouse>
```

そして、通常通り`BACKUP`/`RESTORE`を実行します:

```sql
BACKUP TABLE data TO Disk('s3_plain', 'cloud_backup');
RESTORE TABLE data AS data_restored FROM Disk('s3_plain', 'cloud_backup');
```

:::note
しかし、次のことを覚えておいてください:
- このディスクは`MergeTree`自体には使用せず、`BACKUP`/`RESTORE`のみに使用するべきです。
- テーブルがS3ストレージでサポートされており、ディスクの種類が異なる場合、パーツを宛先バケットにコピーするための`CopyObject`コールを使用せず、ダウンロードおよびアップロードを行います。これは非常に非効率的です。このユースケースには`BACKUP ... TO S3(<endpoint>)`の構文を使用することを優先します。
:::

## 代替案

ClickHouseはデータをディスクに保存しており、ディスクのバックアップ方法はさまざまです。これらは過去に使用されてきた代替案であり、あなたの環境に適しているかもしれません。

### ソースデータを別の場所に複製する {#duplicating-source-data-somewhere-else}

ClickHouseに取り込まれるデータは、しばしば[Apache Kafka](https://kafka.apache.org)のような永続的なキューを通して配信されます。この場合、ClickHouseに書き込まれる間に同じデータストリームを読み取り、どこかに保存する追加のサブスクライバーセットを設定することが可能です。多くの企業はすでにオブジェクトストアや[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)のような分散ファイルシステムの、デフォルトの推奨コールドストレージを持っています。

### ファイルシステムのスナップショット {#filesystem-snapshots}

一部のローカルファイルシステムにはスナップショット機能があります（例：[ZFS](https://en.wikipedia.org/wiki/ZFS)）。ただし、これらはライブクエリの処理には最適でない場合があります。ある解決策は、このようなファイルシステムを持つ追加のレプリカを作成し、それを`SELECT`クエリに使用される[分散テーブル](../engines/table-engines/special/distributed.md)から除外することです。このようなレプリカ上のスナップショットは、データを変更するクエリの範囲外にあります。ボーナスとして、これらのレプリカは特別なハードウェア設定で、サーバー毎に多くのディスクが接続されている場合があり、コスト効果があります。

データ量が小さい場合、リモートテーブルへの単純な`INSERT INTO ... SELECT ...`がうまく機能することもあります。

### パーツの操作 {#manipulations-with-parts}

ClickHouseは`ALTER TABLE ... FREEZE PARTITION ...`クエリを使用してテーブルパーティションのローカルコピーを作成することを許可します。これは`/var/lib/clickhouse/shadow/`フォルダへのハードリンクを使用して実装されているため、通常は古いデータのために追加のディスクスペースを消費しません。作成されたファイルのコピーはClickHouseサーバーによって管理されないため、そこに残しておくことができます：追加の外部システムを必要とせずに単純なバックアップを持つことができ、しかしそれはハードウェアの問題に対しては依然として脆弱です。このため、それらを他の場所にリモートでコピーしてからローカルのコピーを削除するのが良いです。分散ファイルシステムとオブジェクトストアは依然として良い選択ですが、通常接続されているファイルサーバーの容量で十分であることもあります（この場合、転送はネットワークファイルシステムまたは[rsync](https://en.wikipedia.org/wiki/Rsync)経由で行われます）。
データは`ALTER TABLE ... ATTACH PARTITION ...`クエリを使用してバックアップから復元することができます。

パーティション操作に関連するクエリについて詳しくは、[ALTERドキュメント](../sql-reference/statements/alter/partition.md#alter_manipulations-with-partitions)を参照してください。

このアプローチを自動化するためのサードパーティツールが利用可能です：[clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)。

## 同時バックアップ/リストアを許可しない設定

同時バックアップ/リストアを許可しないようにするためには、それぞれの設定を使用することができます。

```xml
<clickhouse>
    <backups>
        <allow_concurrent_backups>false</allow_concurrent_backups>
        <allow_concurrent_restores>false</allow_concurrent_restores>
    </backups>
</clickhouse>
```

デフォルト値はどちらもtrueであり、デフォルトでは同時バックアップ/リストアが許可されています。
これらの設定がfalseの場合、クラスターで1度に実行できるバックアップ/リストアは1つのみです。

## AzureBlobStorage エンドポイントを使用するためのバックアップ/リストアの設定

AzureBlobStorageコンテナにバックアップを書くためには、次の情報が必要です:
- AzureBlobStorageのエンドポイント接続文字列 / URL,
- コンテナ,
- パス,
- アカウント名 (URLが指定された場合)
- アカウントキー (URLが指定された場合)

バックアップの宛先は次のように指定されます:
```
AzureBlobStorage('<接続文字列>/<URL>', '<コンテナ>', '<パス>', '<アカウント名>', '<アカウントキー>')
```

```sql
BACKUP TABLE data TO AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'test_container', 'data_backup');
RESTORE TABLE data AS data_restored FROM AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'test_container', 'data_backup');
```
