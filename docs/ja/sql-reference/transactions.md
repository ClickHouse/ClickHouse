---
slug: /ja/guides/developer/transactional
---
# トランザクショナル (ACID) サポート

## ケース 1: MergeTree* ファミリーの一つのテーブルの一つのパーティションへのINSERT

行が単一のブロックとしてパックされて挿入される場合、これはトランザクショナル (ACID) である (注意参照):
- アトミック: INSERT が成功または全く拒否される: クライアントに確認が送信されると全ての行が挿入され、エラーが送信されると何も挿入されない。
- 一貫性: テーブルの制約が違反されない場合、INSERT 内のすべての行が挿入され、INSERT は成功する; 制約が違反される場合、何も挿入されない。
- 分離性: 同時実行するクライアントはテーブルの一貫したスナップショットを観察する–INSERT 以前の状態か、成功した後の状態; 部分的な状態は見えない。他のトランザクション内のクライアントは[スナップショット分離](https://en.wikipedia.org/wiki/Snapshot_isolation)を享受し、トランザクション外のクライアントは[未コミット読み取り](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_uncommitted)の分離レベルを持つ。
- 耐久性: 成功した INSERT はファイルシステムにクライアントへ応答する前に書き込まれ、単一または複数のレプリカに (これは `insert_quorum` 設定によって制御される)、ClickHouse はOSに格納メディア上のファイルシステムデータを同期するよう要求できる (これは `fsync_after_insert` 設定によって制御される)。
- 複数のテーブルへのINSERTは、マテリアライズドビューが含まれている場合に可能である (クライアントからINSERTされたものがマテリアライズドビューを持つテーブルである場合)。

## ケース 2: MergeTree* ファミリーの一つのテーブルの複数のパーティションへのINSERT

上記のケース 1と同様、詳細は以下の通り:
- テーブルに多数のパーティションがあり、INSERTが複数のパーティションをカバーする場合、各パーティションへの挿入が独立してトランザクショナルである


## ケース 3: MergeTree* ファミリーの一つの分散テーブルへのINSERT

上記のケース 1と同様、詳細は以下の通り:
- 分散テーブルへのINSERTは全体としてトランザクショナルではないが、各シャードへの挿入はトランザクショナルである

## ケース 4: Buffer テーブルの使用

- Buffer テーブルへの挿入は原子性も分離性も一貫性も耐久性もない

## ケース 5: async_insert の使用

上記のケース 1と同様、詳細は以下の通り:
- `async_insert` が有効で `wait_for_async_insert` が 1（デフォルト） の場合でも原子性が保証されるが、`wait_for_async_insert` が 0 に設定されている場合は原子性が保証されない。

## 注意
- クライアントから挿入された行は、あるデータフォーマットで以下の場合に単一のブロックにパックされる:
  - 挿入フォーマットが行ベースの場合（CSV、TSV、Values、JSONEachRowなど）で、データが `max_insert_block_size` 行未満（デフォルトでは約1,000,000）または `min_chunk_bytes_for_parallel_parsing` 未満のバイト数（デフォルトでは10 MB）の場合（並列解析が使用される場合、デフォルトで有効）
  - 挿入フォーマットがカラムベースの場合（Native、Parquet、ORCなど）で、データが一つのデータブロックのみを含む場合
- 挿入されたブロックのサイズは、一般的に多くの設定に依存する（例えば: `max_block_size`, `max_insert_block_size`, `min_insert_block_size_rows`, `min_insert_block_size_bytes`, `preferred_block_size_bytes` など）
- クライアントがサーバから応答を受け取らなかった場合、クライアントはトランザクションが成功したかどうかを知ることができず、トランザクションを繰り返すことができる（ちょうど一度だけの挿入特性を使用して）
- ClickHouse は内部で[MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)を使用し、同時トランザクションのための[スナップショット分離](https://en.wikipedia.org/wiki/Snapshot_isolation)を使用している
- すべてのACID 特性はサーバのキル/クラッシュの場合でも有効である
- 異なるAZへの `insert_quorum` または fsync のいずれかが有効であるべきである典型的な設定において耐久性のある挿入を保証するために
- ACID の「一貫性」は分散システムのセマンティクスをカバーしない、詳細は https://jepsen.io/consistency を参照。これは異なる設定によって制御される（`select_sequential_consistency`）
- これは新しいトランザクション機能をカバーしておらず、複数のテーブル、マテリアライズドビュー、複数のSELECTに対してフルフィーチャーのトランザクションを可能にする（次の「トランザクション、コミット、およびロールバック」セクションを参照）

## トランザクション、コミット、およびロールバック

このドキュメントの冒頭で説明した機能に加えて、ClickHouse はエクスペリメンタルなトランザクション、コミット、およびロールバック機能をサポートしています。

### 要件

- トランザクションを追跡するために ClickHouse Keeper または ZooKeeper を展開
- Atomic DB のみ（デフォルト）
- 非レプリケート MergeTree テーブルエンジンのみ
- `config.d/transactions.xml` に次の設定を追加してエクスペリメンタルなトランザクションサポートを有効化:
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

### 注意
- これはエクスペリメンタルな機能であり、変更が予想されます。
- トランザクション中に例外が発生した場合、そのトランザクションをコミットすることはできません。  これにはすべての例外が含まれ、タイプミスによる `UNKNOWN_FUNCTION` 例外も含まれます。
- ネストされたトランザクションはサポートされていません; 現在のトランザクションを終了し、新しいトランザクションを開始してください

### 設定

以下は単一ノード ClickHouse サーバで ClickHouse Keeper が有効になっている場合の例です。

#### エクスペリメンタルなトランザクションサポートを有効化

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

#### ClickHouse Keeper が有効な単一 ClickHouse サーバノードの基本設定

:::note
ClickHouse サーバと ClickHouse Keeper ノードの適切なクォーラムの展開に関する詳細は、[展開](docs/en/deployment-guides/terminology.md)ドキュメントを参照してください。ここに示された設定は実験目的のものです。
:::

```xml title=/etc/clickhouse-server/config.d/config.xml
<clickhouse replace="true">
    <logger>
        <level>debug</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
        <size>1000M</size>
        <count>3</count>
    </logger>
    <display_name>node 1</display_name>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <zookeeper>
        <node>
            <host>clickhouse-01</host>
            <port>9181</port>
        </node>
    </zookeeper>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>information</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>clickhouse-keeper-01</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

### 例

#### エクスペリメンタルトランザクションが有効であることを確認する

エクスペリメンタルトランザクションが有効であり、トランザクションを追跡するために ClickHouse Keeper が有効であることを確認するために、`BEGIN TRANSACTION` または `START TRANSACTION` に続けて `ROLLBACK` を実行します。

```sql
BEGIN TRANSACTION
```
```response
Ok.
```

:::tip
次のエラーが表示された場合は、構成ファイルを確認して `allow_experimental_transactions` が `1`（または `0` や `false` 以外の値）に設定されていることを確認してください。
```
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

ClickHouse Keeper を確認するには、次のコマンドを実行します:
```
echo ruok | nc localhost 9181
```
ClickHouse Keeper は `imok` と返答するはずです。
:::

```sql
ROLLBACK
```
```response
Ok.
```

#### テスト用のテーブルを作成する

:::tip
テーブルの作成はトランザクショナルではありません。この DDL クエリはトランザクション外で実行してください。
:::

```sql
CREATE TABLE mergetree_table
(
    `n` Int64
)
ENGINE = MergeTree
ORDER BY n
```
```response
Ok.
```

#### トランザクションを開始して行を挿入する

```sql
BEGIN TRANSACTION
```
```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (10)
```
```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```
```response
┌──n─┐
│ 10 │
└────┘
```
:::note
トランザクション内でテーブルをクエリすることで、まだコミットされていなくても行が挿入されたことが確認できます。
:::

#### トランザクションをロールバックし、テーブルを再度クエリする

トランザクションがロールバックされたことを確認します:
```sql
ROLLBACK
```
```response
Ok.
```
```sql
SELECT *
FROM mergetree_table
```
```response
Ok.

0 rows in set. Elapsed: 0.002 sec.
```

#### トランザクションを完了し、再度テーブルをクエリする

```sql
BEGIN TRANSACTION
```
```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (42)
```
```response
Ok.
```

```sql
COMMIT
```
```response
Ok. Elapsed: 0.002 sec.
```

```sql
SELECT *
FROM mergetree_table
```
```response
┌──n─┐
│ 42 │
└────┘
```

### トランザクションの内省

`system.transactions` テーブルをクエリすることでトランザクションを調べることができますが、トランザクション内のセッションからそのテーブルをクエリすることはできません。そのテーブルをクエリするために2つ目の `clickhouse client` セッションを開いてください。

```sql
SELECT *
FROM system.transactions
FORMAT Vertical
```
```response
Row 1:
──────
tid:         (33,61,'51e60bce-6b82-4732-9e1d-b40705ae9ab8')
tid_hash:    11240433987908122467
elapsed:     210.017820947
is_readonly: 1
state:       RUNNING
```

## 詳細

この[メタ問題](https://github.com/ClickHouse/ClickHouse/issues/48794)を参照して、より詳細なテストを見つけ、進捗を最新に保ってください。
