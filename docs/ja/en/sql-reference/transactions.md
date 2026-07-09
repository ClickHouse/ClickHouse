---
description: 'ClickHouse のトランザクション（ACID）サポートについて説明するページ'
slug: /guides/developer/transactional
title: 'トランザクション（ACID）サポート'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="transactional-acid-support">
  # トランザクション (ACID) サポート
</div>

<div id="case-1-insert-into-one-partition-of-one-table-of-the-mergetree-family">
  ## ケース1: MergeTree* ファミリーの1つのテーブルの1つのパーティションへの INSERT
</div>

挿入される行が packed され、単一の block として挿入される場合、これは トランザクショナル (ACID) です (注を参照) :

* Atomic: INSERT は全体として成功するか、全体として拒否されます。クライアントに確認が送信された場合は、すべての行が挿入されています。クライアントに error が送信された場合は、1行も挿入されていません。
* Consistent: テーブルの制約に違反がなければ、INSERT 内のすべての行が挿入され、INSERT は成功します。制約に違反した場合は、1行も挿入されません。
* Isolated: 同時実行中のクライアントには、テーブルの一貫したスナップショット、つまり INSERT の試行前または INSERT の成功後のいずれかの状態だけが見え、部分的な状態は見えません。別のトランザクション内にいるクライアントは [スナップショット分離](https://en.wikipedia.org/wiki/Snapshot_isolation) ですが、トランザクションの外にいるクライアントの分離レベルは [read uncommitted](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Read_uncommitted) です。
* Durable: 成功した INSERT は、単一のレプリカまたは複数のレプリカ (`insert_quorum` setting で制御) において、クライアントに応答する前にファイルシステムへ書き込まれます。また ClickHouse は、OS に対してファイルシステム上のデータをストレージ媒体へ同期するよう要求できます (`fsync_after_insert` setting で制御) 。
* materialized view が関与する場合、1つのステートメントで複数のテーブルに INSERT することも可能です (クライアントからの INSERT 先は、関連付けられた materialized view を持つテーブルです) 。

<div id="case-2-insert-into-multiple-partitions-of-one-table-of-the-mergetree-family">
  ## ケース 2: 1 つの MergeTree* ファミリーのテーブルの複数パーティションへの INSERT
</div>

上記のケース 1 と同じですが、次の点が異なります。

* テーブルに多数のパーティションがあり、INSERT が複数のパーティションにまたがる場合、各パーティションへの挿入はそれぞれ独立してトランザクショナルになります

<div id="case-3-insert-into-one-distributed-table-of-the-mergetree-family">
  ## ケース 3: MergeTree* ファミリーの 1 つの Distributed テーブルへの INSERT
</div>

上記のケース 1 と同じですが、次の点が異なります。

* Distributed テーブルへの INSERT は全体としてはトランザクショナルではありませんが、各分片への挿入自体はトランザクショナルです

<div id="case-4-using-a-buffer-table">
  ## ケース 4: Buffer テーブルを使用する
</div>

* Buffer テーブルへの insert は、原子性、分離性、一貫性、永続性のいずれも満たしません

<div id="case-5-using-async_insert">
  ## ケース 5: `async_insert` の使用
</div>

基本的には上記のケース 1 と同じですが、次の点が異なります。

* `async_insert` が有効で、`wait_for_async_insert` が 1 (デフォルト) に設定されている場合はアトミック性が保証されます。一方、`wait_for_async_insert` が 0 に設定されている場合は、アトミック性は保証されません。

<div id="notes">
  ## 注記
</div>

* 一部のデータフォーマットでは、クライアントから挿入された行は次の場合に 1 つの block に packed されます:
  * insert フォーマットが行ベース (CSV、TSV、Values、JSONEachRow など) で、データの行数が `max_insert_block_size` 未満 (デフォルトでは約 1 000 000) であるか、並列パースを使用している場合 (デフォルトで有効) にデータサイズが `min_chunk_bytes_for_parallel_parsing` 未満 (デフォルトでは 10 MB) である場合
  * insert フォーマットがカラムベース (Native、Parquet、ORC など) で、データに含まれる block が 1 つだけである場合
* 挿入される block のサイズは、一般に多くの Settings に依存します (例: `max_block_size`、`max_insert_block_size`、`min_insert_block_size_rows`、`min_insert_block_size_bytes`、`preferred_block_size_bytes` など)
* クライアントがサーバーから応答を受信しなかった場合、トランザクションが成功したかどうかをクライアントは判断できないため、exactly-once insertion の特性を利用してトランザクションを再試行できます
* ClickHouse は、同時実行トランザクションのために内部的に [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) と [スナップショット分離](https://en.wikipedia.org/wiki/Snapshot_isolation) を使用しています
* すべての ACID 特性は、サーバーが kill された場合やクラッシュした場合でも有効です
* 一般的な構成で durable inserts を確保するには、異なる AZ への insert&#95;quorum または fsync のいずれかを有効にする必要があります
* ACID における &quot;consistency&quot; は分散システムのセマンティクスまでは対象としていません。https://jepsen.io/consistency を参照してください。これは別の設定 (select&#95;sequential&#95;consistency) で制御されます
* この説明では、複数の table、materialized view、複数の SELECT などにまたがるフル機能のトランザクションを可能にする新しい transactions 機能は扱っていません (Transactions、Commit、Rollback に関する次のセクションを参照してください)

<div id="transactions-commit-and-rollback">
  ## トランザクション、コミット、ロールバック
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

ClickHouse は、このドキュメントの冒頭で説明した機能に加え、トランザクション、コミット、ロールバックを実験的にサポートしています。

<div id="requirements">
  ### 要件
</div>

* トランザクションを追跡するため、ClickHouse Keeper または ZooKeeper をデプロイします
* Atomic DB のみ (デフォルト)
* 非レプリケーションの MergeTree テーブルエンジンのみ
* `config.d/transactions.xml` に次の設定を追加して、実験的なトランザクションサポートを有効にします:
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

<div id="notes-1">
  ### 注意事項
</div>

* これは実験的な機能であり、今後変更される可能性があります。
* トランザクション中に例外が発生した場合、そのトランザクションはコミットできません。これには、タイプミスによって発生する `UNKNOWN_FUNCTION` 例外を含む、すべての例外が該当します。
* ネストされたトランザクションはサポートされていません。現在のトランザクションを完了してから、新しいトランザクションを開始してください

<div id="configuration">
  ### 設定
</div>

以下の例では、ClickHouse Keeper を有効にした単一ノード構成の ClickHouse server を使用します。

<div id="enable-experimental-transaction-support">
  #### トランザクションの実験的サポートを有効にする
</div>

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

<div id="basic-configuration-for-a-single-clickhouse-server-node-with-clickhouse-keeper-enabled">
  #### ClickHouse Keeper を有効にした単一の ClickHouse server ノードの基本構成
</div>

:::note
ClickHouse server と、適切なクォーラムを満たす ClickHouse Keeper ノードのデプロイについて詳しくは、[deployment](/ja/deployment-guides/terminology.md) ドキュメントを参照してください。ここで示す構成は実験用です。
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

<div id="example">
  ### 例
</div>

<div id="verify-that-experimental-transactions-are-enabled">
  #### 実験的なトランザクションが有効になっていることを確認する
</div>

実験的なトランザクションと、トランザクションの追跡に使用される ClickHouse Keeper が有効になっていることを確認するには、`BEGIN TRANSACTION` または `START TRANSACTION` を実行し、続けて `ROLLBACK` を実行します。

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

:::tip
次のエラーが表示された場合は、`allow_experimental_transactions` が `1` (または `0` や `false` 以外の任意の値) に設定されていることを確認するため、設定ファイルを確認してください。

```response
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

次のコマンドを実行して ClickHouse Keeper を確認することもできます

```bash
echo ruok | nc localhost 9181
```

ClickHouse Keeper は `imok` と応答するはずです。
:::

```sql
ROLLBACK
```

```response
Ok.
```

<div id="create-a-table-for-testing">
  #### テスト用のテーブルを作成する
</div>

:::tip
テーブルの作成はトランザクションには対応していません。この DDL クエリはトランザクションの外で実行してください。
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

<div id="begin-a-transaction-and-insert-a-row">
  #### トランザクションを開始し、1行を挿入する
</div>

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
トランザクション内でテーブルにクエリを実行すると、まだコミットされていなくても、その行が挿入されていることを確認できます。
:::

<div id="rollback-the-transaction-and-query-the-table-again">
  #### トランザクションをロールバックし、再度テーブルにクエリを実行する
</div>

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

<div id="complete-a-transaction-and-query-the-table-again">
  #### トランザクションを完了し、再度テーブルにクエリを実行する
</div>

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

<div id="transactions-introspection">
  ### トランザクションの内部情報の確認
</div>

`system.transactions` テーブルをクエリするとトランザクションを確認できますが、トランザクション中のセッションからはそのテーブルをクエリできない点に注意してください。そのテーブルをクエリするには、2 つ目の `clickhouse client` セッションを開いてください。

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

<div id="more-details">
  ## 詳細情報
</div>

さらに広範なテストや最新の進捗については、この[meta issue](https://github.com/ClickHouse/ClickHouse/issues/48794)を参照してください。