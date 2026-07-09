---
description: '介绍 ClickHouse 事务（ACID）支持的页面'
slug: /guides/developer/transactional
title: '事务（ACID）支持'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="transactional-acid-support">
  # 事务 (ACID) 支持
</div>

<div id="case-1-insert-into-one-partition-of-one-table-of-the-mergetree-family">
  ## 情况 1：向 MergeTree* 家族中某个表的单个分区执行 INSERT
</div>

如果插入的行被打包为单个块并一次性插入 (见说明) ，则该操作具有事务性 (ACID) ：

* 原子性：一次 INSERT 要么整体成功，要么整体被拒绝：如果向客户端发送了确认，则表示所有行都已插入；如果向客户端返回了 error，则表示没有任何行被插入。
* 一致性：如果未违反表约束，则一次 INSERT 中的所有行都会被插入，并且 INSERT 成功；如果违反了约束，则不会插入任何行。
* 隔离性：并发客户端看到的是表的一致性快照——表的状态要么是发起 INSERT 之前的状态，要么是 INSERT 成功之后的状态；不会看到中间的部分状态。处于另一个事务中的客户端具有[快照隔离](https://en.wikipedia.org/wiki/Snapshot_isolation)，而事务之外的客户端则具有[读未提交](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Read_uncommitted)隔离级别。
* 持久性：成功的 INSERT 会在响应客户端之前写入 filesystem，可写入单个副本或多个副本 (由 `insert_quorum` 设置控制) ；此外，ClickHouse 还可以要求操作系统将 filesystem 中的数据同步到存储介质 (由 `fsync_after_insert` 设置控制) 。
* 如果涉及 materialized view，则可以通过一条语句向多个表执行 INSERT (即客户端将 INSERT 发送到一个具有关联 materialized view 的表) 。

<div id="case-2-insert-into-multiple-partitions-of-one-table-of-the-mergetree-family">
  ## 案例 2：向 MergeTree* 家族中的一个表的多个分区执行 INSERT
</div>

与上面的案例 1 相同，但有一点区别：

* 如果表有很多分区，且 INSERT 涉及多个分区，那么对每个分区的插入都是各自独立具备事务性的

<div id="case-3-insert-into-one-distributed-table-of-the-mergetree-family">
  ## 案例 3：向一个 MergeTree* 家族的分布式表执行 INSERT
</div>

与上面的案例 1 相同，但有以下细节：

* 向 Distributed 表执行 INSERT 整体上不具备事务性，而向每个分片的插入则具备事务性

<div id="case-4-using-a-buffer-table">
  ## 案例 4：使用 Buffer 表
</div>

* 向 Buffer 表中 insert 数据既不具备原子性，也不具备隔离性、一致性或持久性

<div id="case-5-using-async_insert">
  ## 案例 5：使用 async_insert
</div>

与上面的案例 1 相同，但有以下细节：

* 即使启用了 `async_insert`，且 `wait_for_async_insert` 设为 1 (默认值) ，也能保证原子性；但如果 `wait_for_async_insert` 设为 0，则无法保证原子性。

<div id="notes">
  ## 注意事项
</div>

* 客户端以某种数据格式插入的行会在以下情况下打包成一个块：
  * 插入格式为基于行的格式 (如 CSV、TSV、Values、JSONEachRow 等) ，且数据行数少于 `max_insert_block_size` (默认约 1 000 000 行) ；或者在使用并行解析时 (默认启用) ，数据大小小于 `min_chunk_bytes_for_parallel_parsing` 字节 (默认 10 MB)
  * 插入格式为基于列的格式 (如 Native、Parquet、ORC 等) ，且数据只包含一个数据块
* 一般来说，插入块的大小可能取决于许多设置 (例如：`max_block_size`、`max_insert_block_size`、`min_insert_block_size_rows`、`min_insert_block_size_bytes`、`preferred_block_size_bytes` 等)
* 如果客户端没有收到服务器响应，它无法得知事务是否成功，因此可以利用 exactly-once 插入特性重试该事务
* ClickHouse 内部对并发事务使用了 [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) 和 [快照隔离](https://en.wikipedia.org/wiki/Snapshot_isolation)
* 即使在服务器被强制终止或崩溃的情况下，所有 ACID 属性也依然成立
* 在典型部署中，应启用跨不同 AZ 的 insert&#95;quorum 或 fsync，以确保插入具备持久性
* ACID 术语中的“consistency”不涵盖分布式系统语义，参见 https://jepsen.io/consistency；这由其他设置 (select&#95;sequential&#95;consistency) 控制
* 本说明未涵盖新的事务功能；该功能支持跨多个表、materialized views、多个 SELECT 等进行完整事务处理。 (请参阅下一节 Transactions、Commit 和 Rollback)

<div id="transactions-commit-and-rollback">
  ## 事务、提交和回滚
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

除本文档开头所述的功能外，ClickHouse 还提供对事务、提交和回滚的 Experimental 支持。

<div id="requirements">
  ### 要求
</div>

* 部署 ClickHouse Keeper 或 ZooKeeper 以跟踪事务
* 仅支持 Atomic DB (默认)
* 仅支持非复制的 MergeTree 表引擎
* 在 `config.d/transactions.xml` 中添加以下设置，以启用 Experimental 事务支持：
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

<div id="notes-1">
  ### 注意事项
</div>

* 这是一项 Experimental 功能，后续可能会有变动。
* 如果事务过程中发生异常，则无法提交该事务。这包括所有异常，也包括因拼写错误导致的 `UNKNOWN_FUNCTION` 异常。
* 不支持嵌套事务；请先结束当前事务，再启动新事务

<div id="configuration">
  ### 配置
</div>

以下示例均基于启用了 ClickHouse Keeper 的单节点 ClickHouse 服务器。

<div id="enable-experimental-transaction-support">
  #### 启用 Experimental 事务支持
</div>

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

<div id="basic-configuration-for-a-single-clickhouse-server-node-with-clickhouse-keeper-enabled">
  #### 启用 ClickHouse Keeper 的单个 ClickHouse server 节点基础配置
</div>

:::note
有关部署 ClickHouse server 以及满足适当仲裁数量的 ClickHouse Keeper 节点的详细信息，请参阅[部署](/zh/deployment-guides/terminology.md)文档。此处展示的配置仅供实验使用。
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
  ### 示例
</div>

<div id="verify-that-experimental-transactions-are-enabled">
  #### 验证是否已启用 Experimental 事务
</div>

执行 `BEGIN TRANSACTION` 或 `START TRANSACTION`，然后再执行 `ROLLBACK`，以验证是否已启用 Experimental 事务，以及是否已启用 ClickHouse Keeper，因为它用于跟踪事务。

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

:::tip
如果出现以下错误，请检查配置文件，确认 `allow_experimental_transactions` 已设置为 `1` (或除 `0` 或 `false` 以外的任意值) 。

```response
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

你还可以通过执行以下命令来检查 ClickHouse Keeper

```bash
echo ruok | nc localhost 9181
```

ClickHouse Keeper 应响应 `imok`。
:::

```sql
ROLLBACK
```

```response
Ok.
```

<div id="create-a-table-for-testing">
  #### 创建用于测试的表
</div>

:::tip
创建表不支持事务。请在事务外运行此 DDL 查询。
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
  #### 开启事务并插入一行
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
你可以在事务内查询该表，并看到该行已经插入，尽管这次插入操作尚未提交。
:::

<div id="rollback-the-transaction-and-query-the-table-again">
  #### 回滚事务，并再次查询表
</div>

确认事务已回滚：

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
  #### 完成事务并再次查询该表
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
  ### 事务内部信息
</div>

你可以通过查询 `system.transactions` 表来查看事务信息，但请注意，不能在处于事务中的会话内查询该
表。请另开一个 `clickhouse client` 会话来查询该表。

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
  ## 更多细节
</div>

请参阅这个 [meta issue](https://github.com/ClickHouse/ClickHouse/issues/48794)，查看更全面的测试，并及时了解最新进展。