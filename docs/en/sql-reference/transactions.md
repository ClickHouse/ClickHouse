---
slug: /en/guides/developer/transactional
---
# Transactional (ACID) support

## Case 1: INSERT into one partition, of one table, of the MergeTree* family

This is transactional (ACID) if the inserted rows are packed and inserted as a single block (see Notes):
- Atomic: an INSERT succeeds or is rejected as a whole: if a confirmation is sent to the client, then all rows were inserted; if an error is sent to the client, then no rows were inserted.
- Consistent: if there are no table constraints violated, then all rows in an INSERT are inserted and the INSERT succeeds; if constraints are violated, then no rows are inserted.
- Isolated: concurrent clients observe a consistent snapshot of the table–the state of the table either as it was before the INSERT attempt, or after the successful INSERT; no partial state is seen. Clients inside of another transaction have [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation), while clients outside of a transaction have [read uncommitted](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_uncommitted) isolation level.
- Durable: a successful INSERT is written to the filesystem before answering to the client, on a single replica or multiple replicas (controlled by the `insert_quorum` setting), and ClickHouse can ask the OS to sync the filesystem data on the storage media (controlled by the `fsync_after_insert` setting).
- INSERT into multiple tables with one statement is possible if materialized views are involved (the INSERT from the client is to a table which has associate materialized views).

## Case 2: INSERT into multiple partitions, of one table, of the MergeTree* family

Same as Case 1 above, with this detail:
- If table has many partitions and INSERT covers many partitions, then insertion into every partition is transactional on its own


## Case 3: INSERT into one distributed table of the MergeTree* family

Same as Case 1 above, with this detail:
- INSERT into Distributed table is not transactional as a whole, while insertion into every shard is transactional

## Case 4: Using a Buffer table

- insert into Buffer tables is neither atomic nor isolated nor consistent nor durable

## Case 5: Using async_insert

Same as Case 1 above, with this detail:
- atomicity is ensured even if `async_insert` is enabled and `wait_for_async_insert` is set to 1 (the default), but if `wait_for_async_insert` is set to 0, then atomicity is not ensured.

## Notes
- rows inserted from the client in some data format are packed into a single block when:
  - the insert format is row-based (like CSV, TSV, Values, JSONEachRow, etc) and the data contains less then `max_insert_block_size` rows (~1 000 000 by default) or less then `min_chunk_bytes_for_parallel_parsing` bytes (10 MB by default) in case of parallel parsing is used (enabled by default)
  - the insert format is column-based (like Native, Parquet, ORC, etc) and the data contains only one block of data
- the size of the inserted block in general may depend on many settings (for example: `max_block_size`, `max_insert_block_size`, `min_insert_block_size_rows`, `min_insert_block_size_bytes`, `preferred_block_size_bytes`, etc)
- if the client did not receive an answer from the server, the client does not know if the transaction succeeded, and it can repeat the transaction, using exactly-once insertion properties
- ClickHouse is using [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) with [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) internally for concurrent transactions
- all ACID properties are valid even in the case of server kill/crash
- either insert_quorum into different AZ or fsync should be enabled to ensure durable inserts in the typical setup
- "consistency" in ACID terms does not cover the semantics of distributed systems, see https://jepsen.io/consistency which is controlled by different settings (select_sequential_consistency)
- this explanation does not cover a new transactions feature that allow to have full-featured transactions over multiple tables, materialized views, for multiple SELECTs, etc. (see the next section on Transactions, Commit, and Rollback)

## Transactions, Commit, and Rollback

In addition to the functionality described at the top of this document, ClickHouse has experimental support for transactions, commits, and rollback functionality.

### Requirements

- Deploy ClickHouse Keeper or ZooKeeper to track transactions
- Atomic DB only (Default)
- Non-Replicated MergeTree table engine only
- Enable experimental transaction support by adding this setting in `config.d/transactions.xml`:
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

### Notes
- This is an experimental feature, and changes should be expected.
- If an exception occurs during a transaction, you cannot commit the transaction.  This includes all exceptions, including `UNKNOWN_FUNCTION` exceptions caused by typos.  
- Nested transactions are not supported; finish the current transaction and start a new one instead

### Configuration

These examples are with a single node ClickHouse server with ClickHouse Keeper enabled.

#### Enable experimental transaction support

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

#### Basic configuration for a single ClickHouse server node with ClickHouse Keeper enabled

:::note
See the [deployment](docs/en/deployment-guides/terminology.md) documentation for details on deploying ClickHouse server and a proper quorum of ClickHouse Keeper nodes.  The configuration shown here is for experimental purposes.
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

### Example

#### Verify that experimental transactions are enabled

Issue a `BEGIN TRANSACTION` or `START TRANSACTION` followed by a `ROLLBACK` to verify that experimental transactions are enabled, and that ClickHouse Keeper is enabled as it is used to track transactions. 

```sql
BEGIN TRANSACTION
```
```response
Ok.
```

:::tip
If you see the following error, then check your configuration file to make sure that `allow_experimental_transactions` is set to `1` (or any value other than `0` or `false`).
```
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

You can also check ClickHouse Keeper by issuing
```
echo ruok | nc localhost 9181
```
ClickHouse Keeper should respond with `imok`.
:::

```sql
ROLLBACK
```
```response
Ok.
```

#### Create a table for testing

:::tip
Creation of tables is not transactional.  Run this DDL query outside of a transaction.
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

#### Begin a transaction and insert a row

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
You can query the table from within a transaction and see that the row was inserted even though it has not yet been committed.
:::

#### Rollback the transaction, and query the table again

Verify that the transaction is rolled back:
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

#### Complete a transaction and query the table again

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

### Transactions introspection

You can inspect transactions by querying the `system.transactions` table, but note that you cannot query that
table from a session that is in a transaction. Open a second `clickhouse client` session to query that table.

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

## More Details

See this [meta issue](https://github.com/ClickHouse/ClickHouse/issues/48794) to find much more extensive tests and to keep up to date with the progress.

