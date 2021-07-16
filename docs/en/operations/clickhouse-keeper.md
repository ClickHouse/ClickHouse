---
toc_priority: 66
toc_title: ClickHouse Keeper
---

# [pre-production] clickhouse-keeper

ClickHouse server use [ZooKeeper](https://zookeeper.apache.org/) coordination system for data [replication](../engines/table-engines/mergetree-family/replication.md) and [distributed DDL](../sql-reference/distributed-ddl.md) queries execution. ClickHouse Keeper is an alternative coordination system compatible with ZooKeeper.

!!! warning "Warning"
    This feature currently in pre-production stage. We test it in our CI and on small internal installations.

## Implementation details

ZooKeeper is one of the first well-known open-source coordination systems. It's implemented in Java, has quite a simple and powerful data model. ZooKeeper's coordination algorithm called ZAB (ZooKeeper Atomic Broadcast) doesn't provide linearizability guarantees for reads, because each ZooKeeper node serves reads locally. Unlike ZooKeeper `clickhouse-keeper` written in C++ and use [RAFT algorithm](https://raft.github.io/) [implementation](https://github.com/eBay/NuRaft). This algorithm allows to have linearizability for reads and writes, has several open-source implementations in different languages.

By default, `clickhouse-keeper` provides the same guarantees as ZooKeeper (linearizable writes, non-linearizable reads). It has a compatible client-server protocol, so any standard ZooKeeper client can be used to interact with `clickhouse-keeper`. Snapshots and logs have an incompatible format with ZooKeeper, but `clickhouse-keeper-converter` tool allows to convert ZooKeeper data to `clickhouse-keeper` snapshot. Interserver protocol in `clickhouse-keeper` also incompatible with ZooKeeper so mixed ZooKeeper/clickhouse-keeper cluster is impossible.

## Configuration

`clickhouse-keeper` can be used as a standalone replacement for ZooKeeper or as an internal part of the `clickhouse-server`, but in both cases configuration is almost the same `.xml` file. The main `clickhouse-keeper` configuration tag is `<keeper_server>`. Keeper configuration has the following parameters:

-    `tcp_port` — the port for a client to connect (default for ZooKeeper is `2181`)
-    `tcp_port_secure` — the secure port for a client to connect
-    `server_id` — unique server id, each participant of the clickhouse-keeper cluster must have a unique number (1, 2, 3, and so on)
-    `log_storage_path` — path to coordination logs, better to store logs on the non-busy device (same for ZooKeeper)
-    `snapshot_storage_path` — path to coordination snapshots

Other common parameters are inherited from clickhouse-server config (`listen_host`, `logger` and so on).

Internal coordination settings are located in `<keeper_server>.<coordination_settings>` section:

-    `operation_timeout_ms` — timeout for a single client operation
-    `session_timeout_ms` — timeout for client session
-    `dead_session_check_period_ms` — how often clickhouse-keeper check dead sessions and remove them
-    `heart_beat_interval_ms` — how often a clickhouse-keeper leader will send heartbeats to followers
-    `election_timeout_lower_bound_ms` — if follower didn't receive heartbeats from the leader in this interval, then it can initiate leader election
-    `election_timeout_upper_bound_ms` — if follower didn't receive heartbeats from the leader in this interval, then it must initiate leader election
-    `rotate_log_storage_interval` — how many logs to store in a single file
-    `reserved_log_items` — how many coordination logs to store before compaction
-    `snapshot_distance` — how often clickhouse-keeper will create new snapshots (in the number of logs)
-    `snapshots_to_keep` — how many snapshots to keep
-    `stale_log_gap` — the threshold when leader consider follower as stale and send snapshot to it instead of logs
-    `force_sync` — call `fsync` on each write to coordination log
-    `raft_logs_level` — text logging level about coordination (trace, debug, and so on)
-    `shutdown_timeout` — wait to finish internal connections and shutdown
-    `startup_timeout` — if the server doesn't connect to other quorum participants in the specified timeout it will terminate

Quorum configuration is located in `<keeper_server>.<raft_configuration>` section and contain servers description. The only parameter for the whole quorum is `secure`, which enables encrypted connection for communication between quorum participants. The main parameters for each `<server>` are:

-    `id` — server_id in quorum
-    `hostname` — hostname where this server placed
-    `port` — port where this server listen for connections


Examples of configuration for quorum with three nodes can be found in [integration tests](https://github.com/ClickHouse/ClickHouse/tree/master/tests/integration) with `test_keeper_` prefix. Example configuration for server #1:

```xml
<keeper_server>
    <tcp_port>2181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

    <coordination_settings>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <session_timeout_ms>30000</session_timeout_ms>
        <raft_logs_level>trace</raft_logs_level>
    </coordination_settings>

    <raft_configuration>
        <server>
            <id>1</id>
            <hostname>zoo1</hostname>
            <port>9444</port>
        </server>
        <server>
            <id>2</id>
            <hostname>zoo2</hostname>
            <port>9444</port>
        </server>
        <server>
            <id>3</id>
            <hostname>zoo3</hostname>
            <port>9444</port>
        </server>
    </raft_configuration>
</keeper_server>
```

## How to run

`clickhouse-keeper` is bundled into `clickhouse-server` package, just add configuration of `<keeper_server>` and start clickhouse-server as always. If you want to run standalone `clickhouse-keeper` you can start it in a similar way with:

```bash
clickhouse-keeper --config /etc/your_path_to_config/config.xml --daemon
```

## [experimental] Migration from ZooKeeper

Seamlessly migration from ZooKeeper to `clickhouse-keeper` is impossible you have to stop your ZooKeeper cluster, convert data and start `clickhouse-keeper`. `clickhouse-keeper-converter` tool allows to convert ZooKeeper logs and snapshots to `clickhouse-keeper` snapshot. It works only with ZooKeeper > 3.4. Steps for migration:

1. Stop all ZooKeeper nodes.

2. [optional, but recommended] Found ZooKeeper leader node, start and stop it again. It will force ZooKeeper to create consistent snapshot.

3. Run `clickhouse-keeper-converter` on leader, example

```bash
clickhouse-keeper-converter --zookeeper-logs-dir /var/lib/zookeeper/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/version-2 --output-dir /path/to/clickhouse/keeper/snapshots
```

4. Copy snapshot to `clickhouse-server` nodes with configured `keeper` or start `clickhouse-keeper` instead of ZooKeeper. Snapshot must persist only on leader node, leader will sync it automatically to other nodes.

