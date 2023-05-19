---
slug: /zh/operations/system-tables/zookeeper_connection
---
#zookeeper_connection

This table does not exist if ClickHouse Cluster does not config zookeeper. The 'system.zookeeper_connection' table will shows current clickhouse-cluster connected zookeepers info.

each rows will shows the node information of a zk cluster connected to.

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — zookeeper cluster name.
-   `host` ([String](../../sql-reference/data-types/string.md)) — connected zookeeper node host.
-   `port` ([String](../../sql-reference/data-types/string.md)) — connected zookeeper node port.
-   `index` ([UInt8](../../sql-reference/data-types/int-uint.md)) — connected zookeeper node index in zk cluster config.
-   `connected_time` ([String](../../sql-reference/data-types/string.md)) — when establish the connection to this zk node.
-   `is_expired` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Does current connection expired.
-   `keeper_api_version` ([String](../../sql-reference/data-types/string.md)) — shows keeper api version.
-   `client_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — zookeeper connection session id.

Example:

``` sql
SELECT * FROM system.zookeeper_connection;
```

``` text
┌─name──────────────┬─host─────────┬─port─┬─index─┬──────connected_time─┬─is_expired─┬─keeper_api_version─┬──────────client_id─┐
│ default_zookeeper │ 127.0.0.1    │ 2181 │     0 │ 2023-05-19 14:30:16 │          0 │                  0 │ 216349144108826660 │
└───────────────────┴──────────────┴──────┴───────┴─────────────────────┴────────────┴────────────────────┴────────────────────┘
```
