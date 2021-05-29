---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。副本 {#system_tables-replicas}

包含驻留在本地服务器上的复制表的信息和状态。
此表可用于监视。 该表对于每个已复制的\*表都包含一行。

示例:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        1970-01-01 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 1970-01-01 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

列:

-   `database` (`String`)-数据库名称
-   `table` (`String`)-表名
-   `engine` (`String`)-表引擎名称
-   `is_leader` (`UInt8`)-副本是否是领导者。
    一次只有一个副本可以成为领导者。 领导者负责选择要执行的后台合并。
    请注意，可以对任何可用且在ZK中具有会话的副本执行写操作，而不管该副本是否为leader。
-   `can_become_leader` (`UInt8`)-副本是否可以当选为领导者。
-   `is_readonly` (`UInt8`)-副本是否处于只读模式。
    如果配置没有ZooKeeper的部分，如果在ZooKeeper中重新初始化会话时发生未知错误，以及在ZooKeeper中重新初始化会话时发生未知错误，则此模式将打开。
-   `is_session_expired` (`UInt8`)-与ZooKeeper的会话已经过期。 基本上一样 `is_readonly`.
-   `future_parts` (`UInt32`)-由于尚未完成的插入或合并而显示的数据部分的数量。
-   `parts_to_check` (`UInt32`)-队列中用于验证的数据部分的数量。 如果怀疑零件可能已损坏，则将其放入验证队列。
-   `zookeeper_path` (`String`)-在ZooKeeper中的表数据路径。
-   `replica_name` (`String`)-在动物园管理员副本名称. 同一表的不同副本具有不同的名称。
-   `replica_path` (`String`)-在ZooKeeper中的副本数据的路径。 与连接相同 ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`)-表结构的版本号。 指示执行ALTER的次数。 如果副本有不同的版本，这意味着一些副本还没有做出所有的改变。
-   `queue_size` (`UInt32`)-等待执行的操作的队列大小。 操作包括插入数据块、合并和某些其他操作。 它通常与 `future_parts`.
-   `inserts_in_queue` (`UInt32`)-需要插入数据块的数量。 插入通常复制得相当快。 如果这个数字很大，这意味着有什么不对劲。
-   `merges_in_queue` (`UInt32`)-等待进行合并的数量。 有时合并时间很长，因此此值可能长时间大于零。
-   `part_mutations_in_queue` (`UInt32`）-等待进行的突变的数量。
-   `queue_oldest_time` (`DateTime`)-如果 `queue_size` 大于0，显示何时将最旧的操作添加到队列中。
-   `inserts_oldest_time` (`DateTime`）-看 `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`）-看 `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`）-看 `queue_oldest_time`

接下来的4列只有在有ZK活动会话的情况下才具有非零值。

-   `log_max_index` (`UInt64`)-一般活动日志中的最大条目数。
-   `log_pointer` (`UInt64`)-副本复制到其执行队列的常规活动日志中的最大条目数加一。 如果 `log_pointer` 比 `log_max_index`，有点不对劲。
-   `last_queue_update` (`DateTime`)-上次更新队列时。
-   `absolute_delay` (`UInt64`）-当前副本有多大滞后秒。
-   `total_replicas` (`UInt8`)-此表的已知副本总数。
-   `active_replicas` (`UInt8`)-在ZooKeeper中具有会话的此表的副本的数量（即正常运行的副本的数量）。

如果您请求所有列，表可能会工作得有点慢，因为每行都会从ZooKeeper进行几次读取。
如果您没有请求最后4列（log\_max\_index，log\_pointer，total\_replicas，active\_replicas），表工作得很快。

例如，您可以检查一切是否正常工作，如下所示:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

如果这个查询没有返回任何东西，这意味着一切都很好。
