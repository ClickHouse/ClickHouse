# system.zookeeper {#system-zookeeper}

如果未配置ZooKeeper，则表不存在。 允许从配置中定义的ZooKeeper集群读取数据。
查询WHERE子句中必须具有 ‘path’ 的相等条件或者在某个集合中的条件。 这对应于您想要获取数据的ZooKeeper中子节点的路径。

查询 `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` 输出节点 `/clickhouse` 上所有子节点的数据。
要输出所有根节点的数据，请写 path= ‘/’。
如果在‘path’ 中指定的路径不存在，将引发异常。

查询`SELECT * FROM system.zookeeper WHERE path IN ('/', '/clickhouse')` 输出节点 `/` 和 `/clickhouse`上所有子节点的数据。
如果在指定的 ‘path’ 集合中有不存在的路径，将引发异常。
它可用于执行一批ZooKeeper路径查询。

列:

-   `name` (String) — 节点的名称。
-   `path` (String) — 节点的路径。
-   `value` (String) — 节点值。
-   `dataLength` (Int32) — 值的大小。
-   `numChildren` (Int32) — 子节点的数量。
-   `czxid` (Int64) — 创建节点的事务的ID。
-   `mzxid` (Int64) — 上次更改节点的事务的ID。
-   `pzxid` (Int64) — 上次删除或添加子节点的事务的ID。
-   `ctime` (DateTime) — 节点创建的时间。
-   `mtime` (DateTime) — 节点上一次修改的时间。
-   `version` (Int32) — 节点版本：更改节点的次数。
-   `cversion` (Int32) — 添加或删除的子节点的数量。
-   `aversion` (Int32) — ACL的更改次数。
-   `ephemeralOwner` (Int64) — 对于临时节点，拥有此节点的会话的ID。

示例:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```
[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/zookeeper) <!--hide-->
