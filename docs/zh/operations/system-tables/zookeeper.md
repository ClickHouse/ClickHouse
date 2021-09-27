---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# system.zookeeper {#system-zookeeper}

如果未配置ZooKeeper，则该表不存在。 允许从配置中定义的ZooKeeper集群读取数据。
查询必须具有 ‘path’ WHERE子句中的相等条件。 这是ZooKeeper中您想要获取数据的子路径。

查询 `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` 输出`/clickhouse`节点的对所有子路径的数据。
要输出所有根节点的数据，使用path= ‘/’.
如果在指定的路径 ‘path’ 不存在，将引发异常。

查询`SELECT * FROM system.zookeeper WHERE path IN ('/', '/clickhouse')` 输出`/` 和 `/clickhouse`节点上所有子节点的数据。
如果在指定的 ‘path’ 集合中有不存在的路径，将引发异常。
它可以用来做一批ZooKeeper路径查询。

列:

-   `name` (String) — 节点的名字。
-   `path` (String) — 节点的路径。
-   `value` (String) — 节点的值。 
-   `dataLength` (Int32) — 节点的值长度。
-   `numChildren` (Int32) — 子节点的个数。
-   `czxid` (Int64) — 创建该节点的事务ID。
-   `mzxid` (Int64) — 最后修改该节点的事务ID。
-   `pzxid` (Int64) — 最后删除或者增加子节点的事务ID。
-   `ctime` (DateTime) — 节点的创建时间。 
-   `mtime` (DateTime) — 节点的最后修改时间。 
-   `version` (Int32) — 节点版本：节点被修改的次数。
-   `cversion` (Int32) — 增加或删除子节点的个数。
-   `aversion` (Int32) — ACL的修改次数。
-   `ephemeralOwner` (Int64) — 针对临时节点，拥有该节点的事务ID。

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
[原文](https://clickhouse.tech/docs/zh/operations/system-tables/zookeeper) <!--hide-->
