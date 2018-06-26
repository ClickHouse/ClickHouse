# system.zookeeper

This table presents when ZooKeeper is configured. It allows reading data from the ZooKeeper cluster defined in the config.
The query must have a 'path' equality condition in the WHERE clause. This is the path in ZooKeeper for the children that you want to get data for.

The query `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` outputs data for all children on the `/clickhouse` node.
To output data for all root nodes, write path = '/'.
If the path specified in 'path' doesn't exist, an exception will be thrown.

Columns:

- `name String` — Name of the node.
- `path String` — Path to the node.
- `value String` — Value of the node.
- `dataLength Int32` — Size of the value.
- `numChildren Int32` — Number of children.
- `czxid Int64` — ID of the transaction that created the node.
- `mzxid Int64` — ID of the transaction that last changed the node.
- `pzxid Int64` — ID of the transaction that last added or removed children.
- `ctime DateTime` — Time of node creation.
- `mtime DateTime` — Time of the last node modification.
- `version Int32` — Node version - the number of times the node was changed.
- `cversion Int32` — Number of added or removed children.
- `aversion Int32` — Number of changes to ACL.
- `ephemeralOwner Int64` — For ephemeral nodes, the ID of the session that owns this node.


Example:

```sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

```text
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
