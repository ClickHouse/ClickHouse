# system.zookeeper_log {#system-zookeeper_log}

The table does not exist if ZooKeeper is not configured.

This table contains information about the parameters of the request to the ZooKeeper client and the response from it.

For requests, only columns with request parameters are filled in, and the remaining columns are filled with default values (`0` or NULL). When the response arrives, the data from the response is added to the other columns.

Columns with request parameters:

-   `type` ([Enum](../../sql-reference/data-types/enum.md)) — Event type in the ZooKeeper client. Can have one of the following values:
    -   `request` — The request has been sent.
    -   `response` — The response was received.
    -   `finalize` — The connection is lost, no response was received.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — The date when the request was completed.
-   `event_time` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — The date and time when the request was completed.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that was used to make the request.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — Host port.
-   `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — The session ID that the ZooKeeper server sets for each connection.
-   `xid` ([Int32](../../sql-reference/data-types/int-uint.md)) — The ID of the request within the session. This is usually a sequential request number. It is the same for the request line and the paired `response`/`finalize` line.
-   `has_watch` ([UInt8](../../sql-reference/data-types/int-uint.md)) — The request whether the [watch](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) has been installed.
-   `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — The request or response type.
-   `path` ([String](../../sql-reference/data-types/string.md)) — The path to the ZooKeeper node specified in the request (if the request requires specifying a path) or an empty string.
-   `data` ([String](../../sql-reference/data-types/string.md)) — The data written to the ZooKeeper node (for the `SET` and `CREATE` requests — what the request wanted to write, for the response to the `GET` request — what was read) or an empty string.
-   `is_ephemeral` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Is the ZooKeeper node being created as an [ephemeral](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#Ephemeral+Nodes).
-   `is_sequential` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Is the ZooKeeper node being created as an [sequential](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#Sequence+Nodes+--+Unique+Naming).
-   `version` ([Nullable(Int32)](../../sql-reference/data-types/nullable.md)) — The version of the ZooKeeper node that the request expects when executing (for `CHECK`, `SET`, `REMOVE` requests; `-1` if the request does not check the version) or NULL for other requests that do not support version checking.
-   `requests_size` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of requests included in the "multi" request (this is a special request that consists of several consecutive ordinary requests and executes them atomically). All requests included in "multi" request will have the same `xid`.
-   `request_idx` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of the request included in multi (for multi — `0`, then in order from `1`).

Columns with request response parameters:

-   `zxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — ZooKeeper transaction id. The serial number issued by the ZooKeeper server in response to a successfully executed request (`0` if the request was not executed/returned an error/the client does not know whether the request was executed).
-   `error` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — Error code. Can have one of the following values:
    -   `ZOK` — The response to the request was received.
    -   `ZCONNECTIONLOSS` — The connection was lost.
    -   `ZOPERATIONTIMEOUT` — The request execution timeout has expired.
	-   `ZSESSIONEXPIRED` — The session has expired.
    -   `NULL` — The request is completed.
-   `watch_type` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — The type of the "watch" event (for responses with `op_num` = `Watch`), for the remaining responses: NULL.
-   `watch_state` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — The status of the "watch" event (for responses with `op_num` = `Watch`), for the remaining responses: NULL.
-   `path_created` ([String](../../sql-reference/data-types/string.md)) — The path to the created ZooKeeper node (for responses to the `CREATE` request), may differ from the `path` if the node is created as a sequential.
-   `stat_czxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — The `zxid` of the change that caused this ZooKeeper node to be created.
-   `stat_mzxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — The `zxid` of the change that last modified this ZooKeeper node.
-   `stat_pzxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — The transaction id of the change that last modified childern of this ZooKeeper node.
-   `stat_version` ([Int32](../../sql-reference/data-types/int-uint.md)) — The number of changes to the data of this ZooKeeper node.
-   `stat_cversion` ([Int32](../../sql-reference/data-types/int-uint.md)) — The number of changes to the children of this ZooKeeper node.
-   `stat_dataLength` ([Int32](../../sql-reference/data-types/int-uint.md)) — The length of the data field of this ZooKeeper node.
-   `stat_numChildren` ([Int32](../../sql-reference/data-types/int-uint.md)) — The number of children of this ZooKeeper node.
-   `children` ([Array(String)](../../sql-reference/data-types/array.md)) — The list of child ZooKeeper nodes (for responses to `LIST` request).

**Example**

Query:

``` sql
SELECT * FROM system.zookeeper_log WHERE (session_id = '106662742089334927') AND (xid = '10858') FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
type:             Request
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.291792
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:             
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             0
error:            ᴺᵁᴸᴸ
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:     
stat_czxid:       0
stat_mzxid:       0
stat_pzxid:       0
stat_version:     0
stat_cversion:    0
stat_dataLength:  0
stat_numChildren: 0
children:         []

Row 2:
──────
type:             Response
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.292086
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:             
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             16926267
error:            ZOK
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:     
stat_czxid:       16925469
stat_mzxid:       16925469
stat_pzxid:       16926179
stat_version:     0
stat_cversion:    7
stat_dataLength:  0
stat_numChildren: 7
children:         ['query-0000000006','query-0000000005','query-0000000004','query-0000000003','query-0000000002','query-0000000001','query-0000000000']
```

**See Also**

-   [ZooKeeper](../../operations/tips.md#zookeeper)
-   [ZooKeeper guide](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
