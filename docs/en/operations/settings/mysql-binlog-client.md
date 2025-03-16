# The MySQL Binlog Client

The MySQL Binlog Client provides a mechanism in ClickHouse to share the binlog from a MySQL instance among multiple [MaterializedMySQL](../../engines/database-engines/materialized-mysql.md) databases. This avoids consuming unnecessary bandwidth and CPU when replicating more than one schema/database.

The implementation is resilient against crashes and disk issues. The executed GTID sets of the binlog itself and the consuming databases have persisted only after the data they describe has been safely persisted as well. The implementation also tolerates re-doing aborted operations (at-least-once delivery).

# Settings

## use_binlog_client

Forces to reuse existing MySQL binlog connection or creates new one if does not exist. The connection is defined by `user:pass@host:port`.

Default value: 0

**Example**

```sql
-- create MaterializedMySQL databases that read the events from the binlog client
CREATE DATABASE db1 ENGINE = MaterializedMySQL('host:port', 'db1', 'user', 'password') SETTINGS use_binlog_client=1
CREATE DATABASE db2 ENGINE = MaterializedMySQL('host:port', 'db2', 'user', 'password') SETTINGS use_binlog_client=1
CREATE DATABASE db3 ENGINE = MaterializedMySQL('host:port', 'db3', 'user2', 'password2') SETTINGS use_binlog_client=1
```

Databases `db1` and `db2` will use the same binlog connection, since they use the same `user:pass@host:port`. Database `db3` will use separate binlog connection.

## max_bytes_in_binlog_queue

Defines the limit of bytes in the events binlog queue. If bytes in the queue increases this limit, it will stop reading new events from MySQL until the space for new events will be freed. This introduces the memory limits. Very high value could consume all available memory. Very low value could make the databases to wait for new events.

Default value: 67108864

**Example**

```sql
CREATE DATABASE db1 ENGINE = MaterializedMySQL('host:port', 'db1', 'user', 'password') SETTINGS use_binlog_client=1, max_bytes_in_binlog_queue=33554432
CREATE DATABASE db2 ENGINE = MaterializedMySQL('host:port', 'db2', 'user', 'password') SETTINGS use_binlog_client=1
```

If database `db1` is unable to consume binlog events fast enough and the size of the events queue exceeds `33554432` bytes, reading of new events from MySQL is postponed until `db1`
consumes the events and releases some space.

NOTE: This will impact to `db2`, and it will be waiting for new events too, since they share the same connection.

## max_milliseconds_to_wait_in_binlog_queue

Defines the max milliseconds to wait when `max_bytes_in_binlog_queue` exceeded. After that it will detach the database from current binlog connection and will retry establish new one to prevent other databases to wait for this database.

Default value: 10000

**Example**

```sql
CREATE DATABASE db1 ENGINE = MaterializedMySQL('host:port', 'db1', 'user', 'password') SETTINGS use_binlog_client=1, max_bytes_in_binlog_queue=33554432, max_milliseconds_to_wait_in_binlog_queue=1000
CREATE DATABASE db2 ENGINE = MaterializedMySQL('host:port', 'db2', 'user', 'password') SETTINGS use_binlog_client=1
```

If the event queue of database `db1` is full, the binlog connection will be waiting in `1000`ms and if the database is not able to consume the events, it will be detached from the connection to create another one.

NOTE: If the database `db1` has been detached from the shared connection and created new one, after the binlog connections for `db1` and `db2` have the same positions they will be merged to one. And `db1` and `db2` will use the same connection again.

## max_bytes_in_binlog_dispatcher_buffer

Defines the max bytes in the binlog dispatcher's buffer before it is flushed to attached binlog. The events from MySQL binlog connection are buffered before sending to attached databases. It increases the events throughput from the binlog to databases.

Default value: 1048576

## max_flush_milliseconds_in_binlog_dispatcher

Defines the max milliseconds in the binlog dispatcher's buffer to wait before it is flushed to attached binlog. If there are no events received from MySQL binlog connection for a while, after some time buffered events should be sent to the attached databases.

Default value: 1000

# Design

## The Binlog Events Dispatcher

Currently each MaterializedMySQL database opens its own connection to MySQL to subscribe to binlog events. There is a need to have only one connection and _dispatch_ the binlog events to all databases that replicate from the same MySQL instance.

## Each MaterializedMySQL Database Has Its Own Event Queue

To prevent slowing down other instances there should be an _event queue_ per MaterializedMySQL database to handle the events independently of the speed of other instances. The dispatcher reads an event from the binlog, and sends it to every MaterializedMySQL database that needs it. Each database handles its events in separate threads.

## Catching up

If several databases have the same binlog position, they can use the same dispatcher. If a newly created database (or one that has been detached for some time) requests events that have been already processed, we need to create another communication _channel_ to the binlog. We do this by creating another temporary dispatcher for such databases. When the new dispatcher _catches up with_ the old one, the new/temporary dispatcher is not needed anymore and all databases getting events from this dispatcher can be moved to the old one.

## Memory Limit

There is a _memory limit_ to control event queue memory consumption per MySQL Client. If a database is not able to handle events fast enough, and the event queue is getting full, we have the following options:

1. The dispatcher is blocked until the slowest database frees up space for new events. All other databases are waiting for the slowest one. (Preferred)
2. The dispatcher is _never_ blocked, but suspends incremental sync for the slow database and continues dispatching events to remained databases.

## Performance

A lot of CPU can be saved by not processing every event in every database. The binlog contains events for all databases, it is wasteful to distribute row events to a database that it will not process it, especially if there are a lot of databases. This requires some sort of per-database binlog filtering and buffering.

Currently all events are sent to all MaterializedMySQL databases but parsing the event which consumes CPU is up to the database.

# Detailed Design

1. If a client (e.g. database) wants to read a stream of the events from MySQL binlog, it creates a connection to remote binlog by host/user/password and _executed GTID set_ params.
2. If another client wants to read the events from the binlog but for different _executed GTID set_, it is **not** possible to reuse existing connection to MySQL, then need to create another connection to the same remote binlog. (_This is how it is implemented today_).
3. When these 2 connections get the same binlog positions, they read the same events. It is logical to drop duplicate connection and move all its users out. And now one connection dispatches binlog events to several clients. Obviously only connections to the same binlog should be merged.

## Classes

1. One connection can send (or dispatch) events to several clients and might be called `BinlogEventsDispatcher`.
2. Several dispatchers grouped by _user:password@host:port_ in `BinlogClient`. Since they point to the same binlog.
3. The clients should communicate only with public API from `BinlogClient`. The result of using `BinlogClient` is an object that implements `IBinlog` to read events from. This implementation of `IBinlog` must be compatible with old implementation `MySQLFlavor` -> when replacing old implementation by new one, the behavior must not be changed.

## SQL

```sql
-- create MaterializedMySQL databases that read the events from the binlog client
CREATE DATABASE db1_client1 ENGINE = MaterializedMySQL('host:port', 'db', 'user', 'password') SETTINGS use_binlog_client=1, max_bytes_in_binlog_queue=1024;
CREATE DATABASE db2_client1 ENGINE = MaterializedMySQL('host:port', 'db', 'user', 'password') SETTINGS use_binlog_client=1;
CREATE DATABASE db3_client1 ENGINE = MaterializedMySQL('host:port', 'db2', 'user', 'password') SETTINGS use_binlog_client=1;
CREATE DATABASE db4_client2 ENGINE = MaterializedMySQL('host2:port', 'db', 'user', 'password') SETTINGS use_binlog_client=1;
CREATE DATABASE db5_client3 ENGINE = MaterializedMySQL('host:port', 'db', 'user1', 'password') SETTINGS use_binlog_client=1;
CREATE DATABASE db6_old ENGINE = MaterializedMySQL('host:port', 'db', 'user1', 'password') SETTINGS use_binlog_client=0;
```

Databases `db1_client1`, `db2_client1` and `db3_client1` share one instance of `BinlogClient` since they have the same params. `BinlogClient` will create 3 connections to MySQL server thus 3 instances of `BinlogEventsDispatcher`, but if these connections would have the same binlog position, they should be merged to one connection. Means all clients will be moved to one dispatcher and others will be closed. Databases `db4_client2` and `db5_client3` would use 2 different independent `BinlogClient` instances. Database `db6_old` will use old implementation. NOTE: By default `use_binlog_client` is disabled. Setting `max_bytes_in_binlog_queue` defines the max allowed bytes in the binlog queue. By default, it is `1073741824` bytes. If number of bytes exceeds this limit, the dispatching will be stopped until the space will be freed for new events.

## Binlog Table Structure

To see the status of the all `BinlogClient` instances there is `system.mysql_binlogs` system table. It shows the list of all created and _alive_ `IBinlog` instances with information about its `BinlogEventsDispatcher` and `BinlogClient`.

Example:

```
SELECT * FROM system.mysql_binlogs FORMAT Vertical
Row 1:
──────
binlog_client_name:                        root@127.0.0.1:3306
name:                                      test_Clickhouse1
mysql_binlog_name:                         binlog.001154
mysql_binlog_pos:                          7142294
mysql_binlog_timestamp:                    1660082447
mysql_binlog_executed_gtid_set:            a9d88f83-c14e-11ec-bb36-244bfedf7766:1-30523304
dispatcher_name:                           Applier
dispatcher_mysql_binlog_name:              binlog.001154
dispatcher_mysql_binlog_pos:               7142294
dispatcher_mysql_binlog_timestamp:         1660082447
dispatcher_mysql_binlog_executed_gtid_set: a9d88f83-c14e-11ec-bb36-244bfedf7766:1-30523304
size:                                      0
bytes:                                     0
max_bytes:                                 0
```

### Tests

Unit tests:

```
$ ./unit_tests_dbms --gtest_filter=MySQLBinlog.*
```

Integration tests:

```
$ pytest -s -vv test_materialized_mysql_database/test.py::test_binlog_client
```

Dumps events from the file

```
$ ./utils/check-mysql-binlog/check-mysql-binlog --binlog binlog.001392
```

Dumps events from the server

```
$ ./utils/check-mysql-binlog/check-mysql-binlog  --host 127.0.0.1 --port 3306 --user root --password pass --gtid a9d88f83-c14e-11ec-bb36-244bfedf7766:1-30462856
```
