---
toc_priority: 40
toc_title: remote
---

# remote, remoteSecure {#remote-remotesecure}

Allows to access remote servers without creating a [Distributed](../../engines/table-engines/special/distributed.md) table. `remoteSecure` - same as `remote` but with a secured connection.

Both functions can be used in `SELECT` and `INSERT` queries.

**Syntax**

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password'], sharding_key])
remote('addresses_expr', db.table[, 'user'[, 'password'], sharding_key])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password'], sharding_key])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password'], sharding_key])
```

**Parameters**

- `addresses_expr` — An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port`, or just `host`. 
   
    The host can be specified as the server name, or as the IPv4 or IPv6 address. An IPv6 address is specified in square brackets. 
   
    The port is the TCP port on the remote server. If the port is omitted, it uses [tcp_port](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) from the server’s config file in `remote` (by default, 9000) and [tcp_port_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) in `remoteSecure` (by default, 9440).

    The port is required for an IPv6 address.

    Type: [String](../../sql-reference/data-types/string.md).

- `db` — Database name. Type: [String](../../sql-reference/data-types/string.md).
- `table` — Table name. Type: [String](../../sql-reference/data-types/string.md).
- `user` — User name. If the user is not specified, `default` is used. Type: [String](../../sql-reference/data-types/string.md).
- `password` — User password. If the password is not specified, an empty password is used. Type: [String](../../sql-reference/data-types/string.md).
- `sharding_key` — Sharding key to support distributing data across nodes. For example: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Returned value**

The dataset from remote servers.

**Usage**

Using the `remote` table function is less optimal than creating a `Distributed` table because in this case the server connection is re-established for every request. Also, if hostnames are set, the names are resolved, and errors are not counted when working with various replicas. When processing a large number of queries, always create the `Distributed` table ahead of time, and do not use the `remote` table function.

The `remote` table function can be useful in the following cases:

-   Accessing a specific server for data comparison, debugging, and testing.
-   Queries between various ClickHouse clusters for research purposes.
-   Infrequent distributed requests that are made manually.
-   Distributed requests where the set of servers is re-defined each time.

**Adresses**

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Multiple addresses can be comma-separated. In this case, ClickHouse will use distributed processing, so it will send the query to all specified addresses (like shards with different data). Example:

``` text
example01-01-1,example01-02-1
```

Part of the expression can be specified in curly brackets. The previous example can be written as follows:

``` text
example01-0{1,2}-1
```

Curly brackets can contain a range of numbers separated by two dots (non-negative integers). In this case, the range is expanded to a set of values that generate shard addresses. If the first number starts with zero, the values are formed with the same zero alignment. The previous example can be written as follows:

``` text
example01-{01..02}-1
```

If you have multiple pairs of curly brackets, it generates the direct product of the corresponding sets.

Addresses and parts of addresses in curly brackets can be separated by the pipe symbol (\|). In this case, the corresponding sets of addresses are interpreted as replicas, and the query will be sent to the first healthy replica. However, the replicas are iterated in the order currently set in the [load_balancing](../../operations/settings/settings.md#settings-load_balancing) setting. This example specifies two shards that each have two replicas:

``` text
example01-{01..02}-{1|2}
```

The number of addresses generated is limited by a constant. Right now this is 1000 addresses.

**Examples**

Selecting data from a remote server:

``` sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

Inserting data from a remote server into a table:

``` sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/remote/) <!--hide-->
