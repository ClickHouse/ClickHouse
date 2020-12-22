---
toc_priority: 40
toc_title: remote
---

# remote, remoteSecure {#remote-remotesecure}

Allows you to access remote servers without creating a `Distributed` table.

Signatures:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password']])
remote('addresses_expr', db.table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password']])
```

`addresses_expr` – An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port`, or just `host`. The host can be specified as the server name, or as the IPv4 or IPv6 address. An IPv6 address is specified in square brackets. The port is the TCP port on the remote server. If the port is omitted, it uses `tcp_port` from the server’s config file (by default, 9000).

!!! important "Important"
    The port is required for an IPv6 address.

Examples:

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Multiple addresses can be comma-separated. In this case, ClickHouse will use distributed processing, so it will send the query to all specified addresses (like to shards with different data).

Example:

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

Addresses and parts of addresses in curly brackets can be separated by the pipe symbol (\|). In this case, the corresponding sets of addresses are interpreted as replicas, and the query will be sent to the first healthy replica. However, the replicas are iterated in the order currently set in the [load_balancing](../../operations/settings/settings.md) setting.

Example:

``` text
example01-{01..02}-{1|2}
```

This example specifies two shards that each have two replicas.

The number of addresses generated is limited by a constant. Right now this is 1000 addresses.

Using the `remote` table function is less optimal than creating a `Distributed` table, because in this case, the server connection is re-established for every request. In addition, if host names are set, the names are resolved, and errors are not counted when working with various replicas. When processing a large number of queries, always create the `Distributed` table ahead of time, and don’t use the `remote` table function.

The `remote` table function can be useful in the following cases:

-   Accessing a specific server for data comparison, debugging, and testing.
-   Queries between various ClickHouse clusters for research purposes.
-   Infrequent distributed requests that are made manually.
-   Distributed requests where the set of servers is re-defined each time.

If the user is not specified, `default` is used.
If the password is not specified, an empty password is used.

`remoteSecure` - same as `remote` but with secured connection. Default port — [tcp_port_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) from config or 9440.

[Original article](https://clickhouse.tech/docs/en/query_language/table_functions/remote/) <!--hide-->
