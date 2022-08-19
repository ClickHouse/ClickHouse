---
sidebar_position: 40
sidebar_label: remote
---

# remote, remoteSecure

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

## Globs in Addresses {globs-in-addresses}

Patterns in curly brackets `{ }` are used to generate a set of shards and to specify replicas. If there are multiple pairs of curly brackets, then the direct product of the corresponding sets is generated.
The following pattern types are supported.

- {*a*,*b*} - Any number of variants separated by a comma. The pattern is replaced with *a* in the first shard address and it is replaced with *b* in the second shard address and so on. For instance, `example0{1,2}-1` generates addresses `example01-1` and `example02-1`.
- {*n*..*m*} - A range of numbers. This pattern generates shard addresses with incrementing indices from *n* to *m*. `example0{1..2}-1` generates `example01-1` and `example02-1`.
- {*0n*..*0m*} - A range of numbers with leading zeroes. This modification preserves leading zeroes in indices. The pattern `example{01..03}-1` generates `example01-1`, `example02-1` and `example03-1`.
- {*a*|*b*} - Any number of variants separated by a `|`. The pattern specifies replicas. For instance, `example01-{1|2}` generates replicas `example01-1` and `example01-2`.

The query will be sent to the first healthy replica. However, for `remote` the replicas are iterated in the order currently set in the [load_balancing](../../operations/settings/settings.md#settings-load_balancing) setting.
The number of generated addresses is limited by [table_function_remote_max_addresses](../../operations/settings/settings.md#table_function_remote_max_addresses) setting.
