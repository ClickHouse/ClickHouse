# remote, remoteSecure {#remote-remotesecure}

允许您访问远程服务器，而无需创建 `Distributed` 表。`remoteSecure` - 与 `remote` 相同，但是会使用加密链接。

这两个函数都可以在 `SELECT` 和 `INSERT` 查询中使用。

语法:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password'], sharding_key])
remote('addresses_expr', db.table[, 'user'[, 'password'], sharding_key])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password'], sharding_key])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password'], sharding_key])
```

**参数**

- `addresses_expr` – 代表远程服务器地址的一个表达式。可以只是单个服务器地址。 服务器地址可以是 `host:port` 或 `host`。

    `host` 可以指定为服务器名称，或是IPV4或IPV6地址。IPv6地址在方括号中指定。

    `port` 是远程服务器上的TCP端口。 如果省略端口，则 `remote` 使用服务器配置文件中的 [tcp_port](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) （默认情况为，9000），`remoteSecure` 使用 [tcp_port_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) （默认情况为，9440）。

    IPv6地址需要指定端口。

    类型: [String](../../sql-reference/data-types/string.md)。

    - `db` — 数据库名。类型: [String](../../sql-reference/data-types/string.md)。
    - `table` — 表名。类型: [String](../../sql-reference/data-types/string.md)。
    - `user` — 用户名。如果未指定用户，则使用 `default` 。类型: [String](../../sql-reference/data-types/string.md)。
    - `password` — 用户密码。如果未指定密码，则使用空密码。类型: [String](../../sql-reference/data-types/string.md)。
    - `sharding_key` — 分片键以支持在节点之间分布数据。 例如: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`。 类型: [UInt32](../../sql-reference/data-types/int-uint.md)。

    **返回值**

    来自远程服务器的数据集。

    **用法**

    使用 `remote` 表函数没有创建一个 `Distributed` 表更优，因为在这种情况下，将为每个请求重新建立服务器连接。此外，如果设置了主机名，则会解析这些名称，并且在使用各种副本时不会计入错误。 在处理大量查询时，始终优先创建 `Distributed` 表，不要使用 `remote` 表函数。

    该 `remote` 表函数可以在以下情况下是有用的:

    -   访问特定服务器进行数据比较、调试和测试。
    -   在多个ClickHouse集群之间的用户研究目的的查询。
    -   手动发出的不频繁分布式请求。
    -   每次重新定义服务器集的分布式请求。

    **地址**

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

多个地址可以用逗号分隔。在这种情况下，ClickHouse将使用分布式处理，因此它将将查询发送到所有指定的地址（如具有不同数据的分片）。

``` text
example01-01-1,example01-02-1
```

表达式的一部分可以用大括号指定。 前面的示例可以写成如下:

``` text
example01-0{1,2}-1
```

大括号可以包含由两个点（非负整数）分隔的数字范围。 在这种情况下，范围将扩展为生成分片地址的一组值。 如果第一个数字以零开头，则使用相同的零对齐形成值。 前面的示例可以写成如下:

``` text
example01-{01..02}-1
```

如果您有多对大括号，它会生成相应集合的直接乘积。

大括号中的地址和部分地址可以用管道符号(\|)分隔。 在这种情况下，相应的地址集被解释为副本，并且查询将被发送到第一个正常副本。 但是，副本将按照当前[load_balancing](../../operations/settings/settings.md)设置的顺序进行迭代。此示例指定两个分片，每个分片都有两个副本:

``` text
example01-{01..02}-{1|2}
```

生成的地址数由常量限制。目前这是1000个地址。

**示例**

从远程服务器选择数据:

``` sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

将远程服务器中的数据插入表中:

``` sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

[原始文章](https://clickhouse.com/docs/en/query_language/table_functions/remote/) <!--hide-->
