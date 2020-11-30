---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u8FDC\u7A0B"
---

# 远程，远程安全 {#remote-remotesecure}

允许您访问远程服务器，而无需创建 `Distributed` 桌子

签名:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password']])
remote('addresses_expr', db.table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password']])
```

`addresses_expr` – An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port`，或者只是 `host`. 主机可以指定为服务器名称，也可以指定为IPv4或IPv6地址。 IPv6地址在方括号中指定。 端口是远程服务器上的TCP端口。 如果省略端口，它使用 `tcp_port` 从服务器的配置文件（默认情况下，9000）。

!!! important "重要事项"
    IPv6地址需要该端口。

例:

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

多个地址可以用逗号分隔。 在这种情况下，ClickHouse将使用分布式处理，因此它将将查询发送到所有指定的地址（如具有不同数据的分片）。

示例:

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

大括号中的地址和部分地址可以用管道符号(\|)分隔。 在这种情况下，相应的地址集被解释为副本，并且查询将被发送到第一个正常副本。 但是，副本将按照当前设置的顺序进行迭代 [load\_balancing](../../operations/settings/settings.md) 设置。

示例:

``` text
example01-{01..02}-{1|2}
```

此示例指定两个分片，每个分片都有两个副本。

生成的地址数由常量限制。 现在这是1000个地址。

使用 `remote` 表函数比创建一个不太优化 `Distributed` 表，因为在这种情况下，服务器连接被重新建立为每个请求。 此外，如果设置了主机名，则会解析这些名称，并且在使用各种副本时不会计算错误。 在处理大量查询时，始终创建 `Distributed` 表的时间提前，不要使用 `remote` 表功能。

该 `remote` 表函数可以在以下情况下是有用的:

-   访问特定服务器进行数据比较、调试和测试。
-   查询之间的各种ClickHouse群集用于研究目的。
-   手动发出的罕见分布式请求。
-   每次重新定义服务器集的分布式请求。

如果未指定用户, `default` 被使用。
如果未指定密码，则使用空密码。

`remoteSecure` -相同 `remote` but with secured connection. Default port — [tcp\_port\_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) 从配置或9440.

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/remote/) <!--hide-->
