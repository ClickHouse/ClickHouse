---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 47
toc_title: "\u70B9\u51FB\u66F4\u65B0"
---

# 点击更新 {#clickhouse-update}

如果从deb包安装ClickHouse，请在服务器上执行以下命令:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

如果您使用除推荐的deb包之外的其他内容安装ClickHouse，请使用适当的更新方法。

ClickHouse不支持分布式更新。 该操作应在每个单独的服务器上连续执行。 不要同时更新群集上的所有服务器，否则群集将在一段时间内不可用。
