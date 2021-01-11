---
toc_priority: 18
toc_title: 原生接口(TCP)
---

# 原生接口（TCP）{#native-interface-tcp}

原生接口用于[命令行客户端](cli.md)，用于分布式查询处理期间的服务器间通信，以及其他C++程序。可惜的是，原生的ClickHouse协议还没有正式的规范，但它可以从ClickHouse[源代码](https://github.com/ClickHouse/ClickHouse/tree/master/src/Client)通过拦截和分析TCP流量进行反向工程。

[来源文章](https://clickhouse.tech/docs/zh/interfaces/tcp/) <!--hide-->
