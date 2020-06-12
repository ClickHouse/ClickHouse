# 原生客户端接口（TCP） {#yuan-sheng-ke-hu-duan-jie-kou-tcp}

本机协议用于 [命令行客户端](cli.md)，用于分布式查询处理期间的服务器间通信，以及其他C ++程序。 不幸的是，本机ClickHouse协议还没有正式的规范，但它可以从ClickHouse源代码进行逆向工程 [从这里开始](https://github.com/ClickHouse/ClickHouse/tree/master/src/Client)）和/或拦截和分析TCP流量。

[来源文章](https://clickhouse.tech/docs/zh/interfaces/tcp/) <!--hide-->
