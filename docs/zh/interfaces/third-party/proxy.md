---
toc_priority: 29
toc_title: 第三方代理
---

# 第三方代理 {#proxy-servers-from-third-party-developers}

## chproxy {#chproxy}

[chproxy](https://github.com/Vertamedia/chproxy), 是一个用于ClickHouse数据库的HTTP代理和负载均衡器。

特性:

-   用户路由和响应缓存。
-   灵活的限制。
-   自动SSL证书续订。

使用go语言实现。

## KittenHouse {#kittenhouse}

[KittenHouse](https://github.com/VKCOM/kittenhouse)被设计为ClickHouse和应用服务器之间的本地代理，以防不可能或不方便在应用程序端缓冲插入数据。

特性:

-   内存和磁盘上的数据缓冲。
-   表路由。
-   负载平衡和运行状况检查。

使用go语言实现。

## ClickHouse-Bulk {#clickhouse-bulk}

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk)是一个简单的ClickHouse收集器。

特性:

-   按阈值或间隔对请求进行分组并发送。
-   多个远程服务器。
-   基本身份验证。

使用go语言实现。

[Original article](https://clickhouse.com/docs/en/interfaces/third-party/proxy/) <!--hide-->
