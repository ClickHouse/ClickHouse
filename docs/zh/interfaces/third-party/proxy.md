# 来自第三方开发人员的代理服务器 {#lai-zi-di-san-fang-kai-fa-ren-yuan-de-dai-li-fu-wu-qi}

[chproxy](https://github.com/Vertamedia/chproxy) 是ClickHouse数据库的http代理和负载均衡器。

特征

*每用户路由和响应缓存。
*灵活的限制。
\*自动SSL证书续订。

在Go中实现。

## KittenHouse {#kittenhouse}

[KittenHouse](https://github.com/VKCOM/kittenhouse) 设计为ClickHouse和应用程序服务器之间的本地代理，以防在应用程序端缓冲INSERT数据是不可能或不方便的。

特征：

*内存和磁盘数据缓冲。
*每表路由。
\*负载平衡和健康检查。

在Go中实现。

## ﾂ环板-ｮﾂ嘉ｯﾂ偲 {#clickhouse-bulk}

[ﾂ环板-ｮﾂ嘉ｯﾂ偲](https://github.com/nikepan/clickhouse-bulk) 是一个简单的ClickHouse插入收集器。

特征：

*分组请求并按阈值或间隔发送。
*多个远程服务器。
\*基本身份验证。

在Go中实现。

[来源文章](https://clickhouse.tech/docs/zh/interfaces/third-party/proxy/) <!--hide-->
