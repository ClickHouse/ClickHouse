# ClickHouse历史 {#clickhouseli-shi}

ClickHouse最初是为 [YandexMetrica](https://metrica.yandex.com/) [世界第二大Web分析平台](http://w3techs.com/technologies/overview/traffic_analysis/all) 而开发的。多年来一直作为该系统的核心组件被该系统持续使用着。目前为止，该系统在ClickHouse中有超过13万亿条记录，并且每天超过200多亿个事件被处理。它允许直接从原始数据中动态查询并生成报告。本文简要介绍了ClickHouse在其早期发展阶段的目标。

Yandex.Metrica基于用户定义的字段，对实时访问、连接会话，生成实时的统计报表。这种需求往往需要复杂聚合方式，比如对访问用户进行去重。构建报表的数据，是实时接收存储的新数据。

截至2014年4月，Yandex.Metrica每天跟踪大约120亿个事件（用户的点击和浏览）。为了可以创建自定义的报表，我们必须存储全部这些事件。同时，这些查询可能需要在几百毫秒内扫描数百万行的数据，或在几秒内扫描数亿行的数据。

## Yandex.Metrica以及其他Yandex服务的使用案例 {#yandex-metricayi-ji-qi-ta-yandexfu-wu-de-shi-yong-an-li}

在Yandex.Metrica中，ClickHouse被用于多个场景中。
它的主要任务是使用原始数据在线的提供各种数据报告。它使用374台服务器的集群，存储了20.3万亿行的数据。在去除重复与副本数据的情况下，压缩后的数据达到了2PB。未压缩前（TSV格式）它大概有17PB。

ClickHouse还被使用在：

-   存储来自Yandex.Metrica回话重放数据。
-   处理中间数据
-   与Analytics一起构建全球报表。
-   为调试Yandex.Metrica引擎运行查询
-   分析来自API和用户界面的日志数据

ClickHouse在其他Yandex服务中至少有12个安装：search verticals, Market, Direct, business analytics, mobile development, AdFox, personal services等。

## 聚合与非聚合数据 {#ju-he-yu-fei-ju-he-shu-ju}

有一种流行的观点认为，想要有效的计算统计数据，必须要聚合数据，因为聚合将降低数据量。

但是数据聚合是一个有诸多限制的解决方案，例如：

-   你必须提前知道用户定义的报表的字段列表
-   用户无法自定义报表
-   当聚合条件过多时，可能不会减少数据，聚合是无用的。
-   存在大量报表时，有太多的聚合变化（组合爆炸）
-   当聚合条件有非常大的基数时（如：url），数据量没有太大减少（少于两倍）
-   聚合的数据量可能会增长而不是收缩
-   用户不会查看我们为他生成的所有报告，大部分计算将是无用的
-   各种聚合可能违背了数据的逻辑完整性

如果我们直接使用非聚合数据而不进行任何聚合时，我们的计算量可能是减少的。

然而，相对于聚合中很大一部分工作被离线完成，在线计算需要尽快的完成计算，因为用户在等待结果。

Yandex.Metrica 有一个专门用于聚合数据的系统，称为Metrage，它可以用作大部分报表。
从2009年开始，Yandex.Metrica还为非聚合数据使用专门的OLAP数据库，称为OLAPServer，它以前用于报表构建系统。
OLAPServer可以很好的工作在非聚合数据上，但是它有诸多限制，导致无法根据需要将其用于所有报表中。如，缺少对数据类型的支持（只支持数据），无法实时增量的更新数据（只能通过每天重写数据完成）。OLAPServer不是一个数据库管理系统，它只是一个数据库。

为了消除OLAPServer的这些局限性，解决所有报表使用非聚合数据的问题，我们开发了ClickHouse数据库管理系统。

[来源文章](https://clickhouse.tech/docs/en/introduction/ya_metrika_task/) <!--hide-->
