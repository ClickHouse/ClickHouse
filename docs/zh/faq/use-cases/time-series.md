---
title: 我能把 ClickHouse 当做时序数据库来使用吗?
toc_hidden: true
sidebar_position: 101
---

# 我能把 ClickHouse 当做时序数据库来使用吗? {#can-i-use-clickhouse-as-a-time-series-database}

ClickHouse是一个通用的数据存储解决方案[OLAP](../../faq/general/olap.md)的工作负载，而有许多专门的时间序列数据库管理系统。然而，ClickHouse的[专注于查询执行速度](../../faq/general/why-clickhouse-is-so-fast.md)使得它在许多情况下的性能优于专门的系统。关于这个话题有很多独立的基准，所以我们不打算在这里进行论述。相反，让我们将重点放在ClickHouse的重要功能(如果这是你的用例)上。



首先，有 **[specialized codecs](../../sql-reference/statements/create/table.md#create-query-specialized-codecs)**，这是典型的时间序列。无论是常见的算法，如“DoubleDelta”和“Gorilla”，或特定的ClickHouse 数据类型如“T64”。



其次，时间序列查询通常只访问最近的数据，比如一天或一周以前的数据。使用具有快速nVME/SSD驱动器和高容量HDD驱动器的服务器是有意义的。ClickHouse [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)特性允许配置在快速硬盘上保持新鲜的热数据，并随着数据的老化逐渐移动到较慢的硬盘上。如果您的需求需要，也可以汇总或删除更旧的数据。



尽管这与ClickHouse存储和处理原始数据的理念相违背，但你可以使用[materialized views](../../sql-reference/statements/create/view.md)来适应更紧迫的延迟或成本需求。