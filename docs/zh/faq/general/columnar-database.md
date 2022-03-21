---
title: 什么是列存储数据库?
toc_hidden: true
toc_priority: 101
---

# 什么是列存储数据库? {#what-is-a-columnar-database}

列存储数据库独立存储每个列的数据。这只允许从磁盘读取任何给定查询中使用的列的数据。其代价是，影响整行的操作会按比例变得更昂贵。列存储数据库的同义词是面向列的数据库管理系统。ClickHouse就是这样一个典型的例子。

列存储数据库的主要优点是:

- 查询只使用许多列其中的少数列。
— 聚合对大量数据的查询。
— 按列压缩。

下面是构建报表时传统的面向行系统和柱状数据库之间的区别:

**传统行存储**
!(传统行存储)(https://clickhouse.com/docs/en/images/row-oriented.gif)

**列存储**
!(列存储)(https://clickhouse.com/docs/en/images/column-oriented.gif)

列存储数据库是分析应用程序的首选，因为它允许在一个表中有许多列以防万一，但不会在读取查询执行时为未使用的列付出代价。面向列的数据库是为大数据处理而设计的，因为和数据仓库一样，它们通常使用分布式的低成本硬件集群来提高吞吐量。ClickHouse结合了[分布式](../../engines/table-engines/special/distributed.md)和[复制式](../../engines/table-engines/mergetree-family/replication.md)两类表。