---
slug: /zh/faq/operations/delete-old-data
title: 是否可以从ClickHouse表中删除旧记录？
toc_hidden: true
sidebar_position: 20
---

# 是否可以从ClickHouse表中删除旧记录？ {#is-it-possible-to-delete-old-records-from-a-clickhouse-table}

简短的答案是“可以”。ClickHouse具有多种机制，允许通过删除旧数据来释放磁盘空间。每种机制都针对不同的场景。

## TTL {#ttl}

ClickHouse 允许在某些条件发生时自动删除值。这个条件被配置为基于任何列的表达式，通常只是针对任何时间戳列的静态偏移量。

这种方法的主要优势是它不需要任何外部系统来触发，一旦配置了 TTL，数据删除就会自动在后台发生。

:::note
TTL 也可以用来将数据移动到非 [/dev/null](https://en.wikipedia.org/wiki/Null_device) 的不同存储系统，例如从 SSD 到 HDD。
:::

有关 [配置 TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的更多详细信息。

## ALTER DELETE {#alter-delete}

ClickHouse没有像[OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing)数据库那样的实时点删除。最接近的东西是 `Mutation`，执行 `ALTER ... DELETE` 或 `ALTER ... UPDATE` 查询，以区别于普通的`DELETE`或`UPDATE`。因为它们是异步批处理操作，而不是立即修改。`ALTER TABLE`前缀后的其余语法相似。

`ALTER DELETE`可以灵活地用来删除旧数据。如果你需要定期这样做，主要缺点将是需要有一个外部系统来提交查询。还有一些性能方面的考虑，因为即使只有一行要被删除，突变也会重写完整部分。

这是使基于ClickHouse的系统符合[GDPR](https://gdpr-info.eu)的最常见方法。

有关[mutations](../../sql-reference/statements/alter.md/#alter-mutations)的更多详细信息。

## DROP PARTITION {#drop-partition}

`ALTER TABLE ... DROP PARTITION`提供了一种成本效率高的方式来删除整个分区。它不是那么灵活，需要在创建表时配置适当的分区方案，但仍然涵盖了大多数常见情况。像 mutations 一样，需要从外部系统执行以进行常规使用。

有关[操作分区](../../sql-reference/statements/alter/partition.mdx/#alter_drop-partition)的更多详细信息。

## TRUNCATE {#truncate}

从表中删除所有数据是相当激进的，但在某些情况下可能正是您所需要的。

有关[truncate](../../sql-reference/statements/truncate.md)的更多详细信息。
