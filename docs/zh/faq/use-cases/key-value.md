---
title: 我能把 ClickHouse 当做Key-value 键值存储来使用吗？
toc_hidden: true
sidebar_position: 101
---
# 我能把 ClickHouse 当做Key-value 键值存储来使用吗？ {#can-i-use-clickhouse-as-a-key-value-storage}.

简短的回答是 **不能** 。键值类型负载是ClickHouse最<span class="text-danger">**不适合**</span>的多种场景之一。ClickHouse 毕竟只是一个[OLAP](../../faq/general/olap.md)系统，对于这类负载来说，目前还是有很多优秀的键值存储系统可供选择。

然而，在某些情况下，使用ClickHouse进行类似键值的查询仍然是有意义的。通常，一些主要的工作负载是分析性的比较适合使用Clickhouse低预算的产品中，也有一些次要的操作是需要使用键值模式的，同时这些操作的请求吞吐量不会很高，也没有严格的延迟要求。如果你有无限的预算，你会为这样的次要工作负载安装一个次要的键值数据库，但实际上，多维护一个存储系统（监控、备份等）会有额外的成本，这是可以考虑避免的。

如果你决定不遵从这些建议，想要使用ClickHouse运行一些类似键值的查询，那么这里有一些提示。

- ClickHouse中点查询开销大的关键原因是MergeTree表引擎家族[MergeTree表引擎家族]（../../engines/table-engines/mergetree-family/mergetree.md）采用的稀疏主索引。这个索引不能指向每一行具体的数据，相反，它指向每N行，系统必须从邻近的N行扫描到所需的行，沿途读取过多的数据。在一个键值场景中，通过`index_granularity`的设置来减少N的值可能是有用的。
- ClickHouse将每一列保存在一组单独的文件中，所以要组装一个完整的行，它需要访问文件组中的每一个文件。访问数据数量会随着列数的增加而线性增加，所以在键值场景中，需要避免使用许多列，并将所有的有效数据放在一个单一的`String`列中，并以某种序列化格式（如JSON、Protobuf或任何有效的格式）进行编码。
- 还有一种方法，使用[Join](../../engines/table-engines/special/join.md)表引擎代替正常的`MergeTree`表和[joinGet](../../sql-reference/functions/other-functions.md#joinget) 函数来检索数据。它可以提供更好的查询性能，但可能有一些可用性和可靠性问题。下面是一个[使用实例](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00800_versatile_storage_join.sql#L49-L51)。
