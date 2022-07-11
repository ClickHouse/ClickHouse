---
title: 我能把 ClickHouse 当做Key-value 键值存储来使用吗？
toc_hidden: true
sidebar_position: 101
---
# 我能把 ClickHouse 当做Key-value 键值存储来使用吗？ {#can-i-use-clickhouse-as-a-key-value-storage}.

简短的回答是 **不能** 。关键值的工作量是在列表中的最高位置时，**不能**{.text-danger}使用ClickHouse的情况。它是一个[OLAP](../../faq/general/olap.md)系统，毕竟有很多优秀的键值存储系统在那里。

然而，可能在某些情况下，使用ClickHouse进行类似键值的查询仍然是有意义的。通常，是一些低预算的产品，主要的工作负载是分析性的，很适合ClickHouse，但也有一些次要的过程需要一个键值模式，请求吞吐量不是很高，没有严格的延迟要求。如果你有无限的预算，你会为这样的次要工作负载安装一个次要的键值数据库，但实际上，多维护一个存储系统（监控、备份等）会有额外的成本，这可能是值得避免的。

如果你决定违背建议，对ClickHouse运行一些类似键值的查询，这里有一些提示。

- ClickHouse中点查询昂贵的关键原因是其稀疏的主索引[MergeTree表引擎家族]（../../engines/table-engines/mergetree-family/mergetree.md）。这个索引不能指向每一行具体的数据，相反，它指向每N行，系统必须从邻近的N行扫描到所需的行，沿途读取过多的数据。在一个键值场景中，通过`index_granularity`的设置来减少N的值可能是有用的。
- ClickHouse将每一列保存在一组单独的文件中，所以要组装一个完整的行，它需要通过这些文件中的每一个。它们的数量随着列数的增加而线性增加，所以在键值场景中，可能值得避免使用许多列，并将所有的有效数据放在一个单一的`String`列中，并以某种序列化格式（如JSON、Protobuf或任何有效的格式）进行编码。
- 还有一种方法，使用[Join](../../engines/table-engines/special/join.md)表引擎代替正常的`MergeTree`表和[joinGet](../../sql-reference/functions/other-functions.md#joinget) 函数来检索数据。它可以提供更好的查询性能，但可能有一些可用性和可靠性问题。下面是一个[使用实例](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00800_versatile_storage_join.sql#L49-L51)。
