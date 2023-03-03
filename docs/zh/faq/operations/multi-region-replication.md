---
title: ClickHouse支持多区域复制吗?
toc_hidden: true
toc_priority: 30
---

# ClickHouse支持多区域复制吗? {#does-clickhouse-support-multi-region-replication}

简短的回答是“是的”。然而，我们建议将所有区域/数据中心之间的延迟保持在两位数字范围内，否则，在通过分布式共识协议时，写性能将受到影响。例如，美国海岸之间的复制可能会很好，但美国和欧洲之间就不行。

在配置方面，这与单区域复制没有区别，只是使用位于不同位置的主机作为副本。

更多信息，请参见[关于数据复制的完整文章](../../engines/table-engines/mergetree-family/replication.md)。

