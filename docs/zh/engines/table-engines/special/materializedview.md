---
slug: /zh/engines/table-engines/special/materializedview
---
# MaterializedView {#materializedview}

物化视图的使用（更多信息请参阅 [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) ）。它需要使用一个不同的引擎来存储数据，这个引擎要在创建物化视图时指定。当从表中读取时，它就会使用该引擎。
