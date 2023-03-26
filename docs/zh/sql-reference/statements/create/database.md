---
toc_priority: 35
toc_title: DATABASE
---

# CREATE DATABASE {#query-language-create-database}

创建数据库.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

## 条件 {#clauses}

### IF NOT EXISTS {#if-not-exists}

如果`db_name`数据库已经存在，则ClickHouse不会创建新数据库并且：

-   如果指定了子句，则不会引发异常。
-   如果未指定子句，则抛出异常。

### ON CLUSTER {#on-cluster}

ClickHouse在指定集群的所有服务器上创建`db_name`数据库。 更多细节在 [Distributed DDL](../../../sql-reference/distributed-ddl.md) article.

### ENGINE {#engine}

[MySQL](../../../engines/database-engines/mysql.md) 允许您从远程MySQL服务器检索数据. 默认情况下，ClickHouse使用自己的[database engine](../../../engines/database-engines/index.md). 还有一个[lazy](../../../engines/database-engines/lazy.md)引擎.
