---
slug: /zh/sql-reference/statements/use
sidebar_position: 53
sidebar_label: USE
---

# USE 语句 {#use}

``` sql
USE db
```

用于设置会话的当前数据库。

如果查询语句中没有在表名前面以加点的方式指明数据库名， 则用当前数据库进行搜索。

使用 HTTP 协议时无法进行此查询，因为没有会话的概念。
