---
toc_priority: 5
toc_title: MongoDB
---

# MongoDB {#mongodb}

MongoDB 引擎是只读表引擎，允许从远程 MongoDB 集合中读取数据(`SELECT`查询)。引擎只支持非嵌套的数据类型。不支持 `INSERT` 查询。

## 创建一张表 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password);
```

**引擎参数**

-   `host:port` — MongoDB 服务器地址.

-   `database` — 数据库名称.

-   `collection` —  集合名称.

-   `user` — MongoDB 用户.

-   `password` — 用户密码.

## 用法示例 {#usage-example}

ClickHouse 中的表，从 MongoDB 集合中读取数据:

``` text
CREATE TABLE mongo_table
(
    key UInt64,
    data String
) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'testuser', 'clickhouse');
```

查询:

``` sql
SELECT COUNT() FROM mongo_table;
```

``` text
┌─count()─┐
│       4 │
└─────────┘
```

[原始文章](https://clickhouse.com/docs/en/engines/table-engines/integrations/mongodb/) <!--hide-->
