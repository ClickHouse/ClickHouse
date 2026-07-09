---
description: '可对存储在远程 MongoDB 服务器上的数据执行 `SELECT` 查询。'
sidebar_label: 'mongodb'
sidebar_position: 135
slug: /sql-reference/table-functions/mongodb
title: 'mongodb'
doc_type: 'reference'
---

可对存储在远程 MongoDB 服务器上的数据执行 `SELECT` 查询。

<div id="syntax">
  ## 语法
</div>

```sql
mongodb(host:port, database, collection, user, password, structure[, options[, oid_columns]]);
mongodb(uri, collection, structure[, oid_columns]);
mongodb(named_collection_name[, <arg>=<value>...]);
```

<div id="arguments">
  ## 参数
</div>

| 参数            | 说明                                         |
| ------------- | ------------------------------------------ |
| `host:port`   | MongoDB 服务器地址。                             |
| `database`    | 远程 数据库 名称。                            |
| `collection`  | 远程 集合 名称。                          |
| `user`        | MongoDB 用户。                                |
| `password`    | 用户密码。                                      |
| `structure`   | 此函数返回的 ClickHouse 表的 schema。               |
| `options`     | MongoDB 连接字符串 选项 (可选参数) 。      |
| `oid_columns` | 在 WHERE 子句中应视为 `oid` 的列列表，以逗号分隔。默认为 `_id`。 |

:::tip
如果您使用 MongoDB Atlas Cloud 服务，请添加以下选项：

```ini
'connectTimeoutMS=10000&ssl=true&authSource=admin'
```

:::

你也可以通过 URI 进行连接：

```sql
mongodb(uri, collection, structure[, oid_columns])
```

| 参数            | 描述                                        |
| ------------- | ----------------------------------------- |
| `uri`         | 连接字符串。                                    |
| `collection`  | 远程集合名称。                                   |
| `structure`   | 此函数返回的 ClickHouse 表的 schema。              |
| `oid_columns` | 在 WHERE 子句中应视为 `oid` 的列的逗号分隔列表。默认为 `_id`。 |
| :::           |                                           |

你可以使用命名集合来传递这些参数：

```sql
mongodb(_named_collection_[, host][, port][, database][, collection][, user][, password][, structure][, options][, oid_columns])
-- or
mongodb(_named_collection_[, uri][, structure][, oid_columns])
```

<div id="returned_value">
  ## 返回值
</div>

一个表对象，其列与原始 MongoDB 表的列相同。

<div id="examples">
  ## 示例
</div>

假设我们有一个名为 `my_collection` 的集合，它定义在名为 `test` 的 MongoDB 数据库中，并且我们插入了几个文档：

```sql
db.createUser({user:"test_user",pwd:"password",roles:[{role:"readWrite",db:"test"}]})

db.createCollection("my_collection")

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.9", command: "check-cpu-usage -w 75 -c 90" }
)

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.4", command: "system-check"}
)
```

使用 `mongodb` 表函数查询该集合：

```sql
SELECT * FROM mongodb(
    '127.0.0.1:27017',
    'test',
    'my_collection',
    'test_user',
    'password',
    'log_type String, host String, command String',
    'connectTimeoutMS=10000'
)
```

或者：

```sql
SELECT * FROM mongodb(
    'mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
    'my_collection',
    'log_type String, host String, command String'
)
```

或：

```sql
CREATE NAMED COLLECTION mongo_creds AS
       uri='mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
       collection='default_collection';

SELECT * FROM mongodb(
        mongo_creds,
        collection = 'my_collection',
        structure = 'log_type String, host String, command String'
)
```

<div id="related">
  ## 相关
</div>

* [`MongoDB` 表引擎](/zh/engines/table-engines/integrations/mongodb.md)
* [将 MongoDB 用作字典源](../statements/create/dictionary/sources/mongodb.md)