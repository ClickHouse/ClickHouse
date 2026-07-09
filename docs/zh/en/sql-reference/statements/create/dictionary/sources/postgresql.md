---
slug: /sql-reference/statements/create/dictionary/sources/postgresql
title: 'PostgreSQL 字典源'
sidebar_position: 12
sidebar_label: 'PostgreSQL'
description: '在 ClickHouse 中将 PostgreSQL 配置为字典源。'
doc_type: '参考'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(POSTGRESQL(
        port 5432
        host 'postgresql-hostname'
        user 'postgres_user'
        password 'postgres_password'
        db 'db_name'
        table 'table_name'
        replica(host 'example01-1' port 5432 priority 1)
        replica(host 'example01-2' port 5432 priority 2)
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
      <postgresql>
          <host>postgresql-hostname</hoat>
          <port>5432</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </postgresql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

设置字段：

| Setting                | Description                                                           |
| ---------------------- | --------------------------------------------------------------------- |
| `host`                 | PostgreSQL 服务器的主机。你可以为所有副本统一指定，也可以为每个副本分别指定 (在 `<replica>` 内) 。       |
| `port`                 | PostgreSQL 服务器的端口。你可以为所有副本统一指定，也可以为每个副本分别指定 (在 `<replica>` 内) 。       |
| `user`                 | PostgreSQL 用户名。你可以为所有副本统一指定，也可以为每个副本分别指定 (在 `<replica>` 内) 。          |
| `password`             | PostgreSQL 用户的密码。你可以为所有副本统一指定，也可以为每个副本分别指定 (在 `<replica>` 内) 。        |
| `replica`              | 副本配置部分。可以有多个此类部分。                                                     |
| `replica/host`         | PostgreSQL 主机。                                                        |
| `replica/port`         | PostgreSQL 端口。                                                        |
| `replica/priority`     | 副本优先级。尝试建立连接时，ClickHouse 会按优先级顺序遍历各个副本。数字越小，优先级越高。                    |
| `db`                   | 数据库名称。                                                                |
| `table`                | 表名称。                                                                  |
| `where`                | 选择条件。条件的语法与 PostgreSQL 中 `WHERE` 子句的语法相同。例如，`id > 10 AND id < 20`。可选。 |
| `invalidate_query`     | 用于检查字典状态的查询。可选。更多信息请参见 [使用 LIFETIME 刷新字典数据](../lifetime.md) 一节。       |
| `background_reconnect` | 如果连接失败，则在后台重新连接到副本。可选。                                                |
| `query`                | 自定义查询。可选。                                                             |

:::note
`table` 或 `where` 字段不能与 `query` 字段同时使用。此外，`table` 和 `query` 字段中必须声明其中一个。
:::