---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'MySQL 字典源'
sidebar_position: 7
sidebar_label: 'MySQL'
description: '将 MySQL 配置为 ClickHouse 中的字典源。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        port 3306
        user 'clickhouse'
        password 'qwerty'
        replica(host 'example01-1' priority 1)
        replica(host 'example01-2' priority 1)
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
        enable_compression 1
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
      <mysql>
          <port>3306</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <replica>
              <host>example01-1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>example01-2</host>
              <priority>1</priority>
          </replica>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
          <enable_compression>1</enable_compression>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

设置字段：

| 设置                        | 说明                                                                                                                             |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `port`                    | MySQL 服务器的端口。可以为所有副本统一指定，也可以为每个副本分别指定 (在 `<replica>` 内) 。                                                                      |
| `user`                    | MySQL 用户名。可以为所有副本统一指定，也可以为每个副本分别指定 (在 `<replica>` 内) 。                                                                         |
| `password`                | MySQL 用户的密码。可以为所有副本统一指定，也可以为每个副本分别指定 (在 `<replica>` 内) 。                                                                       |
| `replica`                 | 副本配置段。可以有多个这样的配置段。                                                                                                             |
| `replica/host`            | MySQL 主机。                                                                                                                      |
| `replica/priority`        | 副本优先级。尝试连接时，ClickHouse 会按优先级顺序遍历各副本。数字越小，优先级越高。                                                                                |
| `db`                      | database 名称。                                                                                                                   |
| `table`                   | 表名。                                                                                                                            |
| `where`                   | 选择条件。条件语法与 MySQL 中的 `WHERE` 子句相同，例如 `id > 10 AND id < 20`。可选。                                                                  |
| `invalidate_query`        | 用于检查字典状态的查询。可选。详见 [Refreshing dictionary data using LIFETIME](../lifetime.md) 一节。                                              |
| `fail_on_connection_loss` | 控制连接丢失时服务器的行为。如果为 `true`，当 client 与 server 之间的连接丢失时，会立即抛出异常。如果为 `false`，server 在报告错误前会至少重试三次以拉取数据。请注意，重试会导致响应时间增加。默认值：`false`。 |
| `query`                   | 自定义查询。可选。                                                                                                                      |
| `enable_compression`      | 为 MySQL 协议连接启用 zlib 压缩。设置为 `1` 时，ClickHouse 会向 MySQL 服务器请求协议级压缩。也可以在 `<replica>` 内为每个副本单独设置。默认值：`0`。                           |

:::note
`table` 或 `where` 字段不能与 `query` 字段同时使用。此外，`table` 和 `query` 两个字段中必须声明其中一个。
:::

:::note
没有显式的 `secure` 参数。建立 SSL 连接时，安全为必选项。
:::

MySQL 可以在本地主机上通过套接字连接。为此，请设置 `host` 和 `socket`。

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        host 'localhost'
        socket '/path/to/socket/file.sock'
        user 'clickhouse'
        password 'qwerty'
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
      <mysql>
          <host>localhost</host>
          <socket>/path/to/socket/file.sock</socket>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>