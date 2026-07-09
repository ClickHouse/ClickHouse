---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'MongoDB 字典源'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: '将 MongoDB 配置为 ClickHouse 的字典源。'
doc_type: '参考'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MONGODB(
        host 'localhost'
        port 27017
        user ''
        password ''
        db 'test'
        collection 'dictionary_source'
        options 'ssl=true'
    ))
    ```

    或使用 URI：

    ```sql
    SOURCE(MONGODB(
        uri 'mongodb://localhost:27017/clickhouse'
        collection 'dictionary_source'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
        <mongodb>
            <host>localhost</host>
            <port>27017</port>
            <user></user>
            <password></password>
            <db>test</db>
            <collection>dictionary_source</collection>
            <options>ssl=true</options>
        </mongodb>
    </source>
    ```

    或使用 URI：

    ```xml
    <source>
        <mongodb>
            <uri>mongodb://localhost:27017/test?ssl=true</uri>
            <collection>dictionary_source</collection>
        </mongodb>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

设置字段：

| Setting      | Description                            |
| ------------ | -------------------------------------- |
| `host`       | MongoDB 主机。                            |
| `port`       | MongoDB 服务器的端口。                        |
| `user`       | MongoDB 用户名。                           |
| `password`   | MongoDB 用户的密码。                         |
| `db`         | 数据库名称。                                 |
| `collection` | 集合名称。                                  |
| `options`    | MongoDB 连接字符串选项。可选。                    |
| `uri`        | 用于建立连接的 URI (可替代单独的 host/port/db 字段) 。 |

[有关该引擎的更多信息](/zh/engines/table-engines/integrations/mongodb)