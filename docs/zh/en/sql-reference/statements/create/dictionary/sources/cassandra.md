---
slug: /sql-reference/statements/create/dictionary/sources/cassandra
title: 'Cassandra 字典源'
sidebar_position: 11
sidebar_label: 'Cassandra'
description: '将 Cassandra 配置为 ClickHouse 的字典源。'
doc_type: '参考'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CASSANDRA(
        host 'localhost'
        port 9042
        user 'username'
        password 'qwerty123'
        keyspace 'database_name'
        column_family 'table_name'
        allow_filtering 1
        partition_key_prefix 1
        consistency 'One'
        where '"SomeColumn" = 42'
        max_threads 8
        query 'SELECT id, value_1, value_2 FROM database_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
        <cassandra>
            <host>localhost</host>
            <port>9042</port>
            <user>username</user>
            <password>qwerty123</password>
            <keyspase>database_name</keyspase>
            <column_family>table_name</column_family>
            <allow_filtering>1</allow_filtering>
            <partition_key_prefix>1</partition_key_prefix>
            <consistency>One</consistency>
            <where>"SomeColumn" = 42</where>
            <max_threads>8</max_threads>
            <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
        </cassandra>
    </source>
    ```
  </TabItem>
</Tabs>

设置字段：

| 设置                     | 说明                                                                                                                    |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `host`                 | Cassandra 主机，或以逗号分隔的主机列表。                                                                                             |
| `port`                 | Cassandra 服务器的端口。若未指定，则使用默认端口 `9042`。                                                                                 |
| `user`                 | Cassandra 用户名。                                                                                                        |
| `password`             | Cassandra 用户的密码。                                                                                                      |
| `keyspace`             | keyspace (数据库) 名称。                                                                                                    |
| `column_family`        | column family (表) 名称。                                                                                                 |
| `allow_filtering`      | 用于控制是否允许在聚簇键列上使用可能开销较高的条件。默认值为 `1`。                                                                                   |
| `partition_key_prefix` | Cassandra 表主键中分区键列的数量。组合键字典必须设置此项。字典定义中的键列顺序必须与 Cassandra 中保持一致。默认值为 `1` (即第一个键列是分区键，其他键列是聚簇键) 。                      |
| `consistency`          | 一致性级别。可选值：`One`、`Two`、`Three`、`All`、`EachQuorum`、`Quorum`、`LocalQuorum`、`LocalOne`、`Serial`、`LocalSerial`。默认值为 `One`。 |
| `where`                | 可选的筛选条件。                                                                                                              |
| `max_threads`          | 在组合键字典中从多个分区加载数据时，可使用的最大线程数。                                                                                          |
| `query`                | 自定义查询。可选。                                                                                                             |

:::note
`column_family` 或 `where` 字段不能与 `query` 字段同时使用。此外，必须声明 `column_family` 或 `query` 字段中的其中一个。
:::