---
slug: /sql-reference/statements/create/dictionary/sources/cassandra
title: 'Cassandra の Dictionary ソース'
sidebar_position: 11
sidebar_label: 'Cassandra'
description: 'ClickHouse で Cassandra を Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

設定例:

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

  <TabItem value="xml" label="設定ファイル">
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

設定項目:

| Setting                | Description                                                                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                 | Cassandra のホスト、またはカンマ区切りのホスト一覧。                                                                                                                                             |
| `port`                 | Cassandra サーバーのポート。指定しない場合は、デフォルトのポート `9042` が使用されます。                                                                                                                       |
| `user`                 | Cassandra ユーザー名。                                                                                                                                                            |
| `password`             | Cassandra ユーザーのパスワード。                                                                                                                                                       |
| `keyspace`             | キースペース (データベース) の名前。                                                                                                                                                        |
| `column_family`        | カラムファミリー (テーブル) の名前。                                                                                                                                                        |
| `allow_filtering`      | クラスタリング キーカラムに対して、コストが高くなる可能性のある条件を許可するかどうかを指定するフラグです。デフォルト値は `1` です。                                                                                                       |
| `partition_key_prefix` | Cassandra テーブルの主キーに含まれるパーティションキー カラムの数です。複合キー Dictionary では必須です。Dictionary 定義内のキーカラムの順序は、Cassandra と同じである必要があります。デフォルト値は `1` です (最初のキーカラムがパーティションキーで、他のキーカラムはクラスタリングキーです) 。 |
| `consistency`          | 整合性レベル。設定可能な値: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`。デフォルト値は `One` です。                                   |
| `where`                | 任意の絞り込み条件。                                                                                                                                                                  |
| `max_threads`          | 複合キー Dictionary で複数のパーティションからデータを読み込む際に使用するスレッドの最大数です。                                                                                                                      |
| `query`                | カスタムクエリ。任意です。                                                                                                                                                               |

:::note
`column_family` または `where` フィールドは、`query` フィールドと併用できません。また、`column_family` または `query` フィールドのいずれか一方を必ず指定する必要があります。
:::