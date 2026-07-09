---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'MongoDB Dictionary ソース'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: 'MongoDB を ClickHouse の Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

設定例:

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

    または、URI を使用することもできます。

    ```sql
    SOURCE(MONGODB(
        uri 'mongodb://localhost:27017/clickhouse'
        collection 'dictionary_source'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
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

    または、URI を使用することもできます。

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

設定フィールド:

| 設定           | 説明                                           |
| ------------ | -------------------------------------------- |
| `host`       | MongoDB のホスト。                                |
| `port`       | MongoDB サーバーのポート。                            |
| `user`       | MongoDB ユーザー名。                               |
| `password`   | MongoDB ユーザーのパスワード。                          |
| `db`         | データベース名。                                     |
| `collection` | コレクション名。                                     |
| `options`    | MongoDB 接続文字列のオプション。省略可能です。                  |
| `uri`        | 接続の確立に使用する URI (個別の host/port/db フィールドの代替) 。 |

[このエンジンの詳細](/ja/engines/table-engines/integrations/mongodb)