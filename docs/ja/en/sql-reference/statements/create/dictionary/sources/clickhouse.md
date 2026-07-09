---
slug: /sql-reference/statements/create/dictionary/sources/clickhouse
title: 'ClickHouse Dictionary ソース'
sidebar_position: 8
sidebar_label: 'ClickHouse'
description: 'ClickHouse テーブルを Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

設定の例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CLICKHOUSE(
        host 'example01-01-1'
        port 9000
        user 'default'
        password ''
        db 'default'
        table 'ids'
        where 'id=10'
        secure 1
        query 'SELECT id, value_1, value_2 FROM default.ids'
    ));
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <source>
        <clickhouse>
            <host>example01-01-1</host>
            <port>9000</port>
            <user>default</user>
            <password></password>
            <db>default</db>
            <table>ids</table>
            <where>id=10</where>
            <secure>1</secure>
            <query>SELECT id, value_1, value_2 FROM default.ids</query>
        </clickhouse>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

設定フィールド:

| Setting            | Description                                                                                                                                    |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`             | ClickHouse のホスト。ローカルホストの場合、クエリはネットワーク通信を行わずに処理されます。耐障害性を高めるには、[Distributed](/ja/engines/table-engines/special/distributed) テーブルを作成し、後続の設定で指定できます。 |
| `port`             | ClickHouse サーバーのポート。                                                                                                                           |
| `user`             | ClickHouse ユーザー名。                                                                                                                              |
| `password`         | ClickHouse ユーザーのパスワード。                                                                                                                         |
| `db`               | データベース名。                                                                                                                                       |
| `table`            | テーブル名。                                                                                                                                         |
| `where`            | 選択条件。省略可能です。                                                                                                                                   |
| `invalidate_query` | Dictionary の状態を確認するためのクエリです。省略可能です。詳しくは [LIFETIME を使用した Dictionary データの更新](../lifetime.md) セクションを参照してください。                                     |
| `secure`           | 接続に SSL を使用します。                                                                                                                                |
| `query`            | カスタムクエリ。省略可能です。                                                                                                                                |

:::note
`table` または `where` フィールドは、`query` フィールドと併用できません。また、`table` または `query` フィールドのいずれかは必ず指定する必要があります。
:::