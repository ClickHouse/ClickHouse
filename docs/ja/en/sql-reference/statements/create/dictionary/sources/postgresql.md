---
slug: /sql-reference/statements/create/dictionary/sources/postgresql
title: 'PostgreSQL Dictionary ソース'
sidebar_position: 12
sidebar_label: 'PostgreSQL'
description: 'ClickHouse で PostgreSQL を Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

設定例:

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

  <TabItem value="xml" label="設定ファイル">
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

設定フィールド:

| Setting                | Description                                                                                                          |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `host`                 | PostgreSQL server のホストです。すべてのレプリカに対して指定することも、各レプリカごとに個別に指定することもできます (`<replica>` 内) 。                                |
| `port`                 | PostgreSQL server のポートです。すべてのレプリカに対して指定することも、各レプリカごとに個別に指定することもできます (`<replica>` 内) 。                                |
| `user`                 | PostgreSQL ユーザー名です。すべてのレプリカに対して指定することも、各レプリカごとに個別に指定することもできます (`<replica>` 内) 。                                      |
| `password`             | PostgreSQL ユーザーのパスワードです。すべてのレプリカに対して指定することも、各レプリカごとに個別に指定することもできます (`<replica>` 内) 。                                 |
| `replica`              | レプリカ設定のセクションです。複数指定できます。                                                                                             |
| `replica/host`         | PostgreSQL のホストです。                                                                                                   |
| `replica/port`         | PostgreSQL のポートです。                                                                                                   |
| `replica/priority`     | レプリカの優先度です。接続を試みる際、ClickHouse は優先度の順にレプリカをたどります。数値が小さいほど優先度は高くなります。                                                  |
| `db`                   | データベース名です。                                                                                                           |
| `table`                | テーブル名です。                                                                                                             |
| `where`                | 選択条件です。条件の構文は PostgreSQL の `WHERE` clause と同じです。たとえば `id > 10 AND id < 20` です。省略可能です。                                |
| `invalidate_query`     | Dictionary の status を確認するためのクエリです。省略可能です。詳しくは [Refreshing dictionary data using LIFETIME](../lifetime.md) を参照してください。 |
| `background_reconnect` | connection に失敗した場合に、バックグラウンドでレプリカへ再接続します。省略可能です。                                                                     |
| `query`                | カスタムクエリです。省略可能です。                                                                                                    |

:::note
`table` または `where` フィールドは、`query` フィールドと一緒には使用できません。また、`table` または `query` フィールドのいずれか一方を必ず指定する必要があります。
:::