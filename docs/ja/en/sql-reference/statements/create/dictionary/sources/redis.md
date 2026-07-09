---
slug: /sql-reference/statements/create/dictionary/sources/redis
title: 'Redis Dictionary ソース'
sidebar_position: 10
sidebar_label: 'Redis'
description: 'ClickHouse で Redis を Dictionary ソースとして設定する方法。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

設定の例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(REDIS(
        host 'localhost'
        port 6379
        storage_type 'simple'
        db_index 0
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <source>
        <redis>
            <host>localhost</host>
            <port>6379</port>
            <storage_type>simple</storage_type>
            <db_index>0</db_index>
        </redis>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

設定フィールド:

| 設定             | 説明                                                                                                                                                                                                                                                                                                                              |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`         | Redis のホスト。                                                                                                                                                                                                                                                                                                                     |
| `port`         | Redis サーバー上のポート。                                                                                                                                                                                                                                                                                                                |
| `storage_type` | オプションの処理に使用される Redis 内部ストレージの構造です。`simple` はフラットなキー・バリュー map を使用し、シンプルキーのレイアウトに加えて、単一カラムの複合キーのレイアウト (`complex_key_cache` や `complex_key_direct` など) もサポートします。`hash_map` は Redis hash を使用し、複合キーが複数カラムで構成される場合に必要です。この場合、キーカラムはちょうど 2 つである必要があります。キーカラムは整数型または String 型である必要があります。範囲付きレイアウトはサポートされていません。デフォルト値は `simple` です。任意です。 |
| `db_index`     | Redis の論理 database の数値インデックスです。デフォルト値は `0` です。任意です。                                                                                                                                                                                                                                                                             |