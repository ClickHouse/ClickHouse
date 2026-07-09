---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'HTTP(S) Dictionary ソース'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: 'ClickHouse で HTTP または HTTPS のエンドポイントを Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

HTTP(S) サーバーの扱いは、[Dictionary がメモリ内にどのように格納されるか](../layouts/)によって異なります。Dictionary が `cache` および `complex_key_cache` を使って格納されている場合、ClickHouse は `POST` メソッドでリクエストを送信し、必要なキーを要求します。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

ClickHouse が HTTPS リソースにアクセスできるようにするには、サーバー設定で [openSSL を設定](/ja/operations/server-configuration-parameters/settings#openssl)する必要があります。

設定フィールド:

| Setting       | Description                                                               |
| ------------- | ------------------------------------------------------------------------- |
| `url`         | ソース URL。                                                                  |
| `format`      | ファイルのフォーマット。[Formats](/ja/sql-reference/formats) で説明されているすべてのフォーマットをサポートします。 |
| `credentials` | Basic HTTP authentication。省略可能です。                                         |
| `user`        | 認証に必要なユーザー名。                                                              |
| `password`    | 認証に必要なパスワード。                                                              |
| `headers`     | HTTP リクエストで使用するすべてのカスタム HTTP headers エントリ。省略可能です。                         |
| `header`      | 単一の HTTP header エントリ。                                                     |
| `name`        | リクエスト送信時に header で使用する Identifier 名。                                      |
| `value`       | 特定の Identifier 名に設定する値。                                                   |

DDL コマンド (`CREATE DICTIONARY ...`) を使用して Dictionary を作成する際、データベースユーザーが任意の HTTP サーバーにアクセスするのを防ぐため、HTTP Dictionary のリモートホストは config の `remote_url_allow_hosts` セクションの内容に照らして検証されます。