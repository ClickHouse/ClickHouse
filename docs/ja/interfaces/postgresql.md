---
slug: /ja/interfaces/postgresql
sidebar_position: 20
sidebar_label: PostgreSQL インターフェース
---

# PostgreSQL インターフェース

ClickHouse は PostgreSQL のワイヤープロトコルをサポートしており、Postgres クライアントを使用して ClickHouse に接続することが可能です。ある意味、ClickHouse は PostgreSQL インスタンスのように振る舞うことができ、Amazon Redshift など、ClickHouse に直接サポートされていない PostgreSQL クライアントアプリケーションを ClickHouse に接続することができます。

PostgreSQL ワイヤープロトコルを有効にするには、サーバーの設定ファイルに [postgresql_port](../operations/server-configuration-parameters/settings.md#postgresql_port) 設定を追加します。例えば、`config.d` フォルダ内に新しい XML ファイルを作成し、ポートを定義します：

```xml
<clickhouse>
	<postgresql_port>9005</postgresql_port>
</clickhouse>
```

ClickHouse サーバーを起動し、**PostgreSQL compatibility protocol をリスニング中** というメッセージを含むログを確認します：

```response
{} <Information> Application: Listening for PostgreSQL compatibility protocol: 127.0.0.1:9005
```

## psql を ClickHouse に接続する

次のコマンドは、PostgreSQL クライアント `psql` を ClickHouse に接続する方法を示します：

```bash
psql -p [port] -h [hostname] -U [username] [database_name]
```

例：

```bash
psql -p 9005 -h 127.0.0.1 -U alice default
```

:::note
`psql` クライアントはパスワードでのログインを要求するため、パスワードなしでは `default` ユーザーを使用して接続することはできません。`default` ユーザーにパスワードを設定するか、別のユーザーでログインしてください。
:::

`psql` クライアントはパスワードを求めます：

```response
Password for user alice:
psql (14.2, server 22.3.1.1)
WARNING: psql major version 14, server major version 22.
         Some psql features might not work.
Type "help" for help.

default=>
```

以上で完了です！PostgreSQL クライアントが ClickHouse に接続され、すべてのコマンドとクエリは ClickHouse 上で実行されます。

:::note
現在、PostgreSQL プロトコルはプレーンテキストパスワードのみをサポートしています。
:::

## SSL の使用

ClickHouse インスタンスで SSL/TLS が設定されている場合、`postgresql_port` は同じ設定を使用します（ポートはセキュアなクライアントと非セキュアなクライアントの両方で共有されます）。

各クライアントは、SSL を使用して接続するための独自の方法があります。次のコマンドは、証明書とキーを渡して `psql` を ClickHouse にセキュアに接続する方法を示しています：

```bash
psql "port=9005 host=127.0.0.1 user=alice dbname=default sslcert=/path/to/certificate.pem sslkey=/path/to/key.pem sslrootcert=/path/to/rootcert.pem sslmode=verify-ca"
```

詳細な SSL 設定については、[PostgreSQL ドキュメント](https://jdbc.postgresql.org/documentation/head/ssl-client.html)を参照してください。
