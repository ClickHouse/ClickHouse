---
slug: /ja/operations/external-authenticators/http
title: "HTTP"
---
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

HTTPサーバーは、ClickHouseユーザーの認証に使用できます。HTTP認証は、既存の`users.xml`やローカルアクセス制御パスで定義されたユーザーに対してのみ外部認証器として使用できます。現在、GETメソッドを使用する[Basic](https://datatracker.ietf.org/doc/html/rfc7617)認証方式がサポートされています。

## HTTP認証サーバーの定義 {#http-auth-server-definition}

HTTP認証サーバーを定義するには、`config.xml`に`http_authentication_servers`セクションを追加する必要があります。

**例**
```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

`http_authentication_servers`セクション内に異なる名前を使用して複数のHTTPサーバーを定義できます。

**パラメータ**
- `uri` - 認証リクエストを行うためのURI

サーバーと通信するために使用されるソケットのミリ秒単位のタイムアウト:
- `connection_timeout_ms` - デフォルト: 1000 ms
- `receive_timeout_ms` - デフォルト: 1000 ms
- `send_timeout_ms` - デフォルト: 1000 ms

リトライのパラメータ:
- `max_tries` - 認証リクエストを行う最大試行回数。デフォルト: 3
- `retry_initial_backoff_ms` - リトライ時の初期バックオフ間隔。デフォルト: 50 ms
- `retry_max_backoff_ms` - 最大バックオフ間隔。デフォルト: 1000 ms

### `users.xml`でのHTTP認証の有効化 {#enabling-http-auth-in-users-xml}

ユーザーに対してHTTP認証を有効化するには、ユーザー定義で`password`や類似セクションの代わりに`http_authentication`セクションを指定します。

パラメータ:
- `server` - 主な`config.xml`ファイルで前述のように設定したHTTP認証サーバーの名前。
- `scheme` - HTTP認証方式。現在は`Basic`のみサポートされています。デフォルト: Basic

例 (`users.xml`に記述):
```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </my_user>
</clickhouse>
```

:::note
HTTP認証は他の認証メカニズムと併用することはできません。`http_authentication`と並んで`password`のような他のセクションが存在する場合、ClickHouseはシャットダウンします。
:::

### SQLを使用したHTTP認証の有効化 {#enabling-http-auth-using-sql}

ClickHouseで[SQLによるアクセス制御とアカウント管理](/docs/ja/guides/sre/user-management/index.md#access-control)が有効になっている場合、HTTP認証で認識されたユーザーをSQL文を使用して作成することもできます。

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...もしくは、`scheme`を明示的に定義せずデフォルトの`Basic`を使用

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

### セッション設定の伝達 {#passing-session-settings}

HTTP認証サーバーからの応答ボディがJSON形式で`settings`サブオブジェクトを持っている場合、ClickHouseはそのキー: 値ペアを文字列値として解析し、認証されたユーザーの現在のセッションの設定としてセットしようとします。解析が失敗した場合、サーバーからの応答ボディは無視されます。
