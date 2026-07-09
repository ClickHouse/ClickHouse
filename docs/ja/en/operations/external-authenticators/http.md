---
description: 'HTTP に関するドキュメント'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

HTTPサーバーを使用して、ClickHouseユーザーを認証できます。HTTP認証は、`users.xml` またはローカルのアクセス制御パスで定義された既存ユーザーに対する外部認証としてのみ使用できます。現在は、GETメソッドを使用する [Basic](https://datatracker.ietf.org/doc/html/rfc7617) 認証スキームがサポートされています。

<div id="http-auth-server-definition">
  ## HTTP認証サーバーの定義
</div>

HTTP認証サーバーを定義するには、`config.xml` に `http_authentication_servers` セクションを追加する必要があります。

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
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

なお、`http_authentication_servers` セクション内では、異なる名前を使用して複数の HTTP サーバーを定義できます。

**パラメーター**

* `uri` - 認証リクエストの送信先 URI

サーバーとの通信に使用するソケットのタイムアウト (ミリ秒) :

* `connection_timeout_ms` - デフォルト: 1000 ms。
* `receive_timeout_ms` - デフォルト: 1000 ms。
* `send_timeout_ms` - デフォルト: 1000 ms。

再試行パラメーター:

* `max_tries` - 認証リクエストを行う最大試行回数。デフォルト: 3
* `retry_initial_backoff_ms` - 再試行時の backoff の初期間隔。デフォルト: 50 ms
* `retry_max_backoff_ms` - backoff の最大間隔。デフォルト: 1000 ms

転送ヘッダー:

この部分では、クライアントのリクエストヘッダーから外部 HTTP 認証サービスに転送するヘッダーを定義します。ヘッダーは設定で定義されたものと大文字・小文字を区別せずに照合されますが、転送時にはそのまま、つまり変更されずに送信される点に注意してください。

<div id="enabling-http-auth-in-users-xml">
  ### `users.xml` で HTTP認証を有効にする
</div>

ユーザーに HTTP認証を有効にするには、ユーザー定義で `password` などのセクションの代わりに `http_authentication` セクションを指定します。

パラメーター:

* `server` - 前述のとおり、メインの `config.xml` ファイルで設定した HTTP認証サーバーの名前。
* `scheme` - HTTP認証の認証スキーム。現在サポートされているのは `Basic` のみです。デフォルト: Basic

例 (`users.xml` に記述) :

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
HTTP認証は、他の認証方式と併用できない点に注意してください。`http_authentication` とあわせて `password` などの別のセクションが存在すると、ClickHouse は強制終了します。
:::

<div id="enabling-http-auth-using-sql">
  ### SQL を使用した HTTP 認証の有効化
</div>

ClickHouse で [SQL による Access Control and Account Management](/ja/operations/access-rights#access-control-usage) が有効な場合、HTTP 認証で識別されるユーザーは SQL ステートメントを使って作成することもできます。

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...または、認証スキームを明示的に指定しない場合は、`Basic` がデフォルトです

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### セッション設定の受け渡し
</div>

HTTP認証サーバーからのレスポンスボディが JSON フォーマットで、`settings` サブオブジェクトを含んでいる場合、ClickHouse はその key: value のペアを文字列の値としてパースし、認証されたユーザーの現在のセッションのセッション設定として設定しようとします。パースに失敗した場合、サーバーからのレスポンスボディは無視されます。