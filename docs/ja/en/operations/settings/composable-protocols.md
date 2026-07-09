---
description: 'コンポーザブルプロトコルにより、ClickHouseサーバーへのTCPアクセスをより柔軟に構成できます。'
sidebar_label: 'コンポーザブルプロトコル'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'コンポーザブルプロトコル'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

コンポーザブルプロトコルを使用すると、ClickHouseサーバーへの TCP アクセスを
より柔軟に設定できます。この設定は、従来の設定と併存させることも、置き換える
こともできます。

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## コンポーザブルプロトコルの設定
</div>

コンポーザブルプロトコルは、XML設定ファイルで設定できます。XML設定ファイルでは、
`protocols` タグでプロトコルのセクションを示します:

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### プロトコルレイヤーの設定
</div>

基本的なモジュールを使用して、プロトコルレイヤーを定義できます。たとえば、HTTP レイヤーを定義するには、`protocols` セクションに新しい基本モジュールを追加できます。

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

モジュールは、次の項目で設定できます。

* `plain_http` - 別のレイヤーから参照できる名前
* `type` - データを処理するために生成されるプロトコルハンドラーを示します。
  使用できる定義済みのプロトコルハンドラーは次のとおりです。
  * `tcp` - ネイティブの ClickHouse プロトコルハンドラー
  * `http` - HTTP ClickHouse プロトコルハンドラー
  * `tls` - TLS 暗号化レイヤー
  * `proxy1` - PROXYv1 レイヤー
  * `mysql` - MySQL 互換プロトコルハンドラー
  * `postgres` - PostgreSQL 互換プロトコルハンドラー
  * `prometheus` - Prometheus プロトコルハンドラー
  * `interserver` - ClickHouse の interserver ハンドラー

:::note
`gRPC` プロトコルハンドラーは `コンポーザブルプロトコル` では実装されていません
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### エンドポイントの設定
</div>

エンドポイント (待ち受けポート) は、`<port>` と省略可能な `<host>` タグで表されます。
たとえば、先ほど追加した HTTP レイヤーのエンドポイントを設定するには、
次のように設定を変更できます。

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

`<host>` タグが省略された場合は、ルート設定の `<listen_host>` が
使用されます。

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### レイヤーシーケンスの設定
</div>

レイヤーシーケンスは、`<impl>` タグを使用して別のモジュールを参照することで定義します。たとえば、plain&#95;http モジュールの上位に TLS レイヤーを設定するには、設定をさらに次のように変更します。

```xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="endpoint-can-be-attached-to-any-layer">
  ### レイヤーへのエンドポイントの追加
</div>

エンドポイントは任意のレイヤーに追加できます。たとえば、HTTP (ポート 8123) および HTTPS (ポート 8443) 用のエンドポイントを定義できます。

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### 追加のエンドポイントの定義
</div>

追加のエンドポイントは、任意のモジュールを参照し、`<type>` タグを省略することで
定義できます。たとえば、`plain_http` モジュールに対して `another_http`
エンドポイントを次のように定義できます。

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

<div id="custom-http-handlers-per-endpoint">
  ### エンドポイントごとのカスタム HTTP ハンドラー
</div>

デフォルトでは、すべての `type=http` プロトコルのエントリで同じ `<http_handlers>`
設定が共有されます。これを上書きするには、別の設定セクションを参照する `<handlers>` タグを追加します。これにより、各 HTTP ポートで
異なる HTTP ルーティングルールのセットを提供できるようになります。

たとえば、ポート 8124 で独自のハンドラーを持つ別の HTTP API を実行するには、次のようにします。

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

この例では、ポート8123へのリクエストには標準の`<http_handlers>`ルールが使用され、
ポート8124へのリクエストには`<http_handlers_alt>`ルールが使用されます。`<handlers>`
を省略した場合、エンドポイント はデフォルトの`<http_handlers>`にフォールバックします。

カスタムハンドラーセクションは、
[`<http_handlers>`](/ja/docs/operations/server-configuration-parameters/settings#http_handlers)と同じフォーマットに従います。
カスタムハンドラーセクションへの変更は config のリロード時に検出され、対応する
エンドポイント は自動的に自動的に再起動されます。

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### 追加のレイヤーパラメータを指定する
</div>

モジュールによっては、追加のレイヤーパラメータを指定できます。たとえば、TLS レイヤーでは、次のように秘密鍵 (`privateKeyFile`) と証明書ファイル (`certificateFile`) を指定できます。

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```