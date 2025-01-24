---
slug: /ja/operations/settings/composable-protocols
sidebar_position: 64
sidebar_label: Composable Protocols
---

# Composable Protocols

Composable Protocolsは、ClickHouseサーバーへのTCPアクセスの設定をより柔軟に構成できます。この設定は、従来の設定と共存するか、置き換えることができます。

## Composable Protocolsセクションは設定xml内で`protocols`として表されます
**例:**
``` xml
<protocols>

</protocols>
```

## 基本モジュールによってプロトコル層を定義します
**例:**
``` xml
<protocols>

  <!-- plain_httpモジュール -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```
ここで:
- `plain_http` - 他の層から参照されることができる名前
- `type` - データを処理するためにインスタンス化されるプロトコルハンドラを示し、プロトコルハンドラのセットは事前に定義されています:
  * `tcp` - ネイティブclickhouseプロトコルハンドラ
  * `http` - http clickhouseプロトコルハンドラ
  * `tls` - TLS暗号化層
  * `proxy1` - PROXYv1層
  * `mysql` - MySQL互換プロトコルハンドラ
  * `postgres` - PostgreSQL互換プロトコルハンドラ
  * `prometheus` - Prometheusプロトコルハンドラ
  * `interserver` - clickhouseインターサーバーハンドラ

:::note
`Composable Protocols`には`gRPC`プロトコルハンドラは実装されていません。
:::

## エンドポイント（リスニングポート）は`<port>`タグおよび（オプションで）`<host>`タグで表されます
**例:**
``` xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- エンドポイント -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```
`<host>`が省略された場合、ルート設定の`<listen_host>`が使用されます。

## 階層のシーケンスは、他のモジュールを参照する`<impl>`タグによって定義されます
**例:** HTTPSプロトコルの定義
``` xml
<protocols>

  <!-- httpモジュール -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- plain_httpモジュール上のTLSレイヤーとして設定されたhttpsモジュール -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

## エンドポイントは任意の階層にアタッチできます
**例:** HTTP（ポート8123）およびHTTPS（ポート8443）エンドポイントの定義
``` xml
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

## 追加のエンドポイントは任意のモジュールを参照し、`<type>`タグを省略することで定義できます
**例:** `plain_http`モジュールのために定義された`another_http`エンドポイント
``` xml
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

## 一部のモジュールはその層に特有のパラメーターを含めることができます
**例:** TLS層のためにプライベートキー（`privateKeyFile`）および証明書ファイル（`certificateFile`）を指定できます
``` xml
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
