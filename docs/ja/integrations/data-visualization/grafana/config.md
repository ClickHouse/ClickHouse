---
sidebar_label: プラグイン設定
sidebar_position: 3
slug: /ja/integrations/grafana/config
description: GrafanaにおけるClickHouseデータソースプラグインの設定オプション
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_native.md';

# GrafanaでのClickHouseデータソースの設定

設定を変更する最も簡単な方法は、GrafanaのUIでプラグイン設定ページを操作することですが、データソースは[YAMLファイルでプロビジョニング](https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources)することもできます。

このページでは、ClickHouseプラグインの設定で利用可能なオプションの一覧と、YAMLを使用してデータソースをプロビジョニングする人向けの設定スニペットを紹介します。

全てのオプションの概要については、設定オプションの完全なリストを[こちら](#all-yaml-options)にて確認できます。

## 共通設定

設定画面の例：
<img src={require('./images/config_common.png').default} class="image" alt="Secure native config の例" />

共通設定のためのYAML設定例：
```yaml
jsonData:
  host: 127.0.0.1 # (必須) サーバーアドレス。
  port: 9000      # (必須) サーバーポート。nativeの場合はデフォルトで9440（セキュア）および9000（インセキュア）。HTTPの場合はデフォルトで8443（セキュア）および8123（インセキュア）。

  protocol: native # (必須) 接続に使うプロトコル。"native" または "http" に設定可能。
  secure: false    # 接続がセキュアな場合は true に設定。

  username: default # 認証に使用されるユーザー名。

  tlsSkipVerify:     <boolean> # true に設定するとTLS検証をスキップします。
  tlsAuth:           <boolean> # TLSクライアント認証を有効にするにはtrueに設定。
  tlsAuthWithCACert: <boolean> # CA証明書が提供されている場合はtrueに設定。自己署名のTLS証明書を検証するために必要。

secureJsonData:
  password: secureExamplePassword # 認証に使用されるパスワード。

  tlsCACert:     <string> # TLS CA 証明書
  tlsClientCert: <string> # TLS クライアント証明書
  tlsClientKey:  <string> # TLS クライアントキー
```

UIから設定を保存した際には、`version` プロパティが追加されます。これは、設定が保存されたプラグインのバージョンを示します。

### HTTPプロトコル

HTTPプロトコルを介して接続する場合は、設定が追加で表示されます。

<img src={require('./images/config_http.png').default} class="image" alt="追加のHTTP設定オプション" />

#### HTTPパス

HTTPサーバーが異なるURLパスで公開されている場合、ここにそのパスを追加できます。

```yaml
jsonData:
  # 最初のスラッシュを除いた形式
  path: additional/path/example
```

#### カスタムHTTPヘッダー

サーバーに送信されるリクエストにカスタムヘッダーを追加できます。

ヘッダーはプレーンテキストまたはセキュア値のいずれかです。
すべてのヘッダーキーはプレーンテキストで保存され、セキュアヘッダー値は(`password`フィールドと同様に)セキュア設定で保存されます。

:::warning HTTP上のセキュア値
セキュアヘッダー値は、設定でセキュアに保存されますが、接続がセキュアではない場合でもHTTPで送信されます。
:::

プレーン/セキュアヘッダーのYAML例：
```yaml
jsonData:
  httpHeaders:
  - name: X-Example-Plain-Header
    value: plain text value
    secure: false
  - name: X-Example-Secure-Header
    # "value" は除外
    secure: true
secureJsonData:
  secureHttpHeaders.X-Example-Secure-Header: secure header value
```

## 追加設定

これらの追加設定はオプションです。

<img src={require('./images/config_additional.png').default} class="image" alt="追加設定の例" />

YAML例：
```yaml
jsonData:
  defaultDatabase: default # クエリビルダーでロードされるデフォルトのデータベース。デフォルトは "default"。
  defaultTable: <string>   # クエリビルダーでロードされるデフォルトのテーブル。

  dialTimeout: 10    # サーバーへの接続時のダイアルタイムアウト（秒）。デフォルトは "10"。
  queryTimeout: 60   # クエリ実行時のクエリタイムアウト（秒）。デフォルトは60。ユーザーに対する権限が必要です。権限エラーが発生する場合は、"0" に設定して無効化を試みてください。
  validateSql: false # true に設定すると、SQLエディタでSQLを検証します。
```

### OpenTelemetry

OpenTelemetry (OTel) はプラグイン内に深く統合されています。
OpenTelemetryデータは、[エクスポータープラグイン](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)を使用してClickHouseにエクスポートできます。
最善の利用のためには、[ログ](#logs)と[トレース](#traces)の両方のためにOTelを設定することを推奨しています。

[データリンク](./query-builder.md#data-links) を有効にするには、これらのデフォルトを設定する必要があります。この機能は強力な可観測性ワークフローを可能にします。

### ログ

[ログのクエリ構築](./query-builder.md#logs)を高速化するために、デフォルトのデータベース/テーブルと、ログクエリ用のカラムを設定することができます。これにより、クエリビルダーで実行可能なログクエリが事前にロードされ、観察ページでのブラウジングが高速化されます。

OpenTelemetryを使用している場合は、「**Use OTel**」スイッチを有効にし、**デフォルトログテーブル**を`otel_logs`に設定する必要があります。
これにより、選択したOTelスキーマバージョンを使用するためにデフォルトのカラムが自動的にオーバーライドされます。

OpenTelemetryはログには必須ではありませんが、単一のログ/トレースデータセットを使用すると、[データリンク](./query-builder.md#data-links)によるスムーズな可観測性ワークフローを促進します。

ログ設定画面の例：
<img src={require('./images/config_logs.png').default} class="image" alt="ログ設定" />

ログ設定のYAML例：
```yaml
jsonData:
  logs:
    defaultDatabase: default # デフォルトのログデータベース。
    defaultTable: otel_logs  # デフォルトのログテーブル。OTelを使用している場合、これは "otel_logs" に設定する必要があります。

    otelEnabled: false  # OTelが有効である場合にtrueに設定。
    otelVersion: latest # 使用されるotelコレクタースキーマバージョン。バージョンはUIで表示されますが、"latest"はプラグイン内で使用可能な最新のバージョンを使用します。

    # 新しいログクエリを開く際に選択されるデフォルトのカラム。OTelが有効である場合は無視されます。
    timeColumn:       <string> # ログの主な時間カラム。
    logLevelColumn:   <string> # ログのレベル/シビリティ。値は通常 "INFO"、"error"、または "Debug" のようになります。
    logMessageColumn: <string> # ログのメッセージ/コンテンツ。
```

### トレース

[トレースのクエリ構築](./query-builder.md#traces)を高速化するために、デフォルトのデータベース/テーブルと、トレースクエリ用のカラムを設定することができます。これにより、クエリビルダーで実行可能なトレース検索クエリが事前にロードされ、観察ページでのブラウジングが高速化されます。

OpenTelemetryを使用している場合は、「**Use OTel**」スイッチを有効にし、**デフォルトトレーステーブル**を`otel_traces`に設定する必要があります。
これにより、選択したOTelスキーマバージョンを使用するためにデフォルトのカラムが自動的にオーバーライドされます。
OpenTelemetryは必須ではありませんが、この機能はトレースにそのスキーマを使用するのが最適です。

トレース設定画面の例：
<img src={require('./images/config_traces.png').default} class="image" alt="トレース設定" />

トレース設定のYAML例：
```yaml
jsonData:
  traces:
    defaultDatabase: default  # デフォルトのトレースデータベース。
    defaultTable: otel_traces # デフォルトのトレーステーブル。OTelを使用している場合、これは "otel_traces" に設定する必要があります。

    otelEnabled: false  # OTelが有効である場合にtrueに設定。
    otelVersion: latest # 使用されるotelコレクタースキーマバージョン。バージョンはUIで表示されますが、"latest"はプラグイン内で使用可能な最新のバージョンを使用します。

    # 新しいトレースクエリを開くときに選択されるデフォルトのカラム。OTelが有効の場合は無視されます。
    traceIdColumn:       <string>    # トレースIDカラム。
    spanIdColumn:        <string>    # スパンIDカラム。
    operationNameColumn: <string>    # オペレーション名カラム。
    parentSpanIdColumn:  <string>    # 親スパンIDカラム。
    serviceNameColumn:   <string>    # サービス名カラム。
    durationTimeColumn:  <string>    # 継続時間カラム。
    durationUnitColumn:  <time unit> # 継続時間単位。 "seconds", "milliseconds", "microseconds", または "nanoseconds" に設定可能。OTelの場合デフォルトは "nanoseconds"。
    startTimeColumn:     <string>    # 開始時間カラム。トレーススパンの主な時間カラム。
    tagsColumn:          <string>    # タグカラム。これはマップタイプであることが期待されます。
    serviceTagsColumn:   <string>    # サービスタグカラム。これはマップタイプであることが期待されます。
```

### カラムエイリアス

カラムエイリアスは、データを異なる名前とタイプでクエリするのに便利な方法です。
エイリアスを使用することで、ネストされたスキーマを平坦化し、Grafanaで容易に選択できるようにします。

エイリアスが役立つ場合:
- スキーマとそのネストされたプロパティおよびタイプのほとんどを知っている場合
- データをMapタイプで保存している場合
- JSONを文字列として保存している場合
- 選択するカラムを変換するために関数を頻繁に適用する場合

#### テーブル定義のALIASカラム

ClickHouseはカラムエイリアスを組み込みでサポートしており、Grafanaに対応しています。
エイリアスカラムはテーブル上で直接定義できます。

```sql
CREATE TABLE alias_example (
  TimestampNanos DateTime(9),
  TimestampDate ALIAS toDate(TimestampNanos)
)
```

この例では、`TimestampNanos`を`Date`型に変換する`TimestampDate`というエイリアスを作成しています。
このデータは最初のカラムのようにディスクに格納されるのではなく、クエリの際に計算されます。
テーブル定義のエイリアスは `SELECT *` では返されませんが、これはサーバー設定で調整可能です。

詳しくは、[ALIAS](/docs/ja/sql-reference/statements/create/table#alias)カラム型のドキュメントを参照してください。

#### カラムエイリアステーブル

デフォルトでは、Grafanaは`DESC table`の応答に基づいてカラムの提案を行います。
一部のケースでは、Grafanaが表示するカラムを完全に上書きしたい場合があります。
これは、カラムを選択する際にGrafanaでスキーマを隠すのに役立ち、テーブルの複雑さに応じてユーザーエクスペリエンスを向上させます。

テーブル定義のエイリアスと比較して、テーブルを変更することなく簡単に更新できるのが利点です。一部のスキーマでは、エントリが数千に達することがあり、基礎となるテーブル定義を混雑させる可能性があります。また、ユーザーに無視させたいカラムを隠すことも可能です。

Grafanaはエイリアステーブルに以下のカラム構造を要求します：
```sql
CREATE TABLE aliases (
  `alias` String,  -- Grafanaカラムセレクタに表示されるエイリアスの名前
  `select` String, -- SQLジェネレータで使用するSELECT文法
  `type` String    -- 結果カラムのタイプ、プラグインがデータタイプに応じてUIオプションを変更できるように
)
```

以下は、エイリアステーブルを使用して`ALIAS`カラムの動作を再現する方法です：
```sql
CREATE TABLE example_table (
  TimestampNanos DateTime(9)
);

CREATE TABLE example_table_aliases (`alias` String, `select` String, `type` String);

INSERT INTO example_table_aliases (`alias`, `select`, `type`) VALUES
('TimestampNanos', 'TimestampNanos', 'DateTime(9)'), -- (オプション) テーブルから元のカラムを保持
('TimestampDate', 'toDate(TimestampNanos)', 'Date'); -- TimestampNanosをDateに変換する新しいカラムを追加
```

次に、このテーブルをGrafanaで使用するよう設定します。名前は任意で、別のデータベースで定義することも可能です：
<img src={require('./images/alias_table_config_example.png').default} class="image" alt="エイリアステーブル設定の例" />

これでGrafanaは、`DESC example_table`の結果ではなく、エイリアステーブルの結果を参照します：
<img src={require('./images/alias_table_select_example.png').default} class="image" alt="エイリアステーブル選択の例" />

両方の種類のエイリアスを使用して、複雑な型変換やJSONフィールドの抽出を実行できます。

## 全YAMLオプション

これらは、プラグインによって提供されるすべてのYAML設定オプションです。
一部のフィールドには、例の値が入っており、その他はフィールドの型を示します。

YAMLでデータソースをプロビジョニングするための詳細は、[Grafanaのドキュメント](https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources)を参照してください。

```yaml
datasources:
  - name: Example ClickHouse
    uid: clickhouse-example
    type: grafana-clickhouse-datasource
    jsonData:
      host: 127.0.0.1
      port: 9000
      protocol: native
      secure: false
      username: default
      tlsSkipVerify: <boolean>
      tlsAuth: <boolean>
      tlsAuthWithCACert: <boolean>
      defaultDatabase: default
      defaultTable: <string>
      dialTimeout: 10
      queryTimeout: 60
      validateSql: false
      httpHeaders:
      - name: X-Example-Plain-Header
        value: plain text value
        secure: false
      - name: X-Example-Secure-Header
        secure: true
      logs:
        defaultDatabase: default
        defaultTable: otel_logs
        otelEnabled: false
        otelVersion: latest
        timeColumn: <string>
        levelColumn: <string>
        messageColumn: <string>
      traces:
        defaultDatabase: default
        defaultTable: otel_traces
        otelEnabled: false
        otelVersion: latest
        traceIdColumn: <string>
        spanIdColumn: <string>
        operationNameColumn: <string>
        parentSpanIdColumn: <string>
        serviceNameColumn: <string>  
        durationTimeColumn: <string>
        durationUnitColumn: <time unit>
        startTimeColumn: <string>
        tagsColumn: <string>
        serviceTagsColumn: <string>
    secureJsonData:
      tlsCACert:     <string>
      tlsClientCert: <string>
      tlsClientKey:  <string>
      secureHttpHeaders.X-Example-Secure-Header: secure header value
```

