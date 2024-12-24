---
title: OpenTelemetryの統合
description: オブザーバビリティのためのOpenTelemetryとClickHouseの統合
slug: /ja/observability/integrating-opentelemetry
keywords: [オブザーバビリティ, ログ, トレース, メトリクス, OpenTelemetry, Grafana, otel]
---

# データ収集のためのOpenTelemetryの統合

オブザーバビリティソリューションには、ログとトレースを収集およびエクスポートする手段が必要です。この目的のために、ClickHouseは[OpenTelemetry (OTel) プロジェクト](https://opentelemetry.io/)を推奨しています。

"OpenTelemetryは、トレース、メトリクス、ログなどのテレメトリデータを作成および管理するために設計されたオブザーバビリティフレームワークおよびツールキットです。"

ClickHouseやPrometheusとは異なり、OpenTelemetryはオブザーバビリティのバックエンドではなく、テレメトリデータの生成、収集、管理、エクスポートに焦点を当てています。OpenTelemetryの初期の目標は、ユーザーが言語固有のSDKを使用してアプリケーションやシステムを容易に計装できるようにすることでしたが、それはOpenTelemetryコレクターを通じたログの収集も含むように拡張されました。OpenTelemetryコレクターは、テレメトリデータを受信、処理、エクスポートするエージェントまたはプロキシです。

## ClickHouseに関連するコンポーネント

OpenTelemetryは多くのコンポーネントで構成されています。他には、API仕様、標準化されたプロトコル、フィールド/カラムの命名規則を提供するほか、ClickHouseでオブザーバビリティソリューションを構築するために基本的な二つの機能を提供します：

- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)は、テレメトリデータを受信、処理、エクスポートするプロキシです。ClickHouseが駆動するソリューションは、このコンポーネントをログ収集とイベント処理の両方に使用し、バッチ処理および挿入を行います。
- テレメトリデータの仕様、API、エクスポートを実装する[言語SDK](https://opentelemetry.io/docs/languages/)です。これらのSDKは、アプリケーションのコード内でトレースが正しく記録されていること、関連するスパンが生成され、メタデータを介してサービス間でコンテキストが伝達されることを保証します。これにより、分散トレースが形成され、スパンが相関されることを保証します。これらのSDKは、ユーザーがコードを変更することなく、直ちに計装を得ることを意味する一般的なライブラリとフレームワークを自動的に実装するエコシステムによって補完されています。

ClickHouseを駆動するオブザーバビリティソリューションは、これら両方のツールを活用します。

## ディストリビューション

OpenTelemetryコレクターには[多くのディストリビューション](https://github.com/open-telemetry/opentelemetry-collector-releases?tab=readme-ov-file)があります。ClickHouseソリューションに必要なfilelogレシーバーとClickHouseエクスポーターは、[OpenTelemetry Collector Contrib Distro](https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib)にのみ含まれています。

このディストリビューションには多くのコンポーネントが含まれており、ユーザーはさまざまな構成で実験することができます。ただし、製品で実行する場合は、環境に必要なコンポーネントのみを含めるようにコレクターを制限することが推奨されます。その理由には以下のようなものがあります：

- コレクターのサイズを減らし、コレクターのデプロイ時間を短縮する
- 利用可能な攻撃の表面積を減らすことで、コレクターのセキュリティを改善する

[カスタムコレクター](https://opentelemetry.io/docs/collector/custom-collector/)の構築は、[OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder)を使用して行うことができます。

## OTelによるデータの取り込み

### コレクターのデプロイ役割

ログを収集してClickHouseに挿入するために、OpenTelemetry Collectorの使用を推奨します。OpenTelemetry Collectorは、主に2つの役割でデプロイできます：

- **エージェント** - エージェントインスタンスは、サーバーやKubernetesノードなどのエッジでデータを収集したり、OpenTelemetry SDKで計装されたアプリケーションから直接イベントを受信したりします。後者の場合、エージェントインスタンスはアプリケーションと共に、またはアプリケーションと同じホストで実行されます（例えばサイドカーやDaemonSetとして）。エージェントはそのデータを直接ClickHouseに送信したり、ゲートウェイインスタンスに送信したりできます。前者の場合、これは[エージェントデプロイメントパターン](https://opentelemetry.io/docs/collector/deployment/agent/)として知られます。
- **ゲートウェイ** - ゲートウェイインスタンスは、独立したサービスを提供します（例：Kubernetesでのデプロイ）。通常、クラスターごと、データセンターごと、地域ごとに、この役割として動作します。これらはOTLPエンドポイントを介してアプリケーション（または他のコレクターとしてエージェント）からイベントを受信します。通常、一連のゲートウェイインスタンスがデプロイされ、ロードバランサが使用されて負荷を分散します。全エージェントとアプリケーションがこの単一のエンドポイントに信号を送る場合、これは[ゲートウェイデプロイメントパターン](https://opentelemetry.io/docs/collector/deployment/gateway/)としてしばしば言及されます。

以下、エージェントコレクターがシンプルにイベントをClickHouseに直接送信することを想定しています。ゲートウェイを使用する場合およびどのような場合に適用できるかについて詳細は「スケーリング with ゲートウェイ」を参照してください。

### ログの収集

コレクターを使用する主な利点は、サービスがデータを迅速にオフロードし、コレクターが再試行、バッチ処理、暗号化、または機密データのフィルタリングなどの追加処理を負担することを可能にすることです。

コレクターは、その3つの主要な処理ステージに対して[レシーバー](https://opentelemetry.io/docs/collector/configuration/#receivers)、[プロセッサー](https://opentelemetry.io/docs/collector/configuration/#processors)、および[エクスポーター](https://opentelemetry.io/docs/collector/configuration/#exporters)という用語を使用します。レシーバーはデータ収集に使用され、プルベースまたはプッシュベースのいずれかです。プロセッサーはメッセージの変換と強化を行う機能を提供します。エクスポーターはデータをダウンストリームサービスに送信する役割を持ちます。このサービスは理論的には別のコレクターでもかまいませんが、基本的なディスカッションのために、すべてのデータがClickHouseに直接送信されると仮定します。

<img src={require('./images/observability-3.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

ユーザーがレシーバー、プロセッサー、エクスポーターの完全なセットに慣れることをお勧めします。

コレクターはログを収集するための2つの主要なレシーバーを提供します：

**OTLPを介して** - この場合、ログはOTLPプロトコルを介してOpenTelemetry SDKからコレクターに直接（プッシュ）送信されます。[OpenTelemetryデモ](https://opentelemetry.io/docs/demo/)ではこのアプローチが採用されており、各言語のOTLPエクスポーターがローカルコレクターのエンドポイントを想定しています。コレクターはこの場合、OTLPレシーバーで設定する必要があります—設定例は上記[デモの設定](https://github.com/ClickHouse/opentelemetry-demo/blob/main/src/otelcollector/otelcol-config.yml#L5-L12)を参照。また、このアプローチの利点は、ログデータが自動的にTrace Idを含むようになり、ユーザーが特定のログ用のトレースを判別できるようになることです。

<img src={require('./images/observability-4.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

このアプローチでは、ユーザーが[適切な言語SDK](https://opentelemetry.io/docs/languages/)を使用してコードを計装することが求められます。

- **Filelogレシーバーを介したスクレイピング** - このレシーバーはディスク上のファイルを追跡し、ログメッセージを形成し、これらをClickHouseに送信します。このレシーバーは、多行メッセージの検出、ログのロールオーバー処理、再起動に対するロバスト性のチェックポイント設定、構造の抽出など、複雑なタスクを処理します。このレシーバーはさらに、DockerとKubernetesコンテナのログを追跡し、これらから構造を抽出し、ポッドの詳細で充実させることができます。

<img src={require('./images/observability-5.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

**ほとんどのデプロイメントでは上記の組み合わせを使用します。** ユーザーが[コレクタードキュメント](https://opentelemetry.io/docs/collector/)を読み、基本的な概念や[構成構造](https://opentelemetry.io/docs/collector/configuration/)および[インストール方法](https://opentelemetry.io/docs/collector/installation/)に慣れることをお勧めします。

> ヒント: [otelbin.io](https://www.otelbin.io/) は、構成を検証および視覚化するのに役立ちます。

## 構造化vs非構造化

ログは構造化または非構造化のいずれかです。

構造化ログは、JSONなどのデータフォーマットを使用し、httpコードやソースIPアドレスなどのメタデータフィールドを定義します。

```
{
    "remote_addr":"54.36.149.41",
    "remote_user":"-","run_time":"0","time_local":"2019-01-22 00:26:14.000","request_type":"GET",
    "request_path":"\/filter\/27|13 ,27|  5 ,p53","request_protocol":"HTTP\/1.1",
    "status":"200",
    "size":"30577",
    "referer":"-",
    "user_agent":"Mozilla\/5.0 (compatible; AhrefsBot\/6.1; +http:\/\/ahrefs.com\/robot\/)"
}
```

一方、非構造化ログは通常、何らかの固有の構造を持っているものの、単なる文字列としてログを表現します。

```
54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET
/filter/27|13%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,27|%DA%A9%D9%85%D8%AA%D8%B1%20%D8%A7%D8%B2%205%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,p53 HTTP/1.1" 200 30577 "-" "Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)" "-"
```

ユーザーが可能な限り構造化ログを使用し、JSON（例：ndjson）でログを記録することを推奨します。これにより、後のログ処理が簡素化され、ClickHouseに送信する前に[Collector Processor](https://opentelemetry.io/docs/collector/configuration/#processors)や挿入時にマテリアライズドビューを使用して簡素化されます。構造化ログは最終的に後に必要な処理リソースを節約し、ClickHouseソリューションに必要なCPUを削減します。

### 例

例として、構造化された（JSON）および非構造化のロギングデータセット（それぞれ約10m行）が以下のリンクで利用可能です：

- [非構造化](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/access-unstructured.log.gz)
- [構造化](https://datasets-documentation.s3.eu-west-3.amazonaws.com/http_logs/access-structured.log.gz)

以下の例では構造化データセットを使用します。このファイルをダウンロードして展開し、以下の例を再現してください。

以下は、ディスク上のこれらのファイルを読み込むOTel Collectorの単純な構成を示しており、filelogレシーバーを使用して、結果のメッセージをstdoutに出力します。ログが構造化されているため、[`json_parser`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/stanza/docs/operators/json_parser.md)オペレーターを使用します。`access-structured.log`ファイルへのパスを修正してください。

> 以下の例では、ログからタイムスタンプを抽出します。これには`json_parser`オペレーターを使用する必要があり、ログ行全体をJSON文字列に変換し、結果を`LogAttributes`に配置します。これは計算が高コストとなることがあり、[ClickHouseでより効率的に実行できます](https://clickhouse.com/blog/worlds-fastest-json-querying-tool-clickhouse-local) - 「SQLによる構造の抽出」を参照してください。JSONを抽出するために使用する`regex_parser`を使用した同等の非構造化例は[ここ](https://pastila.nl/?01da7ee2/2ffd3ba8124a7d6e4ddf39422ad5b863#swBkiAXvGP7mRPgbuzzHFA==)で提供されています。

**[config-structured-logs.yaml](https://www.otelbin.io/#config=receivers%3A*N_filelog%3A*N___include%3A*N_____-_%2Fopt%2Fdata%2Flogs%2Faccess-structured.log*N___start*_at%3A_beginning*N___operators%3A*N_____-_type%3A_json*_parser*N_______timestamp%3A*N_________parse*_from%3A_attributes.time*_local*N_________layout%3A_*%22*.Y-*.m-*.d_*.H%3A*.M%3A*.S*%22*N*N*Nprocessors%3A*N__batch%3A*N____timeout%3A_5s*N____send*_batch*_size%3A_1*N*N*Nexporters%3A*N_logging%3A*N___loglevel%3A_debug*N*N*Nservice%3A*N_pipelines%3A*N___logs%3A*N_____receivers%3A_%5Bfilelog%5D*N_____processors%3A_%5Bbatch%5D*N_____exporters%3A_%5Blogging%5D%7E)**

```yaml
receivers:
  filelog:
    include:
      - /opt/data/logs/access-structured.log
    start_at: beginning
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.time_local
          layout: '%Y-%m-%d %H:%M:%S'
processors:
  batch:
    timeout: 5s
    send_batch_size: 1
exporters:
  logging:
    loglevel: debug
service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [logging]
```

ユーザーは[公式の指示](https://opentelemetry.io/docs/collector/installation/)に従ってローカルにコレクターをインストールすることができます。重要なことは、これらの指示が[contribディストリビューション](https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib)（`filelog`レシーバーが含まれている）を使用するように修正されることです。例として、`otelcol_0.102.1_darwin_arm64.tar.gz`の代わりに`otelcol-contrib_0.102.1_darwin_arm64.tar.gz`をダウンロードします。リリースは[ここ](https://github.com/open-telemetry/opentelemetry-collector-releases/releases)で見つけることができます。

インストールが完了すると、以下のコマンドを使用してOTel Collectorを実行できます：

```
./otelcol-contrib --config config-logs.yaml
```

構造化されたログを使用すると仮定すると、メッセージは以下のような形式で出力されます：

```
LogRecord #98
ObservedTimestamp: 2024-06-19 13:21:16.414259 +0000 UTC
Timestamp: 2019-01-22 01:12:53 +0000 UTC
SeverityText:
SeverityNumber: Unspecified(0)
Body: Str({"remote_addr":"66.249.66.195","remote_user":"-","run_time":"0","time_local":"2019-01-22 01:12:53.000","request_type":"GET","request_path":"\/product\/7564","request_protocol":"HTTP\/1.1","status":"301","size":"178","referer":"-","user_agent":"Mozilla\/5.0 (Linux; Android 6.0.1; Nexus 5X Build\/MMB29P) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/41.0.2272.96 Mobile Safari\/537.36 (compatible; Googlebot\/2.1; +http:\/\/www.google.com\/bot.html)"})
Attributes:
 	-> remote_user: Str(-)
 	-> request_protocol: Str(HTTP/1.1)
 	-> time_local: Str(2019-01-22 01:12:53.000)
 	-> user_agent: Str(Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html))
 	-> log.file.name: Str(access.log)
 	-> status: Str(301)
 	-> size: Str(178)
 	-> referer: Str(-)
 	-> remote_addr: Str(66.249.66.195)
 	-> request_type: Str(GET)
 	-> request_path: Str(/product/7564)
 	-> run_time: Str(0)
Trace ID:
Span ID:
Flags: 0
```

上記は、OTelコレクターによって生成された単一のログメッセージを表しています。これら同じメッセージを後のセクションでClickHouseに投入します。

ログメッセージの完全なスキーマは、[こちら](https://opentelemetry.io/docs/specs/otel/logs/data-model/)で維持されています。**ユーザーはこのスキーマに慣れることを強くお勧めします。**

重要なのは、ログ行自体が`Body`フィールド内に文字列として保持されていることですが、JSONが`json_parser`のおかげでAttributesフィールドに自動抽出されていることです。この同じ[オペレーター](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/stanza/docs/operators/README.md#what-operators-are-available)を使用して、適切な`Timestamp`カラムにタイムスタンプを抽出しています。OTelでのログ処理の推奨事項については「処理」を参照してください。

> オペレーターは、ログ処理の最も基本的な単位です。各オペレーターは、ファイルから行を読み取る、フィールドからJSONを解析するなどの単一の責任を果たします。オペレーターは、その目的を達成するためにパイプラインで鎖でつながれます。

> 上記のメッセージにはTraceIDやSpanIDフィールドがありません。[分散トレース](https://opentelemetry.io/docs/concepts/observability-primer/#distributed-traces)を実装している場合など、必要な場合には、上記のようにJSONから抽出できます。

ローカルまたはkubernetesのログファイルを収集する必要があるユーザーには、[filelogレシーバー](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/receiver/filelogreceiver/README.md#configuration)で利用可能な構成オプションと[オフセットの取り扱い](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/filelogreceiver#offset-tracking)および[多行ログの解析方法](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/filelogreceiver#example---multiline-logs-parsing)に関して理解を深めることをお勧めします。

## Kubernetesログの収集

Kubernetesログの収集には、[Open Telemetryドキュメントガイド](https://opentelemetry.io/docs/kubernetes/)の利用をお勧めします。ログやメトリクスをポッドのメタデータで強化するために[Kubernetes Attributes Processor](https://opentelemetry.io/docs/kubernetes/collector/components/#kubernetes-attributes-processor)が推奨されます。これにより、ラベルのような動的なメタデータを生成することができます。このデータはカラム`ResourceAttributes`に保存されます。ClickHouseでは、現時点でこのカラムには型`Map(String, String)`を使用しています。この型の操作と最適化の詳細については「マップの使用」と「マップからの抽出」を参照してください。

## トレースの収集

コードを計装してトレースを収集したいユーザーには、公式の[OTelドキュメント](https://opentelemetry.io/docs/languages/)に従うことをお勧めします。

ClickHouseにイベントを送信するためには、適切なレシーバーを介してOTLPプロトコルでトレースイベントを受信するOTelコレクターをデプロイする必要があります。OpenTelemetryデモでは、[対応する言語を計装し](https://opentelemetry.io/docs/demo/)イベントをコレクターに送信する例を提供しています。stdoutにイベントを出力する適切なコレクターの設定例を以下に示します：

### 例

トレースはOTLPで受信される必要があるため、トレースデータ生成用に[telemetrygen](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen)ツールを使用します。インストールについての指示は[こちら](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen)にあります。

以下の設定は、OTLPレシーバーでトレースイベントを受け取り、それをstdoutに送信します。

[config-traces.xml](https://www.otelbin.io/#config=receivers%3A*N_otlp%3A*N___protocols%3A*N_____grpc%3A*N_______endpoint%3A_0.0.0.0%3A4317*N*Nprocessors%3A*N_batch%3A*N___timeout%3A_1s*N*Nexporters%3A*N_logging%3A*N___loglevel%3A_debug*N*Nservice%3A*N_pipelines%3A*N__traces%3A*N____receivers%3A_%5Botlp%5D*N____processors%3A_%5Bbatch%5D*N____exporters%3A_%5Blogging%5D%7E)

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
processors:
  batch:
    timeout: 1s
exporters:
  logging:
    loglevel: debug
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging]
```

この設定を以下のコマンドで実行します：

```bash
./otelcol-contrib --config config-traces.yaml
```

トレースイベントをコレクターに送信するためには、`telemetrygen`を使用して以下のコマンドを実行します：

```bash
$GOBIN/telemetrygen traces --otlp-insecure --traces 300
```

これにより、以下に示すようなトレースメッセージがstdoutに出力されます：

```
Span #86
	Trace ID   	: 1bb5cdd2c9df5f0da320ca22045c60d9
	Parent ID  	: ce129e5c2dd51378
	ID         	: fbb14077b5e149a0
	Name       	: okey-dokey-0
	Kind       	: Server
	Start time 	: 2024-06-19 18:03:41.603868 +0000 UTC
	End time   	: 2024-06-19 18:03:41.603991 +0000 UTC
	Status code	: Unset
	Status message :
Attributes:
 	-> net.peer.ip: Str(1.2.3.4)
 	-> peer.service: Str(telemetrygen-client)
```

上記は、OTelコレクターによって生成された単一のトレースメッセージを表しています。これら同じメッセージを後のセクションでClickHouseに投入します。

トレースメッセージの完全なスキーマは[こちら](https://opentelemetry.io/docs/concepts/signals/traces/)で維持されています。ユーザーはこのスキーマに慣れることを強くお勧めします。

## 処理 - フィルタリング、変換、充実化

以前の例で示したログイベントのタイムスタンプを設定するように、ユーザーはイベントメッセージをフィルタリング、変換、充実化したいと考えることがよくあります。これは、Open Telemetryでのいくつかの能力を使用して達成できます：

- **プロセッサー** - プロセッサーは、[レシーバーが収集したデータを変更または変換](https://opentelemetry.io/docs/collector/transforming-telemetry/)し、エクスポーターに送信する前にそのデータを処理します。[推奨されるプロセッサー](https://github.com/open-telemetry/opentelemetry-collector/tree/main/processor#recommended-processors) の最小セットは通常推奨されますが、これらは任意です。OTeLコレクターをClickHouseと共に使用する際には、以下をプロセッサーに制限することをお勧めします：

    - [memory_limiter](https://github.com/open-telemetry/opentelemetry-collector/blob/main/processor/memorylimiterprocessor/README.md) は、コレクターでのメモリ不足を防ぐために使用されます。「リソースの見積もり」を参照してください。
    - コンテキストに基づいて強化を行うプロセッサー。たとえば、[Kubernetes Attributes Processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/k8sattributesprocessor) は、k8sメタデータで指標やログのリソース属性を自動設定して、イベントをそれらのソースポッドIDで強化することを可能にします。
    - トレースが必要な場合の[末尾もしくは先端のサンプリング](https://opentelemetry.io/docs/concepts/sampling/)。
    - [基本的なフィルタリング](https://opentelemetry.io/docs/collector/transforming-telemetry/) - オペレーターでできない場合の削除イベント。
    - [バッチ](https://github.com/open-telemetry/opentelemetry-collector/tree/main/processor/batchprocessor) - データがバッチで送信されることを保証するためにClickHouseで作業する際に必須です。ClickHouseへのエクスポートを参照してください。

- **オペレーター** - [オペレーター](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/stanza/docs/operators/README.md)は、レシーバーで利用可能な処理の最も基本的な単位を提供します。基本的な解析がサポートされており、SeverityとTimestampなどのフィールドを設定することが可能です。JSONと正規表現の解析がここでサポートされており、そのフィルタリングと基本的な変換が可能です。ここでのイベントフィルタリングをお勧めします。 

オペレーターまたは[トランスフォームプロセッサー](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/processor/transformprocessor/README.md)を使用して過剰なイベント処理を行わないことをお勧めします。これらは相当なメモリおよびCPUオーバーヘッドを引き起こす可能性があります、特にJSON解析。しかし、挿入時にClickHouseでマテリアライズドビューとカラムを使ってすべての処理を行うことも可能です。ただし、コンテキスト認識が必要な強化（例：k8sメタデータの追加）は除きます。「SQLでの構造の抽出」を参照してください。

もし、OTelコレクターを使用して処理をする場合、ゲートウェイインスタンスでの変換を推奨し、エージェントインスタンスで行う作業を最小限にすることをお勧めします。これにより、サーバー上で動作するエージェントが必要とするリソースが可能な限り最小化されます。通常、ユーザーがフィルタリング（不要なネットワーク使用を最小化するため）、タイムスタンプ設定（オペレーターを通して）、コンテキストが必要な強化をエージェントで行うのを見ます。たとえば、ゲートウェイインスタンスが異なるKubernetesクラスターに存在する場合、k8sの強化はエージェントで発生します。

### 例

以下の設定は、非構造化ログファイルの収集を示します。ログ行から構造を抽出するためのオペレーター（`regex_parser`）とイベントをフィルタリングするための使用、及びイベントをバッチ処理しメモリ使用量を制限するためのプロセッサーを利用しています。

[config-unstructured-logs-with-processor.yaml](https://www.otelbin.io/#config=receivers%3A*N_filelog%3A*N___include%3A*N_____-_%2Fopt%2Fdata%2Flogs%2Faccess-unstructured.log*N___start*_at%3A_beginning*N___operators%3A*N_____-_type%3A_regex*_parser*N_______regex%3A_*%22%5E*C*QP*Lip*G%5B*Bd.%5D*P*D*Bs*P-*Bs*P-*Bs*P*B%5B*C*QP*Ltimestamp*G%5B%5E*B%5D%5D*P*D*B%5D%5C*%22*C*QP*Lmethod*G%5BA-Z%5D*P*D*Bs*P*C*QP*Lurl*G%5B%5E*Bs%5D*P*D*Bs*PHTTP%2F%5B%5E*Bs%5D*P%22*Bs*P*C*QP*Lstatus*G*Bd*P*D*Bs*P*C*QP*Lsize*G*Bd*P*D*Bs*P%22*C*QP*Lreferrer*G%5B%5E%22%5D***D%22*Bs*P%22*C*QP*Luser*_agent*G%5B%5E%22%5D***D%22*%22*N_______timestamp%3A*N_________parse*_from%3A_attributes.timestamp*N_________layout%3A_*%22*.d%2F*.b%2F*.Y%3A*.H%3A*.M%3A*.S_*.z*%22*N_________*H22%2FJan%2F2019%3A03%3A56%3A14_*P0330*N*N*Nprocessors%3A*N_batch%3A*N___timeout%3A_1s*N___send*_batch*_size%3A_100*N_memory*_limiter%3A*N___check*_interval%3A_1s*N___limit*_mib%3A_2048*N___spike*_limit*_mib%3A_256*N*N*Nexporters%3A*N_logging%3A*N___loglevel%3A_debug*N*N*Nservice%3A*N_pipelines%3A*N___logs%3A*N_____receivers%3A_%5Bfilelog%5D*N_____processors%3A_%5Bbatch%2C_memory*_limiter%5D*N_____exporters%3A_%5Blogging%5D%7E)

```yaml
receivers:
  filelog:
    include:
      - /opt/data/logs/access-unstructured.log
    start_at: beginning
    operators:
      - type: regex_parser
        regex: '^(?P<ip>[\d.]+)\s+-\s+-\s+\[(?P<timestamp>[^\]]+)\]\s+"(?P<method>[A-Z]+)\s+(?P<url>[^\s]+)\s+HTTP/[^\s]+"\s+(?P<status>\d+)\s+(?P<size>\d+)\s+"(?P<referrer>[^"]*)"\s+"(?P<user_agent>[^"]*)"'
        timestamp:
          parse_from: attributes.timestamp
          layout: '%d/%b/%Y:%H:%M:%S %z'
          #22/Jan/2019:03:56:14 +0330
processors:
  batch:
    timeout: 1s
    send_batch_size: 100
  memory_limiter:
    check_interval: 1s
    limit_mib: 2048
    spike_limit_mib: 256
exporters:
  logging:
    loglevel: debug
service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch, memory_limiter]
      exporters: [logging]
```

```bash
./otelcol-contrib --config config-unstructured-logs-with-processor.yaml
```

## ClickHouseへのエクスポート

エクスポーターはデータを1つ以上のバックエンドまたは目的地に送信します。エクスポーターはプルまたはプッシュベースです。ClickHouseにイベントを送信するためには、プッシュベースの[ClickHouseエクスポーター](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md)を使用する必要があります。

> ClickHouseエクスポーターはコアディストリビューションではなく、[OpenTelemetry Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main)の一部です。ユーザーはContribディストリビューションを使用するか、[独自のコレクターを構築](https://opentelemetry.io/docs/collector/custom-collector/)することができます。

完全な構成ファイルは以下に示されています。

[clickhouse-config.yaml](https://www.otelbin.io/#config=receivers%3A*N_filelog%3A*N___include%3A*N_____-_%2Fopt%2Fdata%2Flogs%2Faccess-structured.log*N___start*_at%3A_beginning*N___operators%3A*N_____-_type%3A_json*_parser*N_______timestamp%3A*N_________parse*_from%3A_attributes.time*_local*N_________layout%3A_*%22*.Y-*.m-*.d_*.H%3A*.M%3A*.S*%22*N_otlp%3A*N____protocols%3A*N______grpc%3A*N________endpoint%3A_0.0.0.0%3A4317*N*Nprocessors%3A*N_batch%3A*N___timeout%3A_5s*N___send*_batch*_size%3A_5000*N*Nexporters%3A*N_clickhouse%3A*N___endpoint%3A_tcp%3A%2F%2Flocalhost%3A9000*Qdial*_timeout*E10s*Acompress*Elz4*Aasync*_insert*E1*N___*H_ttl%3A_72h*N___traces*_table*_name%3A_otel*_traces*N___logs*_table*_name%3A_otel*_logs*N___create*_schema%3A_true*N___timeout%3A_5s*N___database%3A_default*N___sending*_queue%3A*N_____queue*_size%3A_1000*N___retry*_on*_failure%3A*N_____enabled%3A_true*N_____initial*_interval%3A_5s*N_____max*_interval%3A_30s*N_____max*_elapsed*_time%3A_300s*N*Nservice%3A*N_pipelines%3A*N___logs%3A*N_____receivers%3A_%5Bfilelog%5D*N_____processors%3A_%5Bbatch%5D*N_____exporters%3A_%5Bclickhouse%5D*N___traces%3A*N____receivers%3A_%5Botlp%5D*N____processors%3A_%5Bbatch%5D*N____exporters%3A_%5Bclickhouse%5D%7E&distro=otelcol-contrib%7E&distroVersion=v0.103.1%7E)

```yaml
receivers:
  filelog:
    include:
      - /opt/data/logs/access-structured.log
    start_at: beginning
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.time_local
          layout: '%Y-%m-%d %H:%M:%S'
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
processors:
  batch:
    timeout: 5s
    send_batch_size: 5000
exporters:
  clickhouse:
    endpoint: tcp://localhost:9000?dial_timeout=10s&compress=lz4&async_insert=1
    # ttl: 72h
    traces_table_name: otel_traces
    logs_table_name: otel_logs
    create_schema: true
    timeout: 5s
    database: default
    sending_queue:
      queue_size: 1000
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s


service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [clickhouse]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [clickhouse]
```

次に、重要な設定を示しています：

- **パイプライン** - 上記の設定では、レシーバー、プロセッサー、エクスポーターのセットとして[パイプライン](https://opentelemetry.io/docs/collector/configuration/#pipelines)の使用がハイライトされています。ログとトレースのためのものです。
- **エンドポイント** - ClickHouseとの通信は`endpoint`パラメータで設定されています。接続文字列`tcp://localhost:9000?dial_timeout=10s&compress=lz4&async_insert=1`により、通信はTCP上で行われます。ユーザーがトラフィックスイッチングの理由でHTTPを好む場合は、[ここ](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md#configuration-options)で記載されているようにこの接続文字列を変更してください。完全な接続詳細、ユーザー名とパスワードをこの接続文字列に指定する能力を持つものは、[ここ](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md#configuration-options)で詳しく説明されています。

**重要:** 上記の接続文字列は、圧縮（lz4）と非同期挿入の両方を有効にしています。私たちは、常に両方を有効にすることをお勧めします。非同期挿入に関する詳細は「バッチ処理」を参照してください。圧縮は常に指定されなければならず、以前のエクスポーターの古いバージョンではデフォルトで有効にされていません。

- **ttl** - ここでの値はデータが保持される期間を決定します。より詳しい情報は「データの管理」にあります。これは時間単位で時間を指定する必要があります。例：72h。以下の例では無効化しています。データが2019年のもので、挿入後すぐにClickHouseによって削除されるためです。
- **traces_table_name** と **logs_table_name** - ログとトレーステーブルの名前を決定します。
- **create_schema** - デフォルトのスキーマでテーブルを起動時に作成するかどうかを決定します。最初は真に設定することを推奨します。ユーザーは、この値を偽に設定し、独自のスキーマを定義するべきです。
- **database** - 対象データベース。
- **retry_on_failure** - 失敗したバッチを再試行するかどうかを決定する設定。
- **batch** - バッチプロセッサーがイベントをバッチとして送信することを保証します。5000ほどの値を推奨し、タイムアウトを5sとしています。これらのいずれかが最初に達した場合、バッチがフラッシュされてエクスポーターに送信されます。これらの値を下げることで、より速いレイテンシーパイプラインになり、データが即座にクエリ可能になる一方で、より多くの接続とバッチがClickHouseに送信されることになります。非同期挿入を使用していない場合は問題を引き起こす可能性があるため、これを推奨しません（ClickHouseでの[過多パーツ](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse#1-too-many-parts)の問題）。逆に、ユーザーが非同期挿入を使用している場合は、これらの設定でデータが依然としてフラッシュされます。詳細については「バッチ処理」を参照してください。
- **sending_queue** - 送信キューのサイズを制御します。キューにはバッチが含まれており、このキューが超過すると、バッチがドロップされます。詳細については「バックプレッシャーの処理」を参照してください。

構造化ログファイルを抽出し、認証されていない[ローカルインスタンスのClickHouse](https://clickhouse.com/docs/ja/install)が実行中であると仮定すると、以下のコマンドを使用してこの設定を実行できます：

```bash
./otelcol-contrib --config clickhouse-config.yaml
```

このコレクターにトレースデータを送信するためには、`telemetrygen`ツールを使用しての以下のコマンドを実行します：

```bash
$GOBIN/telemetrygen traces --otlp-insecure --traces 300
```

実行中の際には、簡単なクエリでログイベントが存在することを確認してください：

```sql
SELECT *
FROM otel_logs
LIMIT 1
FORMAT Vertical

Row 1:
──────
Timestamp:      	2019-01-22 06:46:14.000000000
TraceId:
SpanId:
TraceFlags:     	0
SeverityText:
SeverityNumber: 	0
ServiceName:
Body:           	{"remote_addr":"109.230.70.66","remote_user":"-","run_time":"0","time_local":"2019-01-22 06:46:14.000","request_type":"GET","request_path":"\/image\/61884\/productModel\/150x150","request_protocol":"HTTP\/1.1","status":"200","size":"1684","referer":"https:\/\/www.zanbil.ir\/filter\/p3%2Cb2","user_agent":"Mozilla\/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko\/20100101 Firefox\/64.0"}
ResourceSchemaUrl:
ResourceAttributes: {}
ScopeSchemaUrl:
ScopeName:
ScopeVersion:
ScopeAttributes:	{}
LogAttributes:  	{'referer':'https://www.zanbil.ir/filter/p3%2Cb2','log.file.name':'access-structured.log','run_time':'0','remote_user':'-','request_protocol':'HTTP/1.1','size':'1684','user_agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0','remote_addr':'109.230.70.66','request_path':'/image/61884/productModel/150x150','status':'200','time_local':'2019-01-22 06:46:14.000','request_type':'GET'}

1 row in set. Elapsed: 0.012 sec. Processed 5.04 thousand rows, 4.62 MB (414.14 thousand rows/s., 379.48 MB/s.)
Peak memory usage: 5.41 MiB.


同様にトレースイベントの場合、ユーザーは`otel_traces`テーブルを確認できます：

SELECT *
FROM otel_traces
LIMIT 1
FORMAT Vertical

Row 1:
──────
Timestamp:      	2024-06-20 11:36:41.181398000
TraceId:        	00bba81fbd38a242ebb0c81a8ab85d8f
SpanId:         	beef91a2c8685ace
ParentSpanId:
TraceState:
SpanName:       	lets-go
SpanKind:       	SPAN_KIND_CLIENT
ServiceName:    	telemetrygen
ResourceAttributes: {'service.name':'telemetrygen'}
ScopeName:      	telemetrygen
ScopeVersion:
SpanAttributes: 	{'peer.service':'telemetrygen-server','net.peer.ip':'1.2.3.4'}
Duration:       	123000
StatusCode:     	STATUS_CODE_UNSET
StatusMessage:
Events.Timestamp:   []
Events.Name:    	[]
Events.Attributes:  []
Links.TraceId:  	[]
Links.SpanId:   	[]
Links.TraceState:   []
Links.Attributes:   []
```

## すぐに使えるスキーマ

デフォルトでは、ClickHouseエクスポーターはログとトレースのためのターゲットログテーブルを作成します。これは`create_schema`の設定を通じて無効にすることができます。さらに、`otel_logs`および`otel_traces`のデフォルトからログとトレーステーブルの名前を変更することが上記で設定されました。

> 以下のスキームは、TTLが72時間として設定された場合を想定しています。

ログのデフォルトスキーマは以下の通りです（`otelcol-contrib v0.102.1`）：

```sql
CREATE TABLE default.otel_logs
(
	`Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	`TraceId` String CODEC(ZSTD(1)),
	`SpanId` String CODEC(ZSTD(1)),
	`TraceFlags` UInt32 CODEC(ZSTD(1)),
	`SeverityText` LowCardinality(String) CODEC(ZSTD(1)),
	`SeverityNumber` Int32 CODEC(ZSTD(1)),
	`ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
	`Body` String CODEC(ZSTD(1)),
	`ResourceSchemaUrl` String CODEC(ZSTD(1)),
	`ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	`ScopeSchemaUrl` String CODEC(ZSTD(1)),
	`ScopeName` String CODEC(ZSTD(1)),
	`ScopeVersion` String CODEC(ZSTD(1)),
	`ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	`LogAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, toUnixTimestamp(Timestamp), TraceId)
TTL toDateTime(Timestamp) + toIntervalDay(3)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
```

このカラムは、公式仕様に基づくOTelのログドキュメンテーション[こちら](https://opentelemetry.io/docs/specs/otel/logs/data-model/)に基づくものです。

このスキーマの重要なポイント：

- デフォルトでテーブルは`PARTITION BY toDate(Timestamp)`によって日付でパーティションされています。これにより有過ぎたデータの削除が効率的に行われます。
- TTL が `TTL toDateTime(Timestamp) + toIntervalDay(3)` を通じて設定されており、コレクター構成で設定されている値に対応しています。[`ttl_only_drop_parts=1`](/ja/operations/settings/settings#ttl_only_drop_parts) とは、保存されたすべての行が期限切れになったときにパーツ全体が削除されることを意味します。クリックハウスにおける、この特性はこ高価な削除を避けることが可能にします。これを常に設定することをお勧めします。詳細については「TTLによるデータ管理」を参照してください。
- テーブルは古典的な[`MergeTree`エンジン](/ja/engines/table-engines/mergetree-family/mergetree)を使用しています。これはログやトレースに推奨されており、変更する必要はほとんどありません。
- テーブルは`ORDER BY (ServiceName, SeverityText, toUnixTimestamp(Timestamp), TraceId)` によって整理されています。これにより、クエリはServiceName、SeverityText、Timestamp、TraceIdに対するフィルターに最適化されます - リストの先頭のカラムは後のカラムよりも速くフィルタリングされます。ユーザーは期待されるアクセスパターンに応じてこの順序を変更する必要があります。詳細については「主キーの選択」を参照してください。
- 上記のスキーマはカラムに`ZSTD(1)`を適用しています。これはログに最適な圧縮を提供します。ユーザーはより良い圧縮を目指してZSTDの圧縮レベル（上記の1）を増やすことができますが、通常あまり利益を得ません。この値を増やすことは、挿入時のCPUオーバーヘッド（圧縮中）を増加させますが、解凍（クエリ）は同等の性能を持ち続けると予想されます。さらなる詳細は[こちら](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)を参照してください。追加の[デルタエンコーディング](https://clickhouse.com/docs/ja/sql-reference/statements/create/table#delta)がTimestampに対して適用されており、そのディスクサイズを縮小することを目的としています。
- `ResourceAttributes`や`LogAttributes`、`ScopeAttributes`がマップであることに注意してください。ユーザーはこれらの違いに慣れる必要があります。 このマップへのアクセス方法やキーへの最適化については「マップを使う」を参照してください。
- その他のタイプも`ServiceName`など最適化されています。Bodyカラムは、JSONであるにもかかわらず、文字列として保存されます。
- ブルームフィルターはマップキー、値、およびBodyカラムに適用されます。これらはこれらのカラムのクエリ時間を改善することを意図しており、通常は不要です。 "二次/データスキップインデックス"を参照してください。

```sql
CREATE TABLE default.otel_traces
(
	`Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	`TraceId` String CODEC(ZSTD(1)),
	`SpanId` String CODEC(ZSTD(1)),
	`ParentSpanId` String CODEC(ZSTD(1)),
	`TraceState` String CODEC(ZSTD(1)),
	`SpanName` LowCardinality(String) CODEC(ZSTD(1)),
	`SpanKind` LowCardinality(String) CODEC(ZSTD(1)),
	`ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
	`ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	`ScopeName` String CODEC(ZSTD(1)),
	`ScopeVersion` String CODEC(ZSTD(1)),
	`SpanAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	`Duration` Int64 CODEC(ZSTD(1)),
	`StatusCode` LowCardinality(String) CODEC(ZSTD(1)),
	`StatusMessage` String CODEC(ZSTD(1)),
	`Events.Timestamp` Array(DateTime64(9)) CODEC(ZSTD(1)),
	`Events.Name` Array(LowCardinality(String)) CODEC(ZSTD(1)),
	`Events.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
	`Links.TraceId` Array(String) CODEC(ZSTD(1)),
	`Links.SpanId` Array(String) CODEC(ZSTD(1)),
	`Links.TraceState` Array(String) CODEC(ZSTD(1)),
	`Links.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_duration Duration TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toUnixTimestamp(Timestamp), TraceId)
TTL toDateTime(Timestamp) + toIntervalDay(3)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
```

再度、これは公式仕様に基づくOTelのトレースドキュメンテーションは[こちら](https://opentelemetry.io/docs/specs/otel/trace/api/)で記載されています。 こちらのスキーマは、ログスキーマの多くの同じ設定を使用しており、スパン専用の追加リンクカラムが含まれています。

ユーザーは自動スキーマの作成を無効にし、手動でテーブルを作成することを推奨します。これにより、主要および二次キーの変更、クエリパフォーマンス最適化のための追加のカラムの導入が可能になります。 詳細は「スキーマ設計」を参照してください。

## 挿入の最適化

高い挿入性能を達成し、一貫性の強い保証を得るために、コレクターを通じた観測可能なデータをClickHouseに挿入する際にはシンプルなルールに準拠してください。OTelコレクターの正しい設定により、このルールに従うことは簡単でしょう。これにより、ユーザーがClickHouseを最初に使用する際のよくある[問題](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse)を避けることができます。

### バッチ処理

デフォルトでは、ClickHouseに送信された各挿入はClickHouseが直ちにそのデータと他の保存する必要のあるメタデータを含むパーツをストレージに作成することを意味します。したがって各挿入がより多くのデータを含み、小規模な挿入をより多く送信する場合に最適化されます。1000以上の行を含むかなり大きなバッチでデータを挿入することをお勧めします。詳細は[こちら](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse#data-needs-to-be-batched-for-optimal-performance)にあります。

デフォルトで、ClickHouseへの挿入は同期的であり、同一の場合は冪等です。マージツリーエンジンファミリーのテーブルでは、ClickHouseはデフォルトで自動的に[挿入時に重複を排除](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse#5-deduplication-at-insert-time)されます。これにより、挿入が以下のような場合に許容されます：

1. データを受信するノードに問題がある場合、INSERTクエリがタイムアウト（またはより具体的なエラーを返す）し、確認が得られません。
2. ノードによってデータが書き込まれましたが、ネットワークの中断が原因で送信側に確認を返すことができない場合、送信側はタイムアウトまたはネットワークエラーを体験します。

コレクターの観点からは、(1)と(2)は区別が難しい場合があります。しかし、どちらの場合も確認されていない挿入はただちに再試行できます。元の挿入が成功した場合、新しく試行された挿入が無視されます。

ユーザーが上記のバッチプロセッサーを使用して、挿入を一貫性のある行のバッチとして送信することをお勧めします。これにより、バッチプロセッサーの`timeout`が達成される前にバッチがフラッシュされることになり、パイプラインのエンドツーエンドのレイテンシーが低くなり、バッチが一貫サイズになります。

### 非同期挿入の使用

コレクターのスループットが低く、データがClickHouseに到達する最低限のエンドツーエンドの待ち時間を期待する場合、大きなバッチを送信する場合があります。この場合、バッチプロセッサーの`timeout`が期限切れになるときに小さなバッチが送信されます。これにより問題が発生し、非同期挿入が必要となります。この場合は特に**エージェント役としてClickHouseに直接送信するようにコレクターが構成されている場合**に発生します。ゲートウェイは、集約器として機能することにより、この問題を軽減します - 「ゲートウェイを使用したスケーリング」を参照してください。

大きなバッチを保証できない場合は、ClickHouseを使用して非同期挿入を[非同期挿入](/ja/cloud/bestpractices/asynchronous-inserts)使用してバッチ処理を委任できます。データはまずバッファに挿入され、その後データベースストレージに書き込まれます。

<img src={require('./images/observability-6.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

[非同期挿入を有効にすると](/ja/optimize/asynchronous-inserts#enabling-asynchronous-inserts)、ClickHouseが①Insertクエリを受け取り、そのクエリのデータが②直ちにメモリ内バッファに書き込まれます。 ③次のバッファフラッシュが行われるとき、バッファ内のデータは[ソートされ](/ja/optimize/sparse-primary-indexes#data-is-stored-on-disk-ordered-by-primary-key-columns)、パートとしてデータベースストレージに書き込まれます。注意点として、データがストレージにフラッシュされる前にはクエリで検索することはできません。バッファフラッシュは[設定可能](/ja/optimize/asynchronous-inserts)です。

コレクターに対する非同期挿入を有効にするには、`async_insert=1`を接続文字列に追加します。配送保証を得るために`wait_for_async_insert=1`を使用することをお勧めします。さらに詳細は[こちら](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse)を参照してください。

非同期挿入からのデータは、ClickHouseバッファがフラッシュされると挿入されます。挿入は`async_insert_max_data_size`を超過したとき、または最初のINSERTクエリから`async_insert_busy_timeout_ms`ミリ秒が経過した後に行われます。`async_insert_stale_timeout_ms`がゼロでない値に設定されている場合、データは`async_insert_stale_timeout_ms`ミリ秒が最終クエリから経過した後に挿入されます。ユーザーはこれらの設定を調整して、パイプラインのエンドツーエンドのレイテンシーを制御できます。バッファフラッシュを最適化するために追加の設定は[こちら](/ja/operations/settings/settings#asynchronous-insert-settings)に記載されています。一般に、デフォルトが適切です。

> エージェントの数が少なく、スループットが低いが厳格なエンドツーエンドのレイテンシーが求められる場合、[適応非同期挿入](https://clickhouse.com/blog/clickhouse-release-24-02#adaptive-asynchronous-inserts)が有用である可能性があります。一般的に、これらはClickHouseで見られる観測性の高スループットユースケースには適合しません。

最後に、非同期挿入を使用する際、ClickHouseへの同期挿入に関連する以前の重複排除の動作はデフォルトでは有効になっていません。必要に応じて、設定[`async_insert_deduplicate`](/ja/operations/settings/settings#async-insert-deduplicate)を参照してください。

この機能の詳細な設定については[こちら](/ja/optimize/asynchronous-inserts#enabling-asynchronous-inserts)で詳しく説明されており、深掘りについては[こちら](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse)があります。

## デプロイメントアーキテクチャ

OTelコレクターをClickhouseで使用する際のいくつかのデプロイメントアーキテクチャが可能です。それぞれの概要と適用可能な場合を以下で説明します。

### エージェントのみ

エージェントのみのアーキテクチャでは、OTelコレクターをエッジにエージェントとしてデプロイします。これらはローカルアプリケーション（例：サイドカーコンテナ）からトレースを受信し、サーバーまたはKubernetesノードからログを収集します。このモードでは、エージェントがデータをClickHouseに直接送信します。

<img src={require('./images/observability-7.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '600px'}} />

<br />

このアーキテクチャは、中小規模のデプロイメントに適しています。その主な利点は、追加のハードウェアを必要とせず、ClickHouse観測可能性ソリューションの全体的なリソースフットプリントを最小限に抑え、アプリケーションとコレクターとの間にシンプルなマッピングを持つことです。

エージェントの数が数百を超える場合は、ゲートウェイベースのアーキテクチャへの移行を検討する必要があります。このアーキテクチャにはいくつかの欠点があり、スケールの問題を抱えています：

- **接続スケーリング** - 各エージェントはClickHouseに接続を確立します。ClickHouseは数百（場合によっては数千）の同時挿入接続を維持可能ですが、最終的にはそれが制限要因となり、挿入が非効率になります。つまり、ClickHouseが接続を維持するためにより多くのリソースを消費することになります。ゲートウェイを使用することで接続数を最小限に抑え、挿入の効率を上げます。
- **エッジでの処理** - このアーキテクチャでは、変換やイベント処理がエッジまたはClickHouseで行われる必要があります。この制限は、複雑なClickHouseのMaterialized Viewを意味するか、重要なサービスに影響を与え、リソースが乏しいエッジに大量の計算を押し付けることを意味します。
- **小規模なバッチとレイテンシ** - エージェントコレクターはそれぞれ非常に少ないイベントを収集することがあります。これにより、配信SLAを満たすために一定の間隔でフラッシュするように構成する必要があります。これにより、コレクターがClickHouseに小規模なバッチを送信することがあります。これは不利ですが、「挿入の最適化」を参照して非同期挿入で軽減できます。

### ゲートウェイを用いたスケーリング

OTelコレクターはゲートウェイインスタンスとしてデプロイされ、上記の制限に対処します。これらは単独のサービスを提供し、通常はデータセンターや地域ごとに展開されます。これらは単一のOTLPエンドポイント経由でアプリケーション（またはエージェントロール内の他のコレクター）からイベントを受信します。通常、ゲートウェイインスタンスのセットがデプロイされ、ロードバランサーを使用してそれらの負荷を分散します。

<img src={require('./images/observability-8.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

このアーキテクチャの目的は、エージェントから計算に集中的な処理をオフロードし、それによってリソースの使用を最小限に抑えることです。これらのゲートウェイは、エージェントが行う必要がある変換タスクを実行できます。さらに、多くのエージェントからのイベントを集約することで、ゲートウェイはClickHouseに大規模なバッチを送信でき、効率的な挿入が可能です。エージェントが追加され、イベントのスループットが増加するにつれて、これらのゲートウェイコレクターは簡単にスケールできます。以下に、例としてのゲートウェイ設定と、それに関連するエージェント設定が示されています。エージェントとゲートウェイ間の通信にはOTLPが使用されています。

[clickhouse-agent-config.yaml](https://www.otelbin.io/#config=receivers%3A*N_filelog%3A*N___include%3A*N_____-_%2Fopt%2Fdata%2Flogs%2Faccess-structured.log*N___start*_at%3A_beginning*N___operators%3A*N_____-_type%3A_json*_parser*N_______timestamp%3A*N_________parse*_from%3A_attributes.time*_local*N_________layout%3A_*%22*.Y-*.m-*.d_*.H%3A*.M%3A*.S*%22*N*Nprocessors%3A*N_batch%3A*N___timeout%3A_5s*N___send*_batch*_size%3A_1000*N*Nexporters%3A*N_otlp%3A*N___endpoint%3A_localhost%3A4317*N___tls%3A*N_____insecure%3A_true_*H_Set_to_false_if_you_are_using_a_secure_connection*N*Nservice%3A*N_telemetry%3A*N___metrics%3A*N_____address%3A_0.0.0.0%3A9888_*H_Modified_as_2_collectors_running_on_same_host*N_pipelines%3A*N___logs%3A*N_____receivers%3A_%5Bfilelog%5D*N_____processors%3A_%5Bbatch%5D*N_____exporters%3A_%5Botlp%5D%7E&distro=otelcol-contrib%7E&distroVersion=v0.103.1%7E)

```yaml
receivers:
  filelog:
    include:
      - /opt/data/logs/access-structured.log
    start_at: beginning
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.time_local
          layout: '%Y-%m-%d %H:%M:%S'
processors:
  batch:
    timeout: 5s
    send_batch_size: 1000
exporters:
  otlp:
    endpoint: localhost:4317
    tls:
      insecure: true # セキュアな接続を使用している場合はfalseに設定してください
service:
  telemetry:
    metrics:
      address: 0.0.0.0:9888 # 同一ホストで2つのコレクターが実行されているために修正
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [otlp]
```

[clickhouse-gateway-config.yaml](https://www.otelbin.io/#config=receivers%3A*N__otlp%3A*N____protocols%3A*N____grpc%3A*N____endpoint%3A_0.0.0.0%3A4317*N*Nprocessors%3A*N__batch%3A*N____timeout%3A_5s*N____send*_batch*_size%3A_10000*N*Nexporters%3A*N__clickhouse%3A*N____endpoint%3A_tcp%3A%2F%2Flocalhost%3A9000*Qdial*_timeout*E10s*Acompress*Elz4*N____ttl%3A_96h*N____traces*_table*_name%3A_otel*_traces*N____logs*_table*_name%3A_otel*_logs*N____create*_schema%3A_true*N____timeout%3A_10s*N____database%3A_default*N____sending*_queue%3A*N____queue*_size%3A_10000*N____retry*_on*_failure%3A*N____enabled%3A_true*N____initial*_interval%3A_5s*N____max*_interval%3A_30s*N____max*_elapsed*_time%3A_300s*N*Nservice%3A*N__pipelines%3A*N____logs%3A*N______receivers%3A_%5Botlp%5D*N______processors%3A_%5Bbatch%5D*N______exporters%3A_%5Bclickhouse%5D%7E&distro=otelcol-contrib%7E&distroVersion=v0.103.1%7E)

```yaml
receivers:
  otlp:
    protocols:
    grpc:
    endpoint: 0.0.0.0:4317
processors:
  batch:
    timeout: 5s
    send_batch_size: 10000
exporters:
  clickhouse:
    endpoint: tcp://localhost:9000?dial_timeout=10s&compress=lz4
    ttl: 96h
    traces_table_name: otel_traces
    logs_table_name: otel_logs
    create_schema: true
    timeout: 10s
    database: default
    sending_queue:
      queue_size: 10000
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [clickhouse]
```

これらの設定は以下のコマンドで実行できます。

```bash
./otelcol-contrib --config clickhouse-gateway-config.yaml
./otelcol-contrib --config clickhouse-agent-config.yaml
```

このアーキテクチャの主な欠点は、一連のコレクターを管理するための関連コストとオーバーヘッドです。

関連する学習と共に、より大規模なゲートウェイベースのアーキテクチャを管理する例については、[このブログ記事](https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog)をお勧めします。

### Kafkaの追加

読者は、このアーキテクチャがメッセージキューとしてKafkaを使用していないことに気付くかもしれません。

Kafkaキューをメッセージバッファーとして使用することは、ログアーキテクチャで一般的なデザインパターンであり、ELKスタックによって普及しました。これにはいくつかの利点があります。主に、より強力なメッセージ配信保証を提供し、バックプレッシャーに対処するのに役立ちます。メッセージは収集エージェントからKafkaに送信され、ディスクに書き込まれます。理論上、クラスター化されたKafkaインスタンスは高スループットのメッセージバッファーを提供するはずです。なぜなら、データをディスクに直線的に書き込む計算オーバーヘッドが少ないからです。Elasticの場合、例えばトークン化とインデックス作成にはかなりのオーバーヘッドがかかります。エージェントからデータを移動することで、ソースでのログローテーションによってメッセージが失われるリスクを減らします。最後に、一部のユースケースで魅力的なメッセージリプレイおよびクロスリージョンレプリケーション機能を提供します。

しかしながら、ClickHouseは非常に迅速にデータを挿入することができ、通常のハードウェアで毎秒何百万行も処理できます。ClickHouseによるバックプレッシャーは**稀です**。Kafkaキューを利用することは、しばしばより多くのアーキテクチャの複雑さとコストを招きます。ログが銀行取引や他のミッションクリティカルなデータと同じ配信保証を必要としないという原則を受け入れられるなら、Kafkaの複雑さを避けることをお勧めします。

ただし、高い配信保証やデータをリプレイする機能（おそらく複数のソースに）を必要とする場合、Kafkaは有用なアーキテクチャ上の追加機能となることがあります。

<img src={require('./images/observability-9.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

この場合、OTelエージェントは[Kafkaエクスポーター](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md)を使用してデータをKafkaに送信するように設定できます。ゲートウェイインスタンスは、[Kafkaレシーバー](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/receiver/kafkareceiver/README.md)を使用してメッセージを消費します。詳細については、ConfluentおよびOTelのドキュメントをお勧めします。

### リソースの見積もり

OTelコレクターのリソース要件は、イベントのスループット、メッセージのサイズ、および行われる処理量に依存します。OpenTelemetryプロジェクトはユーザーがリソース要件を見積もるために使用できる[ベンチマーク](https://opentelemetry.io/docs/collector/benchmarks/)を提供しています。

[私たちの経験](https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog#architectural-overview)では、3コアと12GBのRAMを持つゲートウェイインスタンスは、約60,000イベント/秒を処理できます。これは、フィールドのリネームのみを担当する最小限の処理パイプラインと正規表現を持たないことを前提としています。

ゲートウェイにイベントを送信するエージェントインスタンスの場合、イベントのタイムスタンプのみを設定するだけなら、予想される秒あたりのログに基づいてサイズを決定することをお勧めします。以下は、ユーザーが始めるための参考値を示しています：

| ログレート | コレクターエージェントに必要なリソース |
|--------------|-------------------------------------|
| 1k/秒       | 0.2CPU, 0.2GiB                      |
| 5k/秒       | 0.5 CPU, 0.5GiB                     |
| 10k/秒      | 1 CPU, 1GiB                         |
