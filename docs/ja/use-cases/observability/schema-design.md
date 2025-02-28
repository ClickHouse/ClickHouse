---
title: スキーマ設計
description: 観測性のためのスキーマ設計
slug: /ja/observability/schema-design
keywords: [observability, logs, traces, metrics, OpenTelemetry, Grafana, otel]
---

# 観測性のためのスキーマ設計

ユーザーには、以下の理由から、ログおよびトレース用の独自のスキーマを常に作成することをお勧めします。

- **主キーの選択** - デフォルトのスキーマは特定のアクセスパターンに最適化された `ORDER BY` を使用しています。したがって、あなたのアクセスパターンがこれに一致する可能性は低いです。
- **構造の抽出** - ユーザーは、既存のカラムから新しいカラムを抽出したい場合があるかもしれません（例えば `Body` カラム）。これは、マテリアライズドカラム（および複雑なケースではマテリアライズドビュー）を使用して実現できます。これにはスキーマの変更が必要です。
- **マップの最適化** - デフォルトのスキーマは、属性の保存に Map 型を使用しています。これらのカラムは任意のメタデータの保存を可能にします。これは重要な機能ですが、イベントからのメタデータは通常前もって定義されていないため、ClickHouseのような強く型付けられたデータベースに保存できません。そのため、マップキーやその値へのアクセスは通常のカラムに比べて効率が良くありません。これを解決するために、スキーマを変更し、最も一般的にアクセスされるマップキーをトップレベルのカラムとして配置することができます—「SQLでの構造の抽出」を参照してください。これにはスキーマの変更が必要です。
- **マップキーアクセスの簡略化** - マップ内のキーにアクセスするにはより冗長な構文が必要です。ユーザーはエイリアスを使用することでこれを軽減できます。「エイリアスの使用」を参照してクエリを簡素化してください。
- **セカンダリインデックス** - デフォルトのスキーマは、Map へのアクセスの高速化やテキストクエリの加速のためにセカンダリインデックスを使用します。これらは通常不要で、追加のディスクスペースを消費しますが、使用することができます。ただし、必要であることをテストする必要があります。「セカンダリ / データスキップインデックス」を参照してください。
- **コーデックの使用** - ユーザーは、予期されるデータを理解し、圧縮を改善する証拠がある場合、カラムのコーデックをカスタマイズしたいと思うかもしれません。

_上記の各使用例について詳細に説明します。_

**重要**: ユーザーは最適な圧縮とクエリパフォーマンスを実現するためにスキーマを拡張および変更することが奨励されていますが、主要カラムのOTelスキーマ名付けに従うべきです。ClickHouseのGrafanaプラグインは、クエリビルドを支援するために、いくつかの基本的なOTelカラムの存在を前提としています（例：タイムスタンプやSeverityText）。ログおよびトレースに必要なカラムは、ここに文書化されています [[1]](https://grafana.com/developers/plugin-tools/tutorials/build-a-logs-data-source-plugin#logs-data-frame-format)[[2]](https://grafana.com/docs/grafana/latest/explore/logs-integration/) 及び[こちら](https://grafana.com/docs/grafana/latest/explore/trace-integration/#data-frame-structure)を参照してください。これらのカラム名を変更することができますが、プラグイン設定でデフォルトをオーバーライドしてください。

## SQLでの構造の抽出

構造化されたログまたは非構造化されたログを取り込む際、ユーザーはしばしば以下を実現する必要があります：

- **文字列ブロブからのカラムの抽出**。これをクエリする方が、クエリ時の文字列操作よりも速くなります。
- **マップからのキーの抽出**。デフォルトのスキーマは任意の属性を Map 型のカラムに格納します。この型はスキーマレスな機能を提供し、ユーザーがログやトレースを定義する際に属性のカラムを事前に定義する必要がないという利点があります—特にKubernetesからログを収集し、後でポッドラベルを保持することを保証したい場合、これはしばしば不可能です。マップのキーやその値へのアクセスは、普通のClickHouseカラムに比べて遅くなります。したがって、マップからキーを抽出してルートテーブルのカラムに配置することは、しばしば望ましいです。

以下のクエリを考えてみてください：

特定のURLパスに対して最も多くのPOSTリクエストを受け取るカウントを取りたいとします。JSONブロブは `Body` カラムにStringとして保存されます。また、ユーザーがコレクター内でjson_parserを有効にしている場合、LogAttributesカラムに `Map(String, String)` としても保存される可能性があります。

```sql
SELECT LogAttributes
FROM otel_logs
LIMIT 1
FORMAT Vertical

行 1:
──────
Body:       {"remote_addr":"54.36.149.41","remote_user":"-","run_time":"0","time_local":"2019-01-22 00:26:14.000","request_type":"GET","request_path":"\/filter\/27|13 ,27|  5 ,p53","request_protocol":"HTTP\/1.1","status":"200","size":"30577","referer":"-","user_agent":"Mozilla\/5.0 (compatible; AhrefsBot\/6.1; +http:\/\/ahrefs.com\/robot\/)"}
LogAttributes: {'status':'200','log.file.name':'access-structured.log','request_protocol':'HTTP/1.1','run_time':'0','time_local':'2019-01-22 00:26:14.000','size':'30577','user_agent':'Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)','referer':'-','remote_user':'-','request_type':'GET','request_path':'/filter/27|13 ,27|  5 ,p53','remote_addr':'54.36.149.41'}
```

LogAttributesが利用可能であると想定すると、サイトのどのURLパスが最も多くのPOSTリクエストを受け取っているかをカウントするためのクエリは次のとおりです：

```sql
SELECT path(LogAttributes['request_path']) AS path, count() AS c
FROM otel_logs
WHERE ((LogAttributes['request_type']) = 'POST')
GROUP BY path
ORDER BY c DESC
LIMIT 5

┌─path─────────────────────┬─────c─┐
│ /m/updateVariation    │ 12182 │
│ /site/productCard     │ 11080 │
│ /site/productPrice    │ 10876 │
│ /site/productModelImages │ 10866 │
│ /site/productAdditives │ 10866 │
└──────────────────────────┴───────┘

5 rows in set. Elapsed: 0.735 sec. Processed 10.36 million rows, 4.65 GB (14.10 million rows/s., 6.32 GB/s.)
Peak memory usage: 153.71 MiB.
```

ここでのマップ構文の使用に注意してください。たとえば `LogAttributes['request_path']`や、URLからクエリパラメータを削除するための [`path` 関数](/ja/sql-reference/functions/url-functions#path)です。

ユーザーがコレクター内でJSONパースを有効にしていない場合、`LogAttributes`は空になります。そのため、[JSON関数](/ja/sql-reference/functions/json-functions)を使用して文字列 `Body` からカラムを抽出する必要があります。

> 構造化されたログのJSONパースはClickHouseで行うことを一般的にお勧めします。ClickHouseが最速のJSONパース実装であると自信を持っています。ただし、ユーザーはログを他のソースに送信したい場合もあることを認識しています。それにこのロジックを SQL に配置したくない場合もあるでしょう。

```sql
SELECT path(JSONExtractString(Body, 'request_path')) AS path, count() AS c
FROM otel_logs
WHERE JSONExtractString(Body, 'request_type') = 'POST'
GROUP BY path
ORDER BY c DESC
LIMIT 5

┌─path─────────────────────┬─────c─┐
│ /m/updateVariation    │ 12182 │
│ /site/productCard     │ 11080 │
│ /site/productPrice    │ 10876 │
│ /site/productAdditives │ 10866 │
│ /site/productModelImages │ 10866 │
└──────────────────────────┴───────┘

5 rows in set. Elapsed: 0.668 sec. Processed 10.37 million rows, 5.13 GB (15.52 million rows/s., 7.68 GB/s.)
Peak memory usage: 172.30 MiB.
```

次に、非構造化されたログを考えてみましょう：

```sql
SELECT Body, LogAttributes
FROM otel_logs
LIMIT 1
FORMAT Vertical

行 1:
──────
Body:       151.233.185.144 - - [22/Jan/2019:19:08:54 +0330] "GET /image/105/brand HTTP/1.1" 200 2653 "https://www.zanbil.ir/filter/b43,p56" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36" "-"
LogAttributes: {'log.file.name':'access-unstructured.log'}
```

非構造化ログの類似のクエリには、[`extractAllGroupsVertical` 関数](/ja/sql-reference/functions/string-search-functions#extractallgroupsvertical)を介した正規表現の使用が必要です。

```sql
SELECT
    path((groups[1])[2]) AS path,
    count() AS c
FROM
(
    SELECT extractAllGroupsVertical(Body, '(\\w+)\\s([^\\s]+)\\sHTTP/\\d\\.\\d') AS groups
    FROM otel_logs
    WHERE ((groups[1])[1]) = 'POST'
)
GROUP BY path
ORDER BY c DESC
LIMIT 5

┌─path─────────────────────┬─────c─┐
│ /m/updateVariation    │ 12182 │
│ /site/productCard     │ 11080 │
│ /site/productPrice    │ 10876 │
│ /site/productModelImages │ 10866 │
│ /site/productAdditives │ 10866 │
└──────────────────────────┴───────┘

5 rows in set. Elapsed: 1.953 sec. Processed 10.37 million rows, 3.59 GB (5.31 million rows/s., 1.84 GB/s.)
```

非構造化ログのパースのためのクエリの複雑さとコストが増加しているのに注意してください（パフォーマンスの違いに注意）。したがって、ユーザーには、可能な限り構造化されたログを使用することを強くお勧めします。

> 上記のクエリは、正規表現Dictionaryを利用して最適化できます。「Dictionaryの使用」を参照して詳細を確認してください。

これらの両方の使用例は、上記のクエリロジックを挿入時に移動させることにより、ClickHouseで満たすことができます。以下にいくつかのアプローチを示し、どのアプローチが適切かを強調します。

> ユーザーはまた、ここに説明されているように OTel Collector のプロセッサやオペレーターを使用して処理を行うこともできます。ほとんどの場合、ユーザーは ClickHouse がコレクターのプロセッサよりもリソース効率が高く、高速であることを見つけるでしょう。SQL で全てのイベント処理を行うことの主な欠点は、ソリューションが ClickHouse に結びつくことです。たとえば、ユーザーは OTel コレクターから他の宛先（例：S3）に処理済みのログを送信したい場合もあります。

### マテリアライズドカラム

マテリアライズドカラムは、他のカラムから構造を抽出する最もシンプルな解決策を提供します。このようなカラムの値は常に挿入時に計算され、INSERTクエリで指定することはできません。

> マテリアライズドカラムは、挿入時に新しいカラムに値が抽出されるため、追加のストレージオーバーヘッドが発生します。

マテリアライズドカラムは、任意の ClickHouse 表現をサポートしており、[文字列の処理](/ja/sql-reference/functions/string-functions)（[正規表現や検索](/ja/sql-reference/functions/string-search-functions)を含む）や [URL](/ja/sql-reference/functions/url-functions)、[型変換](/ja/sql-reference/functions/type-conversion-functions)、[JSON からの値の抽出](/ja/sql-reference/functions/json-functions)、または [数学的操作](/ja/sql-reference/functions/math-functions)など、あらゆる分析関数を活用できます。

基本処理にはマテリアライズドカラムを推奨します。これらは特に、マップから値を抽出し、それをルートカラムに昇格させ、型変換を行うのに役立ちます。非常に基本的なスキーマやマテリアライズドビューと組み合わせて使用する場合によく役立ちます。以下は、コレクターによってJSONが `LogAttributes` カラムに抽出されたログのスキーマです：

```sql
CREATE TABLE otel_logs
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
    `RequestPage` String MATERIALIZED path(LogAttributes['request_path']),
    `RequestType` LowCardinality(String) MATERIALIZED LogAttributes['request_type'],
    `RefererDomain` String MATERIALIZED domain(LogAttributes['referer'])
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, toUnixTimestamp(Timestamp), TraceId)
```

JSON関数を使用してString `Body` から抽出するための同等のスキーマは[こちら](https://pastila.nl/?005cbb97/513b174a7d6114bf17ecc657428cf829#gqoOOiomEjIiG6zlWhE+Sg==)にあります。

私たちの3つのマテリアライズドビューのカラムは、リクエストページ、リクエストタイプ、リファラーのドメインを抽出します。これらはマップのキーにアクセスし、その値に関数を適用します。次のクエリは、著しく速くなります：

```sql
SELECT RequestPage AS path, count() AS c
FROM otel_logs
WHERE RequestType = 'POST'
GROUP BY path
ORDER BY c DESC
LIMIT 5

┌─path─────────────────────┬─────c─┐
│ /m/updateVariation    │ 12182 │
│ /site/productCard     │ 11080 │
│ /site/productPrice    │ 10876 │
│ /site/productAdditives │ 10866 │
│ /site/productModelImages │ 10866 │
└──────────────────────────┴───────┘

5 rows in set. Elapsed: 0.173 sec. Processed 10.37 million rows, 418.03 MB (60.07 million rows/s., 2.42 GB/s.)
Peak memory usage: 3.16 MiB.
```

> マテリアライズドカラムは、デフォルトで `SELECT *` の場合に返されません。これは、SELECT * の結果が常にINSERTでテーブルに戻されることができるという不変条件を維持するためです。この動作は `asterisk_include_materialized_columns=1` を設定することで無効にでき、Grafanaでも有効にできます（データソース設定の `Additional Settings -> Custom Settings` を参照）。

## マテリアライズドビュー

マテリアライズドビューは、ログやトレースに対してSQLフィルタリングや変換を適用するための、より強力な手段を提供します。

マテリアライズドビューは、クエリのコストをクエリ時から挿入時にシフトすることを許可します。ClickHouseのマテリアライズドビューは、テーブルにデータが挿入される際に、データブロック上でクエリを実行するトリガーに過ぎません。このクエリの結果が、第二の「ターゲット」テーブルに挿入されます。

<img src={require('./images/observability-10.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '600px'}} />

<br />

> ClickHouseのマテリアライズドビューは、基になるテーブルにデータが流入するにつれてリアルタイムで更新され、継続的に更新されるインデックスのように機能します。対照的に、他のデータベースではマテリアライズドビューは通常クエリの静的スナップショットで、リフレッシュが必要です（ClickHouseのリフレッシュ可能なマテリアライズドビューに似ています）。

マテリアライズドビューに関連付けられたクエリは、理論的には、集計を含む任意のクエリである可能性がありますが、[JOINに制限があります](https://clickhouse.com/blog/using-materialized-views-in-clickhouse#materialized-views-and-joins)。ログやトレースに必要な変換やフィルターワークロードにおいて、ユーザーは任意のSELECT文を可能と見なすことができます。

ユーザーは、クエリが単にテーブル（ソーステーブル）に挿入される行上で実行されるトリガーであり、結果が新しいテーブル（ターゲットテーブル）に送信されることを覚えておく必要があります。

ソーステーブルにデータを二重保存しないようにするため、ソーステーブルのテーブルエンジンを[Nullテーブルエンジン](/ja/engines/table-engines/special/null)に変更し、元のスキーマを保持することができます。私たちのOTelコレクターは、このテーブルにデータを送信し続けます。たとえば、ログの場合、otel_logsテーブルは次のようになります。

```sql
CREATE TABLE otel_logs
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
    `LogAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))
) ENGINE = Null
```

Nullテーブルエンジンは強力な最適化機能です— `/dev/null` のように考えてください。このテーブルはデータを保存しませんが、関連付けられたマテリアライズドビューは挿入された行に対して実行され続けます。

以下のクエリを検討してください。これは、行を私たちが保存したいフォーマットに変換します。LogAttributesからすべてのカラムを抽出し（これはコレクターによって `json_parser` オペレーターを使用して設定されたと仮定）、`SeverityText` と `SeverityNumber` を設定します（これにはいくつかの単純な条件と[これらのカラムに関する定義](https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitytext)に基づいています）。この場合、ポピュレーションされることがわかっているカラムのみを選択します—TraceId、SpanId、TraceFlagsなどのカラムは無視します。

```sql
SELECT
        Body, 
	Timestamp::DateTime AS Timestamp,
	ServiceName,
	LogAttributes['status'] AS Status,
	LogAttributes['request_protocol'] AS RequestProtocol,
	LogAttributes['run_time'] AS RunTime,
	LogAttributes['size'] AS Size,
	LogAttributes['user_agent'] AS UserAgent,
	LogAttributes['referer'] AS Referer,
	LogAttributes['remote_user'] AS RemoteUser,
	LogAttributes['request_type'] AS RequestType,
	LogAttributes['request_path'] AS RequestPath,
	LogAttributes['remote_addr'] AS RemoteAddr,
	domain(LogAttributes['referer']) AS RefererDomain,
	path(LogAttributes['request_path']) AS RequestPage,
	multiIf(Status::UInt64 > 500, 'CRITICAL', Status::UInt64 > 400, 'ERROR', Status::UInt64 > 300, 'WARNING', 'INFO') AS SeverityText,
	multiIf(Status::UInt64 > 500, 20, Status::UInt64 > 400, 17, Status::UInt64 > 300, 13, 9) AS SeverityNumber
FROM otel_logs
LIMIT 1
FORMAT Vertical

行 1:
──────
Body:         {"remote_addr":"54.36.149.41","remote_user":"-","run_time":"0","time_local":"2019-01-22 00:26:14.000","request_type":"GET","request_path":"\/filter\/27|13 ,27|  5 ,p53","request_protocol":"HTTP\/1.1","status":"200","size":"30577","referer":"-","user_agent":"Mozilla\/5.0 (compatible; AhrefsBot\/6.1; +http:\/\/ahrefs.com\/robot\/)"}
Timestamp:    2019-01-22 00:26:14
ServiceName:
Status:       200
RequestProtocol: HTTP/1.1
RunTime:      0
Size:         30577
UserAgent:    Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)
Referer:      -
RemoteUser:   -
RequestType:  GET
RequestPath:  /filter/27|13 ,27|  5 ,p53
RemoteAddr:  54.36.149.41
RefererDomain:
RequestPage:  /filter/27|13 ,27|  5 ,p53
SeverityText: INFO
SeverityNumber:  9

1 row in set. Elapsed: 0.027 sec.
```

上記のカラムには、将来的に追加される可能性のある追加属性を考慮して、`Body` カラムも抽出されています。このカラムはClickHouseでの圧縮が効き、アクセスされることはほとんどないため、クエリパフォーマンスに影響を与えることはありません。最後に、日時をDateTimeに変換（スペースを節約するため）します（「型の最適化」を参照）。

> 上記の `SeverityText` と `SeverityNumber` を抽出するための[条件文](/ja/sql-reference/functions/conditional-functions)の使用に注意してください。これらは複雑な条件を形成し、マップ内の値がセットされているかどうかを確認するのに非常に便利です。ルールに従い、LogAttributesにすべてのキーが存在することを単純に仮定します。ユーザーはそれに慣れることをお勧めします。これは、ログパースの友であり、[NULL値を扱う関数](/ja/sql-reference/functions/functions-for-nulls)の関数の他です！

これらの結果を受け取るためのテーブルが必要です。以下のターゲットテーブルは、上記のクエリに一致します：

```sql
CREATE TABLE otel_logs_v2
(
    `Body` String,
    `Timestamp` DateTime,
    `ServiceName` LowCardinality(String),
    `Status` UInt16,
    `RequestProtocol` LowCardinality(String),
    `RunTime` UInt32,
    `Size` UInt32,
    `UserAgent` String,
    `Referer` String,
    `RemoteUser` String,
    `RequestType` LowCardinality(String),
    `RequestPath` String,
    `RemoteAddress` IPv4,
    `RefererDomain` String,
    `RequestPage` String,
    `SeverityText` LowCardinality(String),
    `SeverityNumber` UInt8
)
ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp)
```

ここで選択された型は、「型の最適化」に関して議論された最適化に基づいています。

> スキーマが劇的に変更されたことに注意してください。実際には、ユーザーはTraceカラムを保持したい場合や、`ResourceAttributes`カラムも保持したい場合があるでしょう（これは通常Kubernetesメタデータを含みます）。Grafanaはトレースカラムを利用して、ログとトレース間のリンク機能を提供できます—「Grafanaの使用」を参照してください。

以下に、`otel_logs` テーブルに対して上記のセレクトを実行し、結果を `otel_logs_v2` に送信するマテリアライズドビュー `otel_logs_mv` を作成します。

```sql
CREATE MATERIALIZED VIEW otel_logs_mv TO otel_logs_v2 AS
SELECT
        Body, 
    Timestamp::DateTime AS Timestamp,
    ServiceName,
    LogAttributes['status']::UInt16 AS Status,
    LogAttributes['request_protocol'] AS RequestProtocol,
    LogAttributes['run_time'] AS RunTime,
    LogAttributes['size'] AS Size,
    LogAttributes['user_agent'] AS UserAgent,
    LogAttributes['referer'] AS Referer,
    LogAttributes['remote_user'] AS RemoteUser,
    LogAttributes['request_type'] AS RequestType,
    LogAttributes['request_path'] AS RequestPath,
    LogAttributes['remote_addr'] AS RemoteAddress,
    domain(LogAttributes['referer']) AS RefererDomain,
    path(LogAttributes['request_path']) AS RequestPage,
    multiIf(Status::UInt64 > 500, 'CRITICAL', Status::UInt64 > 400, 'ERROR', Status::UInt64 > 300, 'WARNING', 'INFO') AS SeverityText,
    multiIf(Status::UInt64 > 500, 20, Status::UInt64 > 400, 17, Status::UInt64 > 300, 13, 9) AS SeverityNumber
FROM otel_logs
```

以下に示すように、上記は視覚化されます。

<img src={require('./images/observability-11.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

「Exporting to ClickHouse」で使用されているコレクター設定を再起動すると、データは期待される形式で `otel_logs_v2` に現れます。型付きJSON抽出関数の使用に注意してください。

```sql
SELECT *
FROM otel_logs_v2
LIMIT 1
FORMAT Vertical

行 1:
──────
Body:         {"remote_addr":"54.36.149.41","remote_user":"-","run_time":"0","time_local":"2019-01-22 00:26:14.000","request_type":"GET","request_path":"\/filter\/27|13 ,27|  5 ,p53","request_protocol":"HTTP\/1.1","status":"200","size":"30577","referer":"-","user_agent":"Mozilla\/5.0 (compatible; AhrefsBot\/6.1; +http:\/\/ahrefs.com\/robot\/)"}
Timestamp:    2019-01-22 00:26:14
ServiceName:
Status:       200
RequestProtocol: HTTP/1.1
RunTime:      0
Size:         30577
UserAgent:    Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)
Referer:      -
RemoteUser:   -
RequestType:  GET
RequestPath:  /filter/27|13 ,27|  5 ,p53
RemoteAddress:  54.36.149.41
RefererDomain:
RequestPage:  /filter/27|13 ,27|  5 ,p53
SeverityText: INFO
SeverityNumber: 9

1 row in set. Elapsed: 0.010 sec.
```

JSON関数を使用して `Body` カラムからカラムを抽出する同等のマテリアライズドビューを以下に示します：

```sql
CREATE MATERIALIZED VIEW otel_logs_mv TO otel_logs_v2 AS
SELECT  Body, 
    Timestamp::DateTime AS Timestamp,
    ServiceName,
    JSONExtractUInt(Body, 'status') AS Status,
    JSONExtractString(Body, 'request_protocol') AS RequestProtocol,
    JSONExtractUInt(Body, 'run_time') AS RunTime,
    JSONExtractUInt(Body, 'size') AS Size,
    JSONExtractString(Body, 'user_agent') AS UserAgent,
    JSONExtractString(Body, 'referer') AS Referer,
    JSONExtractString(Body, 'remote_user') AS RemoteUser,
    JSONExtractString(Body, 'request_type') AS RequestType,
    JSONExtractString(Body, 'request_path') AS RequestPath,
    JSONExtractString(Body, 'remote_addr') AS remote_addr,
    domain(JSONExtractString(Body, 'referer')) AS RefererDomain,
    path(JSONExtractString(Body, 'request_path')) AS RequestPage,
    multiIf(Status::UInt64 > 500, 'CRITICAL', Status::UInt64 > 400, 'ERROR', Status::UInt64 > 300, 'WARNING', 'INFO') AS SeverityText,
    multiIf(Status::UInt64 > 500, 20, Status::UInt64 > 400, 17, Status::UInt64 > 300, 13, 9) AS SeverityNumber
FROM otel_logs
```

### 型に注意

上記のマテリアライズドビューは暗黙の型変換に依存しています—特にLogAttributesマップの使用において。ClickHouseはしばしば、抽出された値を対象テーブルの型に透明にキャストし、必要な構文を減少させます。しかし、ユーザーは、同じスキーマを使用するターゲットテーブルに対して `SELECT` ステートメントを使用してビューを常にテストすることをお勧めします。これにより、型が正しく処理されていることを確認できます。特に以下のケースには注意が必要です：

- マップ内にキーが存在しない場合、空の文字列が返されます。数値の場合、これは適切な値にマッピングする必要があります。これは、[条件文](/ja/sql-reference/functions/conditional-functions)（例：`if(LogAttributes['status'] = ", 200, LogAttributes['status'])`）または[キャスト関数](/ja/sql-reference/functions/type-conversion-functions#touint8163264256ordefault)を使用してこのように達成できます。デフォルト値が許容される場合（例：`toUInt8OrDefault(LogAttributes['status'] )`）。
- 一部の型は常にキャストされない場合があります。例えば、数値の文字列表現は、列挙値にキャストされません。
- JSON抽出関数は、値が見つからない場合、型のデフォルト値を返します。これらの値が意味を持つか確認してください！

> 観測性データの ClickHouse において Nullable を使用することは避けてください。ログやトレースにおいて、空と NULL の違いを区別する必要はほとんどありません。この機能は追加のストレージオーバーヘッドをもたらし、クエリパフォーマンスに悪影響を与えます。詳細については[こちら](/ja/data-modeling/schema-design#optimizing-types)を参照ください。

## 主キー（オーダリングキー）の選択

希望するカラムを抽出したら、オーダリング/主キーを最適化することができます。

オーダリングキーを選択するためのいくつかの簡単なルールがあります。以下は、時には対立することがあるため、これらを順番に検討してください。このプロセスから多くのキーを特定でき、通常4〜5個で十分です。

1. 一般的なフィルタやアクセスパターンに適合するカラムを選択します。ユーザーが通常、特定のカラム（例：ポッド名）でフィルタリングを開始する場合、このカラムは `WHERE` 句で頻繁に使用されます。これらを他の頻繁に使用されないカラムよりも優先してキーに含めます。
2. フィルタリング時に全体の行数の大部分を除外するのに役立つカラムを優先し、必要に応じて読み取るデータの量を減少させます。サービス名やステータスコードはしばしば良い候補です。後者の場合、ユーザーが200でフィルタリングするとほとんどの行にマッチするのが一般的です。500エラーのように小さなサブセットに関連付けられる場合を除きます。
3. テーブル内の他のカラムと高い相関がある可能性が高いカラムを優先します。これにより、これらの値が連続して保存され、圧縮が向上します。
4. オーダリングキーのカラムの `GROUP BY` および `ORDER BY` 操作は、メモリ効率を向上させることができます。

<br />

オーダリングキーの一部のカラムを特定すると、それらを特定の順序で宣言する必要があります。この順序は、クエリ内のセカンダリキー列でのフィルタリング効率と、テーブルのデータファイルの圧縮率に大きな影響を与える可能性があります。一般的に、**カーディナリティの昇順でキーを並べることが最良です**。ただし、オーダリングキー内で後に現れるカラムでフィルタリングする際の効率が低くなることにバランスを取る必要があります。これらの動作をバランスさせ、アクセスパターンを考慮してください。最も重要なのは、さまざまなバリエーションをテストすることです。オーダリングキーやオプティマイズの方法について理解を深めるためには、[この記事](/ja/optimize/sparse-primary-indexes)をお勧めします。

> ログを整形した後に、オーダリングキーを決定することをお勧めします。属性マップ内のキーやJSON抽出式をオーダリングキーとして使用しないでください。オーダリングキーをテーブル内のルートカラムとして保持してください。

## マップの使用

以前の例では、`Map(String, String)` カラム内の値にアクセスするためにマップ構文 `map['key']` を使用していることを示しています。ネストされたキーにアクセスするためにマップ記法を使用するだけでなく、ClickHouse専用の[マップ関数](/ja/sql-reference/functions/tuple-map-functions#mapkeys)が利用可能で、これらのカラムをフィルタリングまたは選択することができます。

たとえば、次のクエリでは、[`mapKeys` 関数](/ja/sql-reference/functions/tuple-map-functions#mapkeys)を使用して、LogAttributesカラム内で利用可能なすべてのユニークキーを特定し、その後に[groupArrayDistinctArray関数](/ja/sql-reference/aggregate-functions/combinators)（コンビネータ）を続けます。

```sql
SELECT groupArrayDistinctArray(mapKeys(LogAttributes))
FROM otel_logs
FORMAT Vertical

行 1:
──────
groupArrayDistinctArray(mapKeys(LogAttributes)): ['remote_user','run_time','request_type','log.file.name','referer','request_path','status','user_agent','remote_addr','time_local','size','request_protocol']

1 row in set. Elapsed: 1.139 sec. Processed 5.63 million rows, 2.53 GB (4.94 million rows/s., 2.22 GB/s.)
Peak memory usage: 71.90 MiB.
```

> マップカラム名にドットを使用することをお勧めせず、将来的にその使用を非推奨にするかもしれません。 `_` を使用してください。

## エイリアスの使用

マップ型をクエリするのは、通常のカラムをクエリするよりも遅くなります—「クエリの高速化」を参照してください。さらに、構文がより複雑で、ユーザーが書くのが煩わしくなる可能性があります。この後の問題に対処するために、エイリアスカラムを使用することをお勧めします。

エイリアス（ALIAS）カラムは、クエリ時に計算され、テーブルに保存されません。したがって、この型のカラムに値をINSERTすることは不可能です。エイリアスを使用することで、マップキーを参照し、構文を簡素化し、マップエントリを通常のカラムとして透過的に露出させることができます。次の例を考えてみてください：

```sql
CREATE TABLE otel_logs
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
    `RequestPath` String MATERIALIZED path(LogAttributes['request_path']),
    `RequestType` LowCardinality(String) MATERIALIZED LogAttributes['request_type'],
    `RefererDomain` String MATERIALIZED domain(LogAttributes['referer']),
    `RemoteAddr` IPv4 ALIAS LogAttributes['remote_addr']
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, Timestamp)
```

いくつかのマテリアライズドカラムと、`ALIAS`カラムである`RemoteAddr`があり、これがマップ`LogAttributes`にアクセスします。これにより、`LogAttributes['remote_addr']`の値をこのカラムを通じてクエリできるため、クエリが簡素化されます。つまり、次のようにします。

```sql
SELECT RemoteAddr
FROM default.otel_logs
LIMIT 5

┌─RemoteAddr────┐
│ 54.36.149.41  │
│ 31.56.96.51   │
│ 31.56.96.51   │
│ 40.77.167.129 │
│ 91.99.72.15   │
└───────────────┘

5 rows in set. Elapsed: 0.011 sec.
```

さらに、`ALIAS`の追加は`ALTER TABLE`コマンドを使用して簡単に行えます。これらのカラムは即座に利用可能です。たとえば次のようにします。

```sql
ALTER TABLE default.otel_logs
	(ADD COLUMN `Size` String ALIAS LogAttributes['size'])

SELECT Size
FROM default.otel_logs_v3
LIMIT 5

┌─Size──┐
│ 30577 │
│ 5667  │
│ 5379  │
│ 1696  │
│ 41483 │
└───────┘

5 rows in set. Elapsed: 0.014 sec.
```

> デフォルトでは、`SELECT *`はALIASカラムを除外します。この動作は`asterisk_include_alias_columns=1`を設定することで無効にできます。

## 型の最適化

型の最適化に関する[一般的な ClickHouse のベストプラクティス](/ja/data-modeling/schema-design#optimizing-types)は、ClickHouseの使用ケースにも適用されます。

## コーデックの使用

型の最適化に加えて、ユーザーはClickHouseの可視性スキーマの圧縮を最適化しようとする際に、[コーデックに関する一般的なベストプラクティス](/ja/data-compression/compression-in-clickhouse#choosing-the-right-column-compression-codec)に従うことができます。

一般に、ユーザーは`ZSTD`コーデックがログとトレースデータセットに非常に適用可能であることを確認できます。圧縮値をデフォルトの1から増加させることで、圧縮が改善される可能性があります。ただし、高い値は挿入時により大きなCPUオーバーヘッドを発生させるため、テストする必要があります。通常、この値を増加させても利益は少ないです。

さらに、タイムスタンプは圧縮に関してデルタエンコーディングから恩恵を受けますが、このカラムが主キー/順序付けキーで使用される場合、クエリパフォーマンスが遅くなることが示されています。ユーザーには、それぞれの圧縮とクエリパフォーマンスのトレードオフを評価することをお勧めします。

## Dictionaryの使用

[Dictionary](/ja/sql-reference/dictionaries)は、ClickHouseの[重要な機能](https://clickhouse.com/blog/faster-queries-dictionaries-clickhouse)であり、さまざまな内部および外部の[ソース](/ja/sql-reference/dictionaries#dictionary-sources)からのデータを、メモリ内で[キー・バリュー](https://en.wikipedia.org/wiki/Key%E2%80%93value_database)表現として提供し、超低遅延のルックアップクエリに最適化されています。

<img src={require('./images/observability-12.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

これは、データを集約する際に、取り込んだデータをその場で強化し、取り込みプロセスを遅延させず、全体的にクエリのパフォーマンスを向上させるなど、さまざまなシナリオで便利です。特にJOINに有益です。可視性の使用ケースではジョインが必要になることは稀ですが、Dictionaryは挿入時とクエリ時の両方で強化目的で役立つことがあります。以下に両方の例を示します。

> Dictionaryを使用してジョインを加速したいユーザーは、[こちら](/ja/dictionary)でさらに詳細を確認できます。

### 挿入時とクエリ時

Dictionaryは、データセットをクエリ時または挿入時に強化するために使用できます。これらのアプローチにはそれぞれ利点と欠点があります。要約すると：

- **挿入時** - これは通常、強化値が変更されず、Dictionaryにポピュレートできる外部ソースに存在する場合に適切です。この場合、挿入時に行を強化すると、クエリ時にDictionaryのルックアップが回避されます。これは、挿入パフォーマンスのコストと、強化された値がカラムとして格納されるため、追加のストレージオーバーヘッドが発生します。
- **クエリ時** - Dictionary内の値が頻繁に変更される場合、クエリ時ルックアップがより適用可能です。値がマッピングされて変更された場合、カラムを更新する（データを再書き込みする）必要がなくなります。この柔軟性は、クエリ時のルックアップコストを考慮に入れる必要があります。たとえば、フィルター句でDictionaryルックアップを使用する場合、複数の行のルックアップが必要であれば、このクエリ時コストは通常は顕著です。結果の強化、すなわち`SELECT`内では、このオーバーヘッドは通常顕著ではありません。

私たちは、ユーザーがDictionaryの基本を理解することをお勧めします。Dictionaryは、特定の[専門関数](/ja/sql-reference/functions/ext-dict-functions#dictgetall)を使用して値を取得できるメモリ内のルックアップテーブルを提供します。

シンプルな強化例については、Dictionaryに関するガイド[こちら](/ja/dictionary)を参照してください。以下では、一般的な可視性強化タスクに焦点を当てます。

### IP Dictionaryの使用

IPアドレスを使用してログやトレースに緯度と経度の値をジオ強化することは、一般的な可視性要件です。`ip_trie` Dictionary構造を使用することで実現できます。

私たちは、[DB-IP.com](https://db-ip.com/)によって提供されている、月ごとに更新される[DB-IP市レベルデータセット](https://github.com/sapics/ip-location-db#db-ip-database-update-monthly)を公開されているものを使用します。

[README](https://github.com/sapics/ip-location-db#csv-format)から、データが次のように構造化されていることを確認できます。

```csv
| ip_range_start | ip_range_end | country_code | state1 | state2 | city | postcode | latitude | longitude | timezone |
```

この構造を考慮して、URLテーブル関数を使用してデータを確認してみましょう。

```sql
SELECT *
FROM url('https://raw.githubusercontent.com/sapics/ip-location-db/master/dbip-city/dbip-city-ipv4.csv.gz', 'CSV', '\n    	\tip_range_start IPv4, \n    	\tip_range_end IPv4, \n    	\tcountry_code Nullable(String), \n    	\tstate1 Nullable(String), \n    	\tstate2 Nullable(String), \n    	\tcity Nullable(String), \n    	\tpostcode Nullable(String), \n    	\tlatitude Float64, \n    	\tlongitude Float64, \n    	\ttimezone Nullable(String)\n	\t')
LIMIT 1
FORMAT Vertical
Row 1:
──────
ip_range_start: 1.0.0.0
ip_range_end:   1.0.0.255
country_code:   AU
state1:     	Queensland
state2:     	ᴺᵁᴸᴸ
city:       	South Brisbane
postcode:   	ᴺᵁᴸᴸ
latitude:   	-27.4767
longitude:  	153.017
timezone:   	ᴺᵁᴸᴸ
```

私たちの生活を簡単にするために、[`URL()`](/ja/engines/table-engines/special/url)テーブルエンジンを使用して、ClickHouseテーブルオブジェクトを作成し、行数を確認します。

```sql
CREATE TABLE geoip_url(
	ip_range_start IPv4,
	ip_range_end IPv4,
	country_code Nullable(String),
	state1 Nullable(String),
	state2 Nullable(String),
	city Nullable(String),
	postcode Nullable(String),
	latitude Float64,
	longitude Float64,
	timezone Nullable(String)
) ENGINE = URL('https://raw.githubusercontent.com/sapics/ip-location-db/master/dbip-city/dbip-city-ipv4.csv.gz', 'CSV')

SELECT count() FROM geoip_url;

┌─count()─┐
│ 3261621 │ -- 3.26百万
└─────────┘
```

私たちの`ip_trie`Dictionaryは、IPアドレス範囲をCIDR表記で表現する必要があるため、`ip_range_start`と`ip_range_end`を変換する必要があります。

各範囲についてのCIDRは、次のクエリで簡潔に計算できます。

```sql
WITH
	bitXor(ip_range_start, ip_range_end) AS xor,
	if(xor != 0, ceil(log2(xor)), 0) AS unmatched,
	32 - unmatched AS cidr_suffix,
	toIPv4(bitAnd(bitNot(pow(2, unmatched) - 1), ip_range_start)::UInt64) AS cidr_address
SELECT
	ip_range_start,
	ip_range_end,
	concat(toString(cidr_address),'/',toString(cidr_suffix)) AS cidr    
FROM
	geoip_url
LIMIT 4;

┌─ip_range_start─┬─ip_range_end─┬─cidr───────┐
│ 1.0.0.0    	│ 1.0.0.255	│ 1.0.0.0/24 │
│ 1.0.1.0    	│ 1.0.3.255	│ 1.0.0.0/22 │
│ 1.0.4.0    	│ 1.0.7.255	│ 1.0.4.0/22 │
│ 1.0.8.0    	│ 1.0.15.255   │ 1.0.8.0/21 │
└────────────────┴──────────────┴────────────┘

4 rows in set. Elapsed: 0.259 sec.
```

> 上記のクエリでは多くのことが行われています。興味のある方は、こちらの[優れた説明](https://clickhouse.com/blog/geolocating-ips-in-clickhouse-and-grafana#using-bit-functions-to-convert-ip-ranges-to-cidr-notation)を読んでください。それ以外は、上記の計算がIP範囲のCIDRを計算することを受け入れてください。

私たちの目的のためには、IP範囲、国コード、および座標だけが必要ですので、新しいテーブルを作成し、GeoIPデータを挿入します。

```sql
CREATE TABLE geoip
(
	`cidr` String,
	`latitude` Float64,
	`longitude` Float64,
	`country_code` String
)
ENGINE = MergeTree
ORDER BY cidr

INSERT INTO geoip
WITH
	bitXor(ip_range_start, ip_range_end) AS xor,
	if(xor != 0, ceil(log2(xor)), 0) AS unmatched,
	32 - unmatched AS cidr_suffix,
	toIPv4(bitAnd(bitNot(pow(2, unmatched) - 1), ip_range_start)::UInt64) AS cidr_address
SELECT
	concat(toString(cidr_address),'/',toString(cidr_suffix)) AS cidr,
	latitude,
	longitude,
	country_code    
FROM geoip_url
```

低遅延のIPルックアップをClickHouseで実行できるように、Dictionaryを活用して、GeoIPデータのキー→属性マッピングをメモリ内に保存します。ClickHouseは、ネットワークプレフィックス（CIDRブロック）を座標および国コードにマッピングするための`ip_trie` [Dictionary構造](/ja/sql-reference/dictionaries#ip_trie)を提供します。次は、このレイアウトを使用し、上述のテーブルをソースとして指定するDictionaryを作成します。

```sql
CREATE DICTIONARY ip_trie (
   cidr String,
   latitude Float64,
   longitude Float64,
   country_code String
)
PRIMARY KEY cidr
SOURCE(CLICKHOUSE(TABLE 'geoip'))
LAYOUT(ip_trie)
LIFETIME(3600);
```

Dictionaryから行を選択し、このデータセットがルックアップ用に利用可能であることを確認できます。

```sql
SELECT * FROM ip_trie LIMIT 3

┌─cidr───────┬─latitude─┬─longitude─┬─country_code─┐
│ 1.0.0.0/22 │  26.0998 │   119.297 │ CN       	│
│ 1.0.0.0/24 │ -27.4767 │   153.017 │ AU       	│
│ 1.0.4.0/22 │ -38.0267 │   145.301 │ AU       	│
└────────────┴──────────┴───────────┴──────────────┘

3 rows in set. Elapsed: 4.662 sec.
```

> ClickHouseのDictionaryは、基になるテーブルデータと使用したライフタイム条件に基づいて、定期的に更新されます。DB-IPデータセットの最新の変更を反映するために、`geoip`テーブルに対して、geoip_urlリモートテーブルからデータを再挿入する必要があります。

GeoIPデータがip_trie Dictionary（便利にもip_trieという名前）にロードされたので、これを使用してIPのジオロケーションを実行できます。これは、次のように[`dictGet()`関数](/ja/sql-reference/functions/ext-dict-functions)を使用することで実現できます。

```sql
SELECT dictGet('ip_trie', ('country_code', 'latitude', 'longitude'), CAST('85.242.48.167', 'IPv4')) AS ip_details

┌─ip_details──────────────┐
│ ('PT',38.7944,-9.34284) │
└─────────────────────────┘

1 row in set. Elapsed: 0.003 sec.
```

ここでの取得速度に注目してください。これにより、ログを強化することができます。この場合、**クエリ時の強化を実行する**ことにします。

元のログデータセットに戻ると、国別にログを集約するために上記を使用できます。以下は、`RemoteAddress`カラムを抽出したマテリアライズドビューから得られるスキーマを使用すると仮定します。

```sql
SELECT dictGet('ip_trie', 'country_code', tuple(RemoteAddress)) AS country,
	formatReadableQuantity(count()) AS num_requests
FROM default.otel_logs_v2
WHERE country != ''
GROUP BY country
ORDER BY count() DESC
LIMIT 5

┌─country─┬─num_requests────┐
│ IR  	│ 736万	│
│ US  	│ 167万	│
│ AE  	│ 52.674万 │
│ DE  	│ 15.935万 │
│ FR  	│ 10.982万 │
└─────────┴─────────────────┘

5 rows in set. Elapsed: 0.140 sec. Processed 20.73 million rows, 82.92 MB (147.79 million rows/s., 591.16 MB/s.)
Peak memory usage: 1.16 MiB.
```

IPから地理的な位置へのマッピングは変わる可能性があるため、ユーザーはリクエストが行われた時点での元の位置を知りたい可能性が高くなります。この理由から、インデックス時の強化はここで望ましいと考えられます。これは、次のように、マテリアライズドカラムを使用することで行うことができます。

```sql
CREATE TABLE otel_logs_v2
(
	`Body` String,
	`Timestamp` DateTime,
	`ServiceName` LowCardinality(String),
	`Status` UInt16,
	`RequestProtocol` LowCardinality(String),
	`RunTime` UInt32,
	`Size` UInt32,
	`UserAgent` String,
	`Referer` String,
	`RemoteUser` String,
	`RequestType` LowCardinality(String),
	`RequestPath` String,
	`RemoteAddress` IPv4,
	`RefererDomain` String,
	`RequestPage` String,
	`SeverityText` LowCardinality(String),
	`SeverityNumber` UInt8,
    `Country` String MATERIALIZED dictGet('ip_trie', 'country_code', tuple(RemoteAddress)),
    `Latitude` Float32 MATERIALIZED dictGet('ip_trie', 'latitude', tuple(RemoteAddress)),
    `Longitude` Float32 MATERIALIZED dictGet('ip_trie', 'longitude', tuple(RemoteAddress))
)
ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp)
```

> ユーザーは、新しいデータに基づいてIP　Dictionaryの強化が定期的に更新されることを望む可能性が高いです。これはDictionaryのLIFETIME句を使用して達成でき、これによりDictionaryが基になるテーブルから定期的に再ロードされます。基になるテーブルを更新する方法は、「リフレッシュ可能なマテリアライズドビューの使用」を参照してください。

上記の国と座標は、国別にグループ化およびフィルタリングする以上の可視化能力を提供します。インスピレーションについては、「地理データの可視化」を参照してください。

### 正規表現Dictionaryの使用（ユーザーエージェント解析）

ユーザーエージェント文字列の解析は、古典的な正規表現の問題であり、ログやトレースベースのデータセットの一般的な要件です。ClickHouseは、正規表現ツリーディクショナリを使用してユーザーエージェントの効率的な解析を提供します。

正規表現ツリーディクショナリは、ClickHouseオープンソースでYAMLRegExpTree Dictionaryソースタイプを使用して定義されており、正規表現ツリーを含むYAMLファイルへのパスを提供します。独自の正規表現Dictionaryを提供する場合に必要な構造の詳細は[こちら](/ja/sql-reference/dictionaries#use-regular-expression-tree-dictionary-in-clickhouse-open-source)にあります。以下では、ユーザーエージェント解析に[ua-parser](https://github.com/ua-parser/uap-core)を使用し、サポートされているCSVフォーマット用のDictionaryをロードします。このアプローチはOSSとClickHouse Cloudに対応しています。

> 以下の例では、2024年6月の最新のuap-coreユーザーエージェント解析用の正規表現のスナップショットを使用します。最新のファイルは時々更新され、[こちら](https://raw.githubusercontent.com/ua-parser/uap-core/master/regexes.yaml)で確認できます。ユーザーは、以下に使用されるCSVファイルをロードする手順を[こちら](/ja/sql-reference/dictionaries#collecting-attribute-values)で確認できます。

次のように、メモリテーブルを作成します。これにより、デバイス、ブラウザ、およびオペレーティングシステムの解析用の正規表現が保持されます。

```sql
CREATE TABLE regexp_os
(
	id UInt64,
	parent_id UInt64,
	regexp String,
	keys   Array(String),
	values Array(String)
) ENGINE=Memory;

CREATE TABLE regexp_browser
(
	id UInt64,
	parent_id UInt64,
	regexp String,
	keys   Array(String),
	values Array(String)
) ENGINE=Memory;

CREATE TABLE regexp_device
(
	id UInt64,
	parent_id UInt64,
	regexp String,
	keys   Array(String),
	values Array(String)
) ENGINE=Memory;
```

これらのテーブルは、URLテーブル関数を使用して、以下の公開ホストCSVファイルからポピュレートできます。

```sql
INSERT INTO regexp_os SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/user_agent_regex/regexp_os.csv', 'CSV', 'id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')

INSERT INTO regexp_device SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/user_agent_regex/regexp_device.csv', 'CSV', 'id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')

INSERT INTO regexp_browser SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/user_agent_regex/regexp_browser.csv', 'CSV', 'id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
```

メモリテーブルがポピュレートされたので、正規表現 Dictionaryをロードできます。ここで注意すべきは、キー値をカラムとして指定する必要があることです。これが、ユーザーエージェントから抽出できる属性になります。

```sql
CREATE DICTIONARY regexp_os_dict
(
	regexp String,
	os_replacement String default 'Other',
	os_v1_replacement String default '0',
	os_v2_replacement String default '0',
	os_v3_replacement String default '0',
	os_v4_replacement String default '0'
)
PRIMARY KEY regexp
SOURCE(CLICKHOUSE(TABLE 'regexp_os'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(REGEXP_TREE);

CREATE DICTIONARY regexp_device_dict
(
	regexp String,
	device_replacement String default 'Other',
	brand_replacement String,
	model_replacement String
)
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_device'))
LIFETIME(0)
LAYOUT(REGEXP_TREE);

CREATE DICTIONARY regexp_browser_dict
(
	regexp String,
	family_replacement String default 'Other',
	v1_replacement String default '0',
	v2_replacement String default '0'
)
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_browser'))
LIFETIME(0)
LAYOUT(REGEXP_TREE);
```

これらのDictionaryをロードした後、サンプルのユーザーエージェントを提供し、新しいDictionaryの抽出機能をテストできます。

```sql
WITH 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0' AS user_agent
SELECT
	dictGet('regexp_device_dict', ('device_replacement', 'brand_replacement', 'model_replacement'), user_agent) AS device,
	dictGet('regexp_browser_dict', ('family_replacement', 'v1_replacement', 'v2_replacement'), user_agent) AS browser,
	dictGet('regexp_os_dict', ('os_replacement', 'os_v1_replacement', 'os_v2_replacement', 'os_v3_replacement'), user_agent) AS os

┌─device────────────────┬─browser───────────────┬─os─────────────────────────┐
│ ('Mac','Apple','Mac') │ ('Firefox','127','0') │ ('Mac OS X','10','15','0') │
└───────────────────────┴───────────────────────┴────────────────────────────┘

1 row in set. Elapsed: 0.003 sec.
```

ユーザーエージェントに関するルールはほとんど変わらないため、新しいブラウザ、オペレーティングシステム、およびデバイスに応じてDictionaryを更新する必要があるため、挿入時にこの抽出を実行するのが理にかなっています。

この作業は、マテリアライズドカラムを使用するか、マテリアライズドビューを使用して行うことができます。以前に使用したマテリアライズドビューを修正します。

```sql
CREATE MATERIALIZED VIEW otel_logs_mv TO otel_logs_v2
AS SELECT
	Body,
	CAST(Timestamp, 'DateTime') AS Timestamp,
	ServiceName,
	LogAttributes['status'] AS Status,
	LogAttributes['request_protocol'] AS RequestProtocol,
	LogAttributes['run_time'] AS RunTime,
	LogAttributes['size'] AS Size,
	LogAttributes['user_agent'] AS UserAgent,
	LogAttributes['referer'] AS Referer,
	LogAttributes['remote_user'] AS RemoteUser,
	LogAttributes['request_type'] AS RequestType,
	LogAttributes['request_path'] AS RequestPath,
	LogAttributes['remote_addr'] AS RemoteAddress,
	domain(LogAttributes['referer']) AS RefererDomain,
	path(LogAttributes['request_path']) AS RequestPage,
	multiIf(CAST(Status, 'UInt64') > 500, 'CRITICAL', CAST(Status, 'UInt64') > 400, 'ERROR', CAST(Status, 'UInt64') > 300, 'WARNING', 'INFO') AS SeverityText,
	multiIf(CAST(Status, 'UInt64') > 500, 20, CAST(Status, 'UInt64') > 400, 17, CAST(Status, 'UInt64') > 300, 13, 9) AS SeverityNumber,
	dictGet('regexp_device_dict', ('device_replacement', 'brand_replacement', 'model_replacement'), UserAgent) AS Device,
	dictGet('regexp_browser_dict', ('family_replacement', 'v1_replacement', 'v2_replacement'), UserAgent) AS Browser,
	dictGet('regexp_os_dict', ('os_replacement', 'os_v1_replacement', 'os_v2_replacement', 'os_v3_replacement'), UserAgent) AS Os
FROM otel_logs
```

これにより、ターゲットテーブル`otel_logs_v2`のスキーマを修正する必要があります。

```sql
CREATE TABLE default.otel_logs_v2
(
	`Body` String,
	`Timestamp` DateTime,
	`ServiceName` LowCardinality(String),
	`Status` UInt8,
	`RequestProtocol` LowCardinality(String),
	`RunTime` UInt32,
	`Size` UInt32,
	`UserAgent` String,
	`Referer` String,
	`RemoteUser` String,
	`RequestType` LowCardinality(String),
	`RequestPath` String,
	`remote_addr` IPv4,
	`RefererDomain` String,
	`RequestPage` String,
	`SeverityText` LowCardinality(String),
	`SeverityNumber` UInt8,
	`Device` Tuple(device_replacement LowCardinality(String), brand_replacement LowCardinality(String), model_replacement LowCardinality(String)),
	`Browser` Tuple(family_replacement LowCardinality(String), v1_replacement LowCardinality(String), v2_replacement LowCardinality(String)),
	`Os` Tuple(os_replacement LowCardinality(String), os_v1_replacement LowCardinality(String), os_v2_replacement LowCardinality(String), os_v3_replacement LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp, Status)
```

コレクタを再起動し、構造化されたログを取り込んだ後、次のように新しく抽出されたDevice、Browser、およびOsカラムをクエリできます。

```sql
SELECT Device, Browser, Os
FROM otel_logs_v2
LIMIT 1
FORMAT Vertical

Row 1:
──────
Device:  ('Spider','Spider','Desktop')
Browser: ('AhrefsBot','6','1')
Os:  	('Other','0','0','0')
```

> ユーザーエージェントカラムに対してタプルを使用することに注意してください。タプルは、階層が事前に知られている複雑な構造に推奨されます。サブカラムは、マップキーと異なり、通常のカラムと同じパフォーマンスを提供しますが、異種型が可能です。

### さらなる読み物

Dictionaryに関するより多くの例や詳細については、次のような記事をお勧めします。

- [Dictionaryの高度なトピック](/ja/dictionary#advanced-dictionary-topics)
- [「Dictionaryを使用してクエリを加速する」](https://clickhouse.com/blog/faster-queries-dictionaries-clickhouse)
- [Dictionary](/ja/sql-reference/dictionaries)

## クエリの加速

ClickHouseは、クエリパフォーマンスを加速するためのいくつかのテクニックをサポートしています。以下は、適切な主キー/順序付けキーを選択して最も人気のあるアクセスパターンを最適化し、圧縮を最大化する過程で検討すべきことです。これは通常、最小限の労力でパフォーマンスに最も大きな影響を与えます。

## 集約のためのマテリアライズドビュー（増分）使用

以前のセクションでは、データ変換とフィルタリングのためにマテリアライズドビューを使用することを探りました。しかし、マテリアライズドビューは、挿入時に集約を事前計算し、その結果をストアするためにも使用できます。この結果は、後続の挿入からの結果で更新できるため、実際に挿入時に集約を計算することができます。

ここでの主なアイデアは、結果が元のデータの小さい表現（集約の場合の一部のスケッチ）であることが多いということです。結果をターゲットテーブルから読み取るためのシンプルなクエリと組み合わせると、元のデータで同じ計算を行うよりもクエリ時間が早くなります。

構造化ログを使用して、時間ごとのトラフィック合計を計算する次のクエリを考えてみましょう。

```sql
SELECT toStartOfHour(Timestamp) AS Hour,
	sum(toUInt64OrDefault(LogAttributes['size'])) AS TotalBytes
FROM otel_logs
GROUP BY Hour
ORDER BY Hour DESC
LIMIT 5

┌────────────────Hour─┬─TotalBytes─┐
│ 2019-01-26 16:00:00 │ 1661716343 │
│ 2019-01-26 15:00:00 │ 1824015281 │
│ 2019-01-26 14:00:00 │ 1506284139 │
│ 2019-01-26 13:00:00 │ 1580955392 │
│ 2019-01-26 12:00:00 │ 1736840933 │
└─────────────────────┴────────────┘

5 rows in set. Elapsed: 0.666 sec. Processed 10.37 million rows, 4.73 GB (15.56 million rows/s., 7.10 GB/s.)
Peak memory usage: 1.40 MiB.
```

これはGrafanaでの一般的なラインチャートになる可能性があります。このクエリは明らかに非常に速いです - データセットはわずか10百万行であり、ClickHouseは速いです！しかし、これをビリオンやトリリオン行にスケールさせると、私たちはこのクエリパフォーマンスを維持することを望むでしょう。

> このクエリは、LogAttributesからサイズキーを抽出する以前のマテリアライズドビューであるotel_logs_v2を使用すると、10倍速くなります。この例では生データを使用していますが、これはこのクエリが一般的な場合は以前のビューを使用することを推奨します。

マテリアライズドビューを使用して挿入時に計算を行う結果を受け取るテーブルが必要です。このテーブルは、各時間ごとに1行のみを保持する必要があります。既存の時間に対して更新が受信された場合、他のカラムは既存の時間の行にマージされる必要があります。部分的な状態を他のカラムのために保存することで、この増分状態のマージを実現できます。

これはClickHouseの特別なエンジンタイプ、すなわちSummingMergeTreeを必要とします。これにより、同じ順序キーを持つすべての行が1つの行に置き換えられ、数値カラムの値が合計された行が含まれます。次のテーブルは、同じ日付の行をマージし、数値カラムを合計します。

```sql
CREATE TABLE bytes_per_hour
(
  `Hour` DateTime,
  `TotalBytes` UInt64
)
ENGINE = SummingMergeTree
ORDER BY Hour
```

挿入時に上記のSELECTを実行して、マテリアライズドビューを示すと仮定します。bytes_per_hourテーブルは空で、データの受信がまだ行われていないとします。マテリアライズドビューは、otel_logsに挿入されたデータに対して上記のSELECTを実行し、その結果をbytes_per_hourに送信します。構文は以下の通りです。

```sql
CREATE MATERIALIZED VIEW bytes_per_hour_mv TO bytes_per_hour AS
SELECT toStartOfHour(Timestamp) AS Hour,
       sum(toUInt64OrDefault(LogAttributes['size'])) AS TotalBytes
FROM otel_logs
GROUP BY Hour
```

ここでの`TO`句は重要であり、結果が送信される場所、すなわち`bytes_per_hour`を示しています。

Otel Collectorを再起動し、ログを再送信すると、`bytes_per_hour`テーブルは上記のクエリ結果で増分的にポピュレートされます。完了後、`bytes_per_hour`のサイズを確認すると、時間ごとに1行のはずです。

```sql
SELECT count()
FROM bytes_per_hour
FINAL

┌─count()─┐
│ 	113 │
└─────────┘
```

ここでは、`otel_logs`内の10百万行から113行に実質的に減少しました。このキーは、新しいログが`otel_logs`テーブルに挿入されると、新しい値がそれぞれの時間のために`bytes_per_hour`に送信され、バックグラウンドで非同期に自動的にマージされます。`bytes_per_hour`は常に小さく、最新の状態に保たれます。

行のマージは非同期であるため、ユーザーがクエリを行う際には、時間ごとに複数の行が存在する可能性があります。クエリ時に未処理の行がマージされるようにするために、2つのオプションがあります。

1. テーブル名の[`FINAL`修飾子](/ja/sql-reference/statements/select/from#final-modifier)を使用します。これは、上記のカウントクエリでも行いました。
2. 最終テーブルで使用される順序キー（この場合はTimestamp）を集約し、メトリックを合計します。これは通常、効率が良く柔軟性があり（テーブルは他の用途にも使用できる）、前者は一部のクエリにはより簡単かもしれません。両方を以下に示します。

```sql
SELECT
	Hour,
	sum(TotalBytes) AS TotalBytes
FROM bytes_per_hour
GROUP BY Hour
ORDER BY Hour DESC
LIMIT 5

┌────────────────Hour─┬─TotalBytes─┐
│ 2019-01-26 16:00:00 │ 1661716343 │
│ 2019-01-26 15:00:00 │ 1824015281 │
│ 2019-01-26 14:00:00 │ 1506284139 │
│ 2019-01-26 13:00:00 │ 1580955392 │
│ 2019-01-26 12:00:00 │ 1736840933 │
└─────────────────────┴────────────┘

5 rows in set. Elapsed: 0.008 sec.

SELECT
	Hour,
	TotalBytes
FROM bytes_per_hour
FINAL
ORDER BY Hour DESC
LIMIT 5

┌────────────────Hour─┬─TotalBytes─┐
│ 2019-01-26 16:00:00 │ 1661716343 │
│ 2019-01-26 15:00:00 │ 1824015281 │
│ 2019-01-26 14:00:00 │ 1506284139 │
│ 2019-01-26 13:00:00 │ 1580955392 │
│ 2019-01-26 12:00:00 │ 1736840933 │
└─────────────────────┴────────────┘

5 rows in set. Elapsed: 0.005 sec.
```

この結果、クエリの速度は0.6秒から0.008秒に75倍向上しました！

> これらの節約は、より大きなデータセットやより複雑なクエリではさらに大きくなる可能性があります。例については[こちら](https://github.com/ClickHouse/clickpy)を参照してください。

### より複雑な例

上記の例は、SummingMergeTreeを使用して時間ごとの単純なカウントを集約しています。単純な合計を超える統計には、異なるターゲットテーブルエンジンであるAggregatingMergeTreeが必要です。

たとえば、日ごとのユニークなIPアドレス数（ユニークなユーザー数）を計算したいとします。このためのクエリは次の通りです。

```sql
SELECT toStartOfHour(Timestamp) AS Hour, uniq(LogAttributes['remote_addr']) AS UniqueUsers
FROM otel_logs
GROUP BY Hour
ORDER BY Hour DESC

┌────────────────Hour─┬─UniqueUsers─┐
│ 2019-01-26 16:00:00 │   	4763 │
…
│ 2019-01-22 00:00:00 │    	536 │
└─────────────────────┴────────────┘

113 rows in set. Elapsed: 0.667 sec. Processed 10.37 million rows, 4.73 GB (15.53 million rows/s., 7.09 GB/s.)
```

集約のために増分更新を維持するには、AggregatingMergeTreeが必要です。

```sql
CREATE TABLE unique_visitors_per_hour
(
  `Hour` DateTime,
  `UniqueUsers` AggregateFunction(uniq, IPv4)
)
ENGINE = AggregatingMergeTree
ORDER BY Hour
```

ClickHouseが集約状態が保存されることを認識するように、`UniqueUsers`カラムの型を[`AggregateFunction`](/ja/sql-reference/data-types/aggregatefunction)として定義し、部分状態のソース関数（uniq）とソースカラムの型（IPv4）を指定します。SummingMergeTreeと同様に、同じ`ORDER BY`キー値を持つ行がマージされます（上記の例ではHour）。

関連するマテリアライズドビューは、以前のクエリを使用します。

```sql
CREATE MATERIALIZED VIEW unique_visitors_per_hour_mv TO unique_visitors_per_hour AS
SELECT toStartOfHour(Timestamp) AS Hour,
	uniqState(LogAttributes['remote_addr']::IPv4) AS UniqueUsers
FROM otel_logs
GROUP BY Hour
ORDER BY Hour DESC
```

集約関数の末尾に`suffix`を追加することに注意してください。これは、関数の集約状態が戻されることを保証し、最終結果ではありません。これにより、この部分状態が他の状態とマージするための追加情報を含むことができます。

データが再読み込みされた後、コレクタの再起動を経て、`unique_visitors_per_hour`テーブルに113行が確認できることができます。

```sql
SELECT count()
FROM unique_visitors_per_hour
FINAL

┌─count()─┐
│ 	113 │
└─────────┘
```
Our final query needs to utilize the Merge suffix for our functions (as the columns store partial aggregation states):

```sql
SELECT Hour, uniqMerge(UniqueUsers) AS UniqueUsers
FROM unique_visitors_per_hour
GROUP BY Hour
ORDER BY Hour DESC

┌────────────────Hour─┬─UniqueUsers─┐
│ 2019-01-26 16:00:00 │   	 4763 │

│ 2019-01-22 00:00:00 │		 536 │
└─────────────────────┴─────────────┘

113 rows in set. Elapsed: 0.027 sec.
```

Note we use a `GROUP BY` here instead of using `FINAL`.

## 使用したマテリアライズドビュー (累積) による高速検索

ユーザーは、フィルターおよび集計句で頻繁に使用されるカラムで ClickHouse の順序キーを選択する際に、そのアクセスパターンを考慮する必要があります。これは、ユーザーが多様なアクセスパターンを持つ観測性のユースケースにおいて、単一のカラムセットでカプセル化できないため制約になることがあります。これをデフォルトのOTelスキーマに組み込まれた例で最もよく示しています。トレースのデフォルトスキーマを考えてみましょう：

```sql
CREATE TABLE otel_traces
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
```

このスキーマは、`ServiceName`、`SpanName`、および `Timestamp` によるフィルタリングに最適化されています。トレーシングでは、ユーザーは特定の `TraceId` による検索を行い、関連するトレースのスパンを取得する必要があります。これが順序キーに存在しますが、その位置が最後にあるため、[フィルタリングの効率が低下する](https://en/optimize/sparse-primary-indexes#ordering-key-columns-efficiently)ことがあり、単一のトレースを取得する際に、多くのデータをスキャンしなければならない可能性があります。

OTel コレクターは、この課題に対処するために、マテリアライズドビューと関連するテーブルもインストールします。テーブルとビューは以下のようになります：

```sql
CREATE TABLE otel_traces_trace_id_ts
(
	`TraceId` String CODEC(ZSTD(1)),
	`Start` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	`End` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (TraceId, toUnixTimestamp(Start))


CREATE MATERIALIZED VIEW otel_traces_trace_id_ts_mv TO otel_traces_trace_id_ts
(
	`TraceId` String,
	`Start` DateTime64(9),
	`End` DateTime64(9)
)
AS SELECT
	TraceId,
	min(Timestamp) AS Start,
	max(Timestamp) AS End
FROM otel_traces
WHERE TraceId != ''
GROUP BY TraceId
```

このビューは、テーブル `otel_traces_trace_id_ts` にトレースの最小および最大のタイムスタンプを持つことを実効的に保証します。このテーブルは、`TraceId` によって順序付けられており、これによりこれらのタイムスタンプを効率的に取得できます。これらのタイムスタンプ範囲は、メイントレース `otel_traces` テーブルをクエリする際にも使用できます。具体的には、トレースのIDで取得する際にGrafanaが使用するクエリは以下の通りです：

```sql
WITH 'ae9226c78d1d360601e6383928e4d22d' AS trace_id,
	(
    	SELECT min(Start)
    	  FROM default.otel_traces_trace_id_ts
    	  WHERE TraceId = trace_id
	) AS trace_start,
	(
    	SELECT max(End) + 1
    	  FROM default.otel_traces_trace_id_ts
    	  WHERE TraceId = trace_id
	) AS trace_end
SELECT
	TraceId AS traceID,
	SpanId AS spanID,
	ParentSpanId AS parentSpanID,
	ServiceName AS serviceName,
	SpanName AS operationName,
	Timestamp AS startTime,
	Duration * 0.000001 AS duration,
	arrayMap(key -> map('key', key, 'value', SpanAttributes[key]), mapKeys(SpanAttributes)) AS tags,
	arrayMap(key -> map('key', key, 'value', ResourceAttributes[key]), mapKeys(ResourceAttributes)) AS serviceTags
FROM otel_traces
WHERE (traceID = trace_id) AND (startTime >= trace_start) AND (startTime <= trace_end)
LIMIT 1000
```

ここでのCTEは、トレースID `ae9226c78d1d360601e6383928e4d22d` の最小および最大タイムスタンプを特定し、これを使用してメインの `otel_traces` テーブルの関連スパンをフィルタリングします。

この同じアプローチは、同様のアクセスパターンに適用できます。データモデリングで類似の例を探求しています。

## プロジェクションの使用

ClickHouse のプロジェクションにより、ユーザーはテーブルに対して複数の `ORDER BY` 句を指定できます。

前のセクションでは、マテリアライズドビューを使用して ClickHouse で集計を事前に計算し、行を変換し、異なるアクセスパターンに合わせた観測性のクエリを最適化する方法を探求しました。 

マテリアライズドビューは、トレースIDによる検索を最適化するために、元のテーブルとは異なる順序キーを持つターゲットテーブルに行を送信する例を提供しました。

プロジェクションは、主キーの一部ではないカラムに対するクエリを最適化するために、同じ問題に対処するために使用できます。

理論的には、この能力はテーブルのために複数の順序キーを提供するために使用できますが、一つの明確な欠点があります：データの重複です。具体的には、データは各プロジェクションに対して指定された順序に加え、主な主キーの順序で書き込む必要があります。これにより、挿入が遅くなり、より多くのディスクスペースが消費されます。

> プロジェクションは、マテリアライズドビューと同様の多くの機能を提供しますが、後者が好まれることが多く、控えめに使用するべきです。ユーザーは欠点を理解し、いつ適切に使用するかを理解する必要があります。例えば、プロジェクションは集計を事前に計算するために使用できますが、ユーザーはこれにマテリアライズドビューを使用することをお勧めします。

<img src={require('./images/observability-13.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '800px'}} />

<br />

以下のクエリを考慮します。このクエリは `otel_logs_v2` テーブルを 500 のエラーコードでフィルターします。これは、ユーザーがエラーコードでフィルタリングしたいと考えているため、ログに使用される一般的なアクセスパターンです：

```sql
SELECT Timestamp, RequestPath, Status, RemoteAddress, UserAgent
FROM otel_logs_v2
WHERE Status = 500
FORMAT `Null`

Ok.

0 rows in set. Elapsed: 0.177 sec. Processed 10.37 million rows, 685.32 MB (58.66 million rows/s., 3.88 GB/s.)
Peak memory usage: 56.54 MiB.
```

> この場合、`FORMAT Null` を使用して結果を印刷していません。これにより、すべての結果が読み取られますが返却されないため、LIMIT によるクエリの早期終了を防ぎます。これは、全ての1000万行をスキャンするのにかかった時間を示すためのものです。

上記のクエリは、選択した順序キー `(ServiceName, Timestamp)` に対して線形スキャンを必要とします。私たちは、上記のクエリのパフォーマンスを向上させるために、`Status` を順序キーの最後に追加することができますが、プロジェクションを追加することもできます。

```sql
ALTER TABLE otel_logs_v2 (
  ADD PROJECTION status
  (
     SELECT Timestamp, RequestPath, Status, RemoteAddress, UserAgent ORDER BY Status
  )
)

ALTER TABLE otel_logs_v2 MATERIALIZE PROJECTION status
```

プロジェクションを最初に作成し、次にそれをマテリアライズする必要があります。この後のコマンドにより、データが異なる順序の2つの異なる形式でディスクに保存されます。データを作成するときにプロジェクションを定義することもできます。以下のように自動的に維持されます。

```sql
CREATE TABLE otel_logs_v2
(
	`Body` String,
	`Timestamp` DateTime,
	`ServiceName` LowCardinality(String),
	`Status` UInt16,
	`RequestProtocol` LowCardinality(String),
	`RunTime` UInt32,
	`Size` UInt32,
	`UserAgent` String,
	`Referer` String,
	`RemoteUser` String,
	`RequestType` LowCardinality(String),
	`RequestPath` String,
	`RemoteAddress` IPv4,
	`RefererDomain` String,
	`RequestPage` String,
	`SeverityText` LowCardinality(String),
	`SeverityNumber` UInt8,
        PROJECTION status
	(
    	   SELECT Timestamp, RequestPath, Status, RemoteAddress, UserAgent
   	   ORDER BY Status
	)
)
ENGINE = MergeTree
ORDER BY (ServiceName, Timestamp)
```

重要なのは、`ALTER`を介してプロジェクションが作成された場合、その作成は非同期であり、`MATERIALIZE PROJECTION`コマンドが発行されます。この操作の進行状況は、次のクエリで確認でき、`is_done=1`を待つことができます。

```sql
SELECT parts_to_do, is_done, latest_fail_reason
FROM system.mutations
WHERE (`table` = 'otel_logs_v2') AND (command LIKE '%MATERIALIZE%')

┌─parts_to_do─┬─is_done─┬─latest_fail_reason─┐
│       	0 │   	1 │                	│
└─────────────┴─────────┴────────────────────┘

1 row in set. Elapsed: 0.008 sec.
```

上記のクエリを繰り返すと、パフォーマンスが大幅に向上したことがわかりますが、追加のストレージがかかっています（ストレージサイズと圧縮の測定方法については「テーブルサイズと圧縮の測定」を参照してください）。

```sql
SELECT Timestamp, RequestPath, Status, RemoteAddress, UserAgent
FROM otel_logs_v2
WHERE Status = 500
FORMAT `Null`

0 rows in set. Elapsed: 0.031 sec. Processed 51.42 thousand rows, 22.85 MB (1.65 million rows/s., 734.63 MB/s.)
Peak memory usage: 27.85 MiB.
```

上記の例では、以前のクエリで使用されるカラムをプロジェクションに指定しています。これにより、これらの指定されたカラムのみが、ステータスによって順序付けられてディスクに保存されます。逆に、ここで `SELECT *` を使用した場合、すべてのカラムが保存されます。これにより、（カラムの任意のサブセットを使用した）より多くのクエリがプロジェクションの恩恵を受けることができますが、追加のストレージが必要になります。ディスクスペースと圧縮を測定する方法については、「テーブルサイズと圧縮の測定」を参照してください。

## セカンダリ/データスキップインデックス

ClickHouse で主キーがどれほどチューニングされていても、いくつかのクエリは必然的に全テーブルスキャンを必要とします。これは、マテリアライズドビュー（および一部のクエリ用のプロジェクション）を使用することで軽減できますが、これには追加のメンテナンスが必要であり、ユーザーがその利用可能性を把握している必要があります。従来のリレーショナルデータベースは、セカンダリインデックスを使用してこれを解決しますが、ClickHouse のような列指向データベースでは効果がありません。代わりに、ClickHouse は「スキップ」インデックスを使用しており、これにより、データベースは一致する値がない大きなデータチャンクをスキップすることができ、クエリのパフォーマンスを大幅に向上させることができます。

デフォルトのOTelスキーマは、マップアクセスへのアクセスを加速するためにセカンダリインデックスを使用しています。一般的に効果がないと考えており、カスタムスキーマにコピーすることはお勧めしませんが、スキップインデックスは依然として有用です。

ユーザーは、適用する前に[セカンダリインデックスに関するガイド](https://en/optimize/skipping-indexes)を読み理解する必要があります。

**一般に、主キーとターゲットとなる非主キーのカラム/式の間に強い相関関係が存在する場合、かつユーザーがまれな値、つまり多くのグラニュールで発生しない値を探している場合に効果的です。**

## テキスト検索のためのブルームフィルター

観測性のクエリにおいて、セカンダリインデックスはユーザーがテキスト検索を行う必要があるときに役立ちます。具体的には、ngram およびトークンベースのブルームフィルターインデックス [`ngrambf_v1`](https://en/optimize/skipping-indexes#bloom-filter-types) および [`tokenbf_v1`](https://en/optimize/skipping-indexes#bloom-filter-types) を使用して、`LIKE` 、`IN`、および hasToken 演算子を使用して文字列カラムに対する検索を加速できます。特に、トークンベースのインデックスは、非英数字を区切りとして使用してトークンを生成します。これは、クエリ時にトークン（全体の単語）のみが一致することを意味します。より細かい一致には、[N-gramブルームフィルター](https://en/optimize/skipping-indexes#bloom-filter-types)を使用できます。これにより、指定されたサイズのn-gramに文字列を分割し、部分語の一致が可能になります。

生成されるトークンを評価するため、したがって一致するトークンを評価するために、`tokens` 関数を使用できます：

```sql
SELECT tokens('https://www.zanbil.ir/m/filter/b113')

┌─tokens────────────────────────────────────────────┐
│ ['https','www','zanbil','ir','m','filter','b113'] │
└───────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.008 sec.
```

`ngram` 関数も同様の機能を提供します。具体的には、n-gramサイズを第二のパラメータとして指定できます：

```sql
SELECT ngrams('https://www.zanbil.ir/m/filter/b113', 3)

┌─ngrams('https://www.zanbil.ir/m/filter/b113', 3)────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ ['htt','ttp','tps','ps:','s:/','://','//w','/ww','www','ww.','w.z','.za','zan','anb','nbi','bil','il.','l.i','.ir','ir/','r/m','/m/','m/f','/fi','fil','ilt','lte','ter','er/','r/b','/b1','b11','113'] │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.008 sec.
```

> ClickHouse には、セカンダリインデックスとしての逆インデックスのエクスペリメンタルサポートもあります。これは現在、ログデータセットには推奨されていませんが、製品版に準備が整った場合にトークンベースのブルームフィルターに取って代わることを期待しています。

例の目的のためには、構造化ログデータセットを使用します。たとえば、`Referer` カラムに `ultra` を含むログをカウントしたいとします。

```sql
SELECT count()
FROM otel_logs_v2
WHERE Referer LIKE '%ultra%'

┌─count()─┐
│  114514 │
└─────────┘

1 row in set. Elapsed: 0.177 sec. Processed 10.37 million rows, 908.49 MB (58.57 million rows/s., 5.13 GB/s.)
```

ここでは、n-gramサイズを3で一致させる必要があります。したがって、`ngrambf_v1` インデックスを作成します。 

```sql
CREATE TABLE otel_logs_bloom
(
	`Body` String,
	`Timestamp` DateTime,
	`ServiceName` LowCardinality(String),
	`Status` UInt16,
	`RequestProtocol` LowCardinality(String),
	`RunTime` UInt32,
	`Size` UInt32,
	`UserAgent` String,
	`Referer` String,
	`RemoteUser` String,
	`RequestType` LowCardinality(String),
	`RequestPath` String,
	`RemoteAddress` IPv4,
	`RefererDomain` String,
	`RequestPage` String,
	`SeverityText` LowCardinality(String),
	`SeverityNumber` UInt8,
	INDEX idx_span_attr_value Referer TYPE ngrambf_v1(3, 10000, 3, 7) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (Timestamp)
```

インデックス `ngrambf_v1(3, 10000, 3, 7)` は、ここで4つのパラメータを取ります。最後の7はシードを表します。その他は、n-gram サイズ (3)、値 `m` (フィルターサイズ)、およびハッシュ関数の数 `k` (7) を表します。 `k` と `m` は調整が必要であり、ユニークな n-gram / トークンの数と、フィルターが偽の負を返す確率に基づいています - すなわち、値がグラニュールに存在しないことを確認します。これらの値を特定するのに役立つ推奨[関数](https://en/engines/table-engines/mergetree-family/mergetree#bloom-filter)があります。

適切にチューニングされれば、ここでのスピードアップは顕著です：

```sql
SELECT count()
FROM otel_logs_bloom
WHERE Referer LIKE '%ultra%'
┌─count()─┐
│ 	182 │
└─────────┘

1 row in set. Elapsed: 0.077 sec. Processed 4.22 million rows, 375.29 MB (54.81 million rows/s., 4.87 GB/s.)
Peak memory usage: 129.60 KiB.
```

> 上記は例示的な目的のためです。ユーザーは、テキスト検索を最適化しようとするのではなく、挿入時にログから構造を抽出することをお勧めします。ただし、テキスト検索が役立つケースもあります。たとえば、スタックトレースや他の大きな文字列など、構造があまり決定的でない場合です。

ブルームフィルターを使用する際の一般的なガイドライン：

ブルームの目的は[グラニュール](https://en/optimize/sparse-primary-indexes#clickhouse-index-design)をフィルタリングし、カラムのすべての値を読み込んで線形スキャンを回避することです。`EXPLAIN`句を使用し、`indexes=1`パラメータを指定することで、スキップされたグラニュールの数を特定できます。以下は、元のテーブル `otel_logs_v2` と、ngrambf を持つ `otel_logs_bloom` テーブルのレスポンスを示します。

```sql
EXPLAIN indexes = 1
SELECT count()
FROM otel_logs_v2
WHERE Referer LIKE '%ultra%'

┌─explain────────────────────────────────────────────────────────────┐
│ Expression ((Project names + Projection))                      	│
│   Aggregating                                                  	│
│ 	Expression (Before GROUP BY)                               	│
│   	Filter ((WHERE + Change column names to column identifiers)) │
│     	ReadFromMergeTree (default.otel_logs_v2)               	│
│     	Indexes:                                               	│
│       	PrimaryKey                                           	│
│         	Condition: true                                    	│
│         	Parts: 9/9                                         	│
│         	Granules: 1278/1278                                	│
└────────────────────────────────────────────────────────────────────┘

10 rows in set. Elapsed: 0.016 sec.


EXPLAIN indexes = 1
SELECT count()
FROM otel_logs_bloom
WHERE Referer LIKE '%ultra%'

┌─explain────────────────────────────────────────────────────────────┐
│ Expression ((Project names + Projection))                      	│
│   Aggregating                                                  	│
│ 	Expression (Before GROUP BY)                               	│
│   	Filter ((WHERE + Change column names to column identifiers)) │
│     	ReadFromMergeTree (default.otel_logs_bloom)            	│
│     	Indexes:                                               	│
│       	PrimaryKey                                           	│
│         	Condition: true                                    	│
│         	Parts: 8/8                                         	│
│         	Granules: 1276/1276                                	│
│       	Skip                                                 	│
│         	Name: idx_span_attr_value                          	│
│         	Description: ngrambf_v1 GRANULARITY 1              	│
│         	Parts: 8/8                                         	│
│         	Granules: 517/1276                                 	│
└────────────────────────────────────────────────────────────────────┘
```

ブルームフィルターは、通常、列自体よりも小さい場合にのみ高速化されます。大きければ、パフォーマンスの利点はほとんどないでしょう。以下のクエリを使用して、フィルターと列のサイズを比較してください：

```sql
SELECT
	name,
	formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
	formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
	round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.columns
WHERE (`table` = 'otel_logs_bloom') AND (name = 'Referer')
GROUP BY name
ORDER BY sum(data_compressed_bytes) DESC

┌─name────┬─compressed_size─┬─uncompressed_size─┬─ratio─┐
│ Referer │ 56.16 MiB   	│ 789.21 MiB    	│ 14.05 │
└─────────┴─────────────────┴───────────────────┴───────┘

1 row in set. Elapsed: 0.018 sec.


SELECT
	`table`,
	formatReadableSize(data_compressed_bytes) AS compressed_size,
	formatReadableSize(data_uncompressed_bytes) AS uncompressed_size
FROM system.data_skipping_indices
WHERE `table` = 'otel_logs_bloom'

┌─table───────────┬─compressed_size─┬─uncompressed_size─┐
│ otel_logs_bloom │ 12.03 MiB   	│ 12.17 MiB     	│
└─────────────────┴─────────────────┴───────────────────┘

1 row in set. Elapsed: 0.004 sec.
```

上記の例では、セカンダリブルームフィルターインデックスが12MBで、列自体の圧縮サイズである56MBの約5倍小さいことがわかります。

ブルームフィルターは、重要なチューニングが必要です。最適な設定を特定するのに役立つメモを[こちらに](https://en/engines/table-engines/mergetree-family/mergetree#bloom-filter)従うことをお勧めします。ブルームフィルターは、挿入およびマージ時にも高価になる可能性があります。製品前にブルームフィルターを追加する前に、挿入パフォーマンスへの影響を評価する必要があります。

セカンダリスキップインデックスに関する詳細は[こちら](https://en/optimize/skipping-indexes#skip-index-functions)にあります。

## マップからの抽出

Map 型は OTel スキーマにおいて広く使用されています。このタイプのキーと値は同じ型である必要があります - これは Kubernetes ラベルなどのメタデータには十分です。ここで注意すべきは、Map 型のサブキーをクエリする際に、親カラム全体が読み込まれることです。Map に多くのキーがある場合、これは、キーがカラムとして存在する場合に読み取るよりも多くのデータがディスクから読み取られるため、クエリに重要なペナルティを課すことがあります。

特定のキーを頻繁にクエリする場合は、それをルートに専用のカラムに移動することを検討してください。これは通常、一般的なアクセスパターンへの反応として、デプロイ後に発生するタスクであり、製品前に予測することは難しいかもしれません。「スキーマの進化」を参照して、デプロイ後にスキーマを変更する方法を確認してください。

## テーブルサイズと圧縮の測定

ClickHouse が観測性のために使用される主な理由の一つは、圧縮です。

ストレージコストを大幅に削減するだけでなく、ディスク上のデータが少ないということは、I/O が少なくなり、クエリや挿入が速くなります。I/O の削減は、CPUの観点からいかなる圧縮アルゴリズムのオーバーヘッドに対しても、上回るべきです。したがって、データの圧縮を改善することが、ClickHouse のクエリが速いことを確保する際の最初の焦点であるべきです。

圧縮を測定する方法の詳細は[こちら](https://en/data-compression/compression-in-clickhouse)にあります。
