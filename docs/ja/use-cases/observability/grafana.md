---
title: Grafanaの使用
description: GrafanaとClickHouseを使用したオブザーバビリティ
slug: /ja/observability/grafana
keywords: [observability, logs, traces, metrics, OpenTelemetry, Grafana, otel]
---

# GrafanaとClickHouseを使用したオブザーバビリティ

Grafanaは、ClickHouseにおけるオブザーバビリティデータの視覚化ツールとして推奨されています。これは、Grafana用の公式ClickHouseプラグインを使用して実現します。インストール手順は[こちら](/ja/integrations/grafana)で参照できます。

プラグインのバージョン4では、ログとトレースが新しいクエリビルダーエクスペリエンスにおいて第一級の存在となっています。これにより、SREがSQLクエリを書かずに済む環境を提供し、SQLベースのオブザーバビリティを簡素化し、この新しいパラダイムを推進します。この一環として、Open Telemetry (OTel) をプラグインの中核に据え、今後数年間のSQLベースのオブザーバビリティの基盤になると考えています。

## Open Telemetry統合

GrafanaでClickhouseデータソースを設定する際、ユーザーはログおよびトレース用のデフォルトのデータベースとテーブルを指定し、これらのテーブルがOTelスキーマに準拠しているかどうかを指定できます。これにより、プラグインはGrafanaでログとトレースを正確に表示するためのカラムを返します。デフォルトのOTelスキーマを変更して独自のカラム名を使用する場合は、これらを指定することができます。時刻（Timestamp）、ログレベル（SeverityText）、メッセージ本文（Body）などのカラム名をデフォルトのOTelカラム名のままで使用する場合、変更は不要です。

> ユーザーは、HTTPまたはネイティブプロトコルを使用してGrafanaをClickHouseに接続できます。後者は、Grafanaユーザーが発行する集約クエリで顕著な性能向上を提供することはほとんどありませんが、HTTPプロトコルは通常ユーザーにとってプロキシや内省が簡単です。

ログ設定には、ログを正しく表示するために、時刻、ログレベル、およびメッセージカラムが必要です。

トレース設定はもう少し複雑です（完全なリストは[こちら](/ja/engines/table-engines/mergetree-family/mergetree#mergetree-data-storage)）。ここで必要なカラムは、その後にフルトレースプロファイルを構築するためのクエリを抽象化するために必要です。これらのクエリは、データがOTelと類似した構造を持っていることを前提としているため、標準スキーマから大きく逸脱しているユーザーは、この機能の恩恵を受けるためにビューを使用する必要があります。

<img src={require('./images/observability-15.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '300px'}} />

<br />

設定が完了すると、ユーザーは[Grafana Explore](https://grafana.com/docs/grafana/latest/explore/)に移動してログとトレースの検索を開始できます。

## ログ

Grafanaのログに関する要件に従う場合、クエリビルダーで `Query Type: Log` を選択して `Run Query` をクリックできます。クエリビルダーは、ログをリストして表示するためのクエリを作成します。例えば次のように：

```sql
SELECT Timestamp as timestamp, Body as body, SeverityText as level, TraceId as traceID FROM "default"."otel_logs" WHERE ( timestamp >= $__fromTime AND timestamp <= $__toTime ) ORDER BY timestamp DESC LIMIT 1000
```

<img src={require('./images/observability-16.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

クエリビルダーは、ユーザーがSQLを書く必要を避ける簡単な手段を提供します。キーワードを含むログを見つけることを含むフィルタリングは、クエリビルダーで行えます。より複雑なクエリを書くことを望むユーザーは、SQLエディタに切り替えることができます。適切なカラムが返され、`logs` がクエリタイプとして選択されている限り、結果はログとしてレンダリングされます。ログのレンダリングに必要なカラムは[こちら](https://grafana.com/developers/plugin-tools/tutorials/build-a-logs-data-source-plugin#logs-data-frame-format)に記載されています。

### トレースへのログ

ログがトレースIDを含む場合、特定のログ行からトレースを閲覧することができます。

<img src={require('./images/observability-17.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

## トレース

前述のログ体験と同様に、Grafanaがトレースをレンダリングするために必要なカラムが満たされている場合（例えば、OTelスキーマを使用している場合）、クエリビルダーは必要なクエリを自動的に作成することができます。`Query Type: Traces` を選択し、`Run Query` をクリックすると、以下のようなクエリが生成され実行されます（設定されたカラムに依存しており、以下はOTelを使用することを前提としています）：

```sql
SELECT "TraceId" as traceID,
  "ServiceName" as serviceName,
  "SpanName" as operationName,
  "Timestamp" as startTime,
  multiply("Duration", 0.000001) as duration
FROM "default"."otel_traces"
WHERE ( Timestamp >= $__fromTime AND Timestamp <= $__toTime )
  AND ( ParentSpanId = '' )
  AND ( Duration > 0 )
  ORDER BY Timestamp DESC, Duration DESC LIMIT 1000
```

このクエリはGrafanaが期待するカラム名を返し、以下に示すようにトレースのテーブルをレンダリングします。継続時間や他のカラムを基にしたフィルタリングも、SQLを書くことなく実行できます。

<img src={require('./images/observability-18.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

より複雑なクエリを書きたいユーザーは、`SQL Editor`に切り替えることができます。

### トレース詳細の表示

上に示したように、トレースIDはクリック可能なリンクとして表示されます。トレースIDをクリックすると、リンク`View Trace`を通じて関連するスパンの表示を選択できます。これにより、スパンを必要な構造で取得し、結果をウォーターフォールとしてレンダリングするための以下のクエリ（OTelカラムを想定）が発行されます。

```sql
WITH '<trace_id>' as trace_id,
  (SELECT min(Start) FROM "default"."otel_traces_trace_id_ts"
    WHERE TraceId = trace_id) as trace_start,
  (SELECT max(End) + 1 FROM "default"."otel_traces_trace_id_ts"
    WHERE TraceId = trace_id) as trace_end
SELECT "TraceId" as traceID,
  "SpanId" as spanID,
  "ParentSpanId" as parentSpanID,
  "ServiceName" as serviceName,
  "SpanName" as operationName,
  "Timestamp" as startTime,
  multiply("Duration", 0.000001) as duration,
  arrayMap(key -> map('key', key, 'value',"SpanAttributes"[key]),
  mapKeys("SpanAttributes")) as tags,
  arrayMap(key -> map('key', key, 'value',"ResourceAttributes"[key]),
  mapKeys("ResourceAttributes")) as serviceTags
FROM "default"."otel_traces"
WHERE traceID = trace_id
  AND startTime >= trace_start
  AND startTime <= trace_end
LIMIT 1000
```

> 上記クエリがトレースIDのルックアップを行うためにMaterialized View `otel_traces_trace_id_ts` を使用する方法に注目してください。詳細は「クエリの高速化 - ルックアップ用のMaterialized Viewsの活用」を参照してください。

<img src={require('./images/observability-19.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

### トレースからログへ

ログがトレースIDを含む場合、ユーザーはトレースから関連するログへナビゲートできます。ログを表示するにはトレースIDをクリックし、`View Logs`を選択します。この操作は、デフォルトのOTelカラムを想定した以下のクエリを発行します。

```sql
SELECT Timestamp as "timestamp",
  Body as "body", SeverityText as "level",
  TraceId as "traceID" FROM "default"."otel_logs"
WHERE ( traceID = '<trace_id>' )
ORDER BY timestamp ASC LIMIT 1000
```

<img src={require('./images/observability-20.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

## ダッシュボード

GrafanaでClickHouseデータソースを使用してダッシュボードを構築できます。詳細については、GrafanaとClickHouseの[データソースドキュメント](https://github.com/grafana/clickhouse-datasource)を参考にすることをお勧めします。特に[マクロ](https://github.com/grafana/clickhouse-datasource?tab=readme-ov-file#macros)と[変数](https://grafana.com/docs/grafana/latest/dashboards/variables/)の概念に焦点を当ててください。

プラグインは、OTelの仕様に準拠したログおよびトレースデータ用の例となるダッシュボード「Simple ClickHouse OTel dashboarding」を含む、いくつかの標準ダッシュボードを提供します。これは、ユーザーがOTelのデフォルトカラム名に準拠することを要求し、データソースの設定からインストールできます。

<img src={require('./images/observability-21.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

以下に、可視化のためのいくつかの簡単なヒントを提供します。

### 時系列

統計とともに、線グラフはオブザーバビリティの使用シーンで最も一般的な視覚化形式です。Clickhouseプラグインは、`datetime` に名前付けされた `time` と数値カラムを返すクエリの場合、自動的に線グラフをレンダリングします。例えば：

```sql
SELECT
 $__timeInterval(Timestamp) as time,
 quantile(0.99)(Duration)/1000000 AS p99
FROM otel_traces
WHERE
 $__timeFilter(Timestamp)
 AND ( Timestamp  >= $__fromTime AND Timestamp <= $__toTime )
GROUP BY time
ORDER BY time ASC
LIMIT 100000
```

<img src={require('./images/observability-22.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

### 複数ラインのチャート

以下の条件が満たされる場合、クエリのために複数ラインのチャートが自動的にレンダリングされます：

- フィールド1: エイリアス名がtimeのdatetimeフィールド
- フィールド2: グループ化するための値。これはStringでなければなりません。
- フィールド3以降: メトリック値
 
例えば：

```sql
SELECT
  $__timeInterval(Timestamp) as time,
  ServiceName,
  quantile(0.99)(Duration)/1000000 AS p99
FROM otel_traces
WHERE $__timeFilter(Timestamp)
AND ( Timestamp  >= $__fromTime AND Timestamp <= $__toTime )
GROUP BY ServiceName, time
ORDER BY time ASC
LIMIT 100000
```

<img src={require('./images/observability-23.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />

### 地理データの可視化

以前のセクションでIP Dictionaryを使用して地理座標でオブザーバビリティデータを拡張する方法を探しました。`latitude` と `longitude` のカラムがあると仮定し、オブザーバビリティを `geohashEncode` 関数を使用して可視化できます。これにより、GrafanaのGeo Mapチャートと互換性のあるジオハッシュが生成されます。以下に例のクエリと可視化を示します：

```sql
WITH coords AS
	(
    	SELECT
        	Latitude,
        	Longitude,
        	geohashEncode(Longitude, Latitude, 4) AS hash
    	FROM otel_logs_v2
    	WHERE (Longitude != 0) AND (Latitude != 0)
	)
SELECT
	hash,
	count() AS heat,
	round(log10(heat), 2) AS adj_heat
FROM coords
GROUP BY hash
```

<img src={require('./images/observability-24.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px'}} />

<br />
