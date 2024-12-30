---
slug: /ja/operations/system-tables/opentelemetry_span_log
---
# opentelemetry_span_log

クエリが実行された際の[トレーススパン](https://opentracing.io/docs/overview/spans/)に関する情報を含みます。

カラム:

- `trace_id` ([UUID](../../sql-reference/data-types/uuid.md)) — 実行されたクエリのトレースID。
- `span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `トレーススパン`のID。
- `parent_span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 親の`トレーススパン`のID。
- `operation_name` ([String](../../sql-reference/data-types/string.md)) — 操作の名前。
- `kind` ([Enum8](../../sql-reference/data-types/enum.md)) — スパンの[SpanKind](https://opentelemetry.io/docs/reference/specification/trace/api/#spankind)。
    - `INTERNAL` — スパンがアプリケーション内の内部操作を表していることを示します。
    - `SERVER` — スパンが同期RPCまたは他のリモートリクエストのサーバー側処理をカバーしていることを示します。
    - `CLIENT` — スパンが何らかのリモートサービスへのリクエストを記述していることを示します。
    - `PRODUCER` — スパンが非同期リクエストの開始を記述していることを示します。この親スパンは、しばしば対応する子CONSUMERスパンが始まる前に終了することがあります。
    - `CONSUMER` - スパンが非同期PRODUCERリクエストの子を記述していることを示します。
- `start_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `トレーススパン`の開始時間（マイクロ秒単位）。
- `finish_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `トレーススパン`の終了時間（マイクロ秒単位）。
- `finish_date` ([Date](../../sql-reference/data-types/date.md)) — `トレーススパン`の終了日。
- `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — `トレーススパン`に依存する[属性](https://opentelemetry.io/docs/go/instrumentation/#attributes)名。`OpenTelemetry`標準の推奨に従って記入されます。
- `attribute.values` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — `トレーススパン`に依存する属性値。`OpenTelemetry`標準の推奨に従って記入されます。

**例**

クエリ:

``` sql
SELECT * FROM system.opentelemetry_span_log LIMIT 1 FORMAT Vertical;
```

結果:

``` text
Row 1:
──────
trace_id:         cdab0847-0d62-61d5-4d38-dd65b19a1914
span_id:          701487461015578150
parent_span_id:   2991972114672045096
operation_name:   DB::Block DB::InterpreterSelectQuery::getSampleBlockImpl()
kind:             INTERNAL
start_time_us:    1612374594529090
finish_time_us:   1612374594529108
finish_date:      2021-02-03
attribute.names:  []
attribute.values: []
```

**関連情報**

- [OpenTelemetry](../../operations/opentelemetry.md)
