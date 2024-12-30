# processors_profile_log

このテーブルはプロセッサーレベルのプロファイリングを含んでいます（[`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md#explain-pipeline)で確認できます）。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行しているサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — イベントが発生した日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — イベントが発生した日時。
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — イベントが発生した日時（マイクロ秒精度）。
- `id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — プロセッサーのID。
- `parent_ids` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — 親プロセッサーのID。
- `plan_step` ([UInt64](../../sql-reference/data-types/int-uint.md)) — このプロセッサーを作成したクエリプランステップのID。プロセッサーが任意のステップから追加されていない場合はゼロ。
- `plan_group` ([UInt64](../../sql-reference/data-types/int-uint.md)) — クエリプランステップによって作成された場合のプロセッサーのグループ。同じクエリプランステップから追加されたプロセッサーの論理的な区分で、美化されたEXPLAIN PIPELINEの結果に使用される。
- `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — 初期クエリのID（分散クエリ実行のため）。
- `query_id` ([String](../../sql-reference/data-types/string.md)) — クエリのID。
- `name` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — プロセッサーの名前。
- `elapsed_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — このプロセッサーが実行されたマイクロ秒の数。
- `input_wait_elapsed_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 他のプロセッサーからのデータを待機していたマイクロ秒の数。
- `output_wait_elapsed_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 出力ポートがいっぱいのため待機していたマイクロ秒の数。
- `input_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — プロセッサーによって消費された行の数。
- `input_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — プロセッサーによって消費されたバイト数。
- `output_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — プロセッサーによって生成された行の数。
- `output_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — プロセッサーによって生成されたバイト数。

**例**

クエリ:

``` sql
EXPLAIN PIPELINE
SELECT sleep(1)
┌─explain─────────────────────────┐
│ (Expression)                    │
│ ExpressionTransform             │
│   (SettingQuotaAndLimits)       │
│     (ReadFromStorage)           │
│     SourceFromSingleChunk 0 → 1 │
└─────────────────────────────────┘

SELECT sleep(1)
SETTINGS log_processors_profiles = 1
Query id: feb5ed16-1c24-4227-aa54-78c02b3b27d4
┌─sleep(1)─┐
│        0 │
└──────────┘
1 rows in set. Elapsed: 1.018 sec.

SELECT
    name,
    elapsed_us,
    input_wait_elapsed_us,
    output_wait_elapsed_us
FROM system.processors_profile_log
WHERE query_id = 'feb5ed16-1c24-4227-aa54-78c02b3b27d4'
ORDER BY name ASC
```

結果:

``` text
┌─name────────────────────┬─elapsed_us─┬─input_wait_elapsed_us─┬─output_wait_elapsed_us─┐
│ ExpressionTransform     │    1000497 │                  2823 │                    197 │
│ LazyOutputFormat        │         36 │               1002188 │                      0 │
│ LimitsCheckingTransform │         10 │               1002994 │                    106 │
│ NullSource              │          5 │               1002074 │                      0 │
│ NullSource              │          1 │               1002084 │                      0 │
│ SourceFromSingleChunk   │         45 │                  4736 │                1000819 │
└─────────────────────────┴────────────┴───────────────────────┴────────────────────────┘
```

ここで見ることができるのは:

- `ExpressionTransform` は `sleep(1)` 関数を実行しており、そのため `work` は1e6を取り、`elapsed_us` > 1e6。
- `SourceFromSingleChunk` は待機が必要で、`ExpressionTransform` は `sleep(1)` の実行中にデータを受け入れないため、`PortFull` 状態で1e6 us待機する必要があり、`output_wait_elapsed_us` > 1e6。
- `LimitsCheckingTransform`/`NullSource`/`LazyOutputFormat` は、`ExpressionTransform` が `sleep(1)` を実行して結果を処理するまで待機する必要があるため、`input_wait_elapsed_us` > 1e6。

**参照**

- [`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md#explain-pipeline)
