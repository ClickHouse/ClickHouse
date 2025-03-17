---
slug: /ja/operations/system-tables/asynchronous_metric_log
---
# asynchronous_metric_log

`system.asynchronous_metrics`の履歴値を保持し、一定の時間間隔で（一秒ごとにデフォルト設定）保存します。デフォルトで有効になっています。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行するサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — イベントの日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — イベントの時刻。
- `metric` ([String](../../sql-reference/data-types/string.md)) — メトリクスの名前。
- `value` ([Float64](../../sql-reference/data-types/float.md)) — メトリクスの値。

**例**

``` sql
SELECT * FROM system.asynchronous_metric_log LIMIT 3 \G
```

``` text
Row 1:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:07
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
value:      0.001

Row 2:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:08
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
value:      0

Row 3:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:09
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
value:      0
```

**関連項目**

- [system.asynchronous_metrics](../system-tables/asynchronous_metrics.md) — バックグラウンドで定期的に計算されるメトリクスを含む。
- [system.metric_log](../system-tables/metric_log.md) — テーブル`system.metrics`と`system.events`からのメトリクス値の履歴を保持し、定期的にディスクに書き込まれる。
