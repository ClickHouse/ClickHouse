---
slug: /ja/operations/system-tables/dashboards
---
# dashboards

[HTTPインターフェース](/docs/ja/interfaces/http.md)を介してアクセス可能な`/dashboard`ページで使用されるクエリを含んでいます。このテーブルは、監視やトラブルシューティングに役立ちます。ダッシュボード内の各チャートに対して行が含まれています。

:::note
`/dashboard`ページは`system.dashboards`だけでなく、同じスキーマを持つ任意のテーブルからクエリをレンダリングできます。これにより、カスタムダッシュボードを作成することができます。
:::

例:

``` sql
SELECT *
FROM system.dashboards
WHERE title ILIKE '%CPU%'
```

``` text
Row 1:
──────
dashboard: overview
title:     CPU Usage (cores)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000
FROM system.metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 2:
──────
dashboard: overview
title:     CPU Wait
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000
FROM system.metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 3:
──────
dashboard: overview
title:     OS CPU Usage (Userspace)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM system.asynchronous_metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 4:
──────
dashboard: overview
title:     OS CPU Usage (Kernel)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM system.asynchronous_metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
```

カラム:

- `dashboard` (`String`) - ダッシュボード名。
- `title` (`String`) - チャートのタイトル。
- `query` (`String`) - 表示するデータを取得するためのクエリ。
