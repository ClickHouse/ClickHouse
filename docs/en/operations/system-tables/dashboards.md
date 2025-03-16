---
slug: /en/operations/system-tables/dashboards
---
# dashboards

Contains queries used by `/dashboard` page accessible though [HTTP interface](/docs/en/interfaces/http.md).
This table can be useful for monitoring and troubleshooting. The table contains a row for every chart in a dashboard.

:::note
`/dashboard` page can render queries not only from `system.dashboards`, but from any table with the same schema.
This can be useful to create custom dashboards.
:::

Example:

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

Columns:

- `dashboard` (`String`) - The dashboard name.
- `title` (`String`) - The title of a chart.
- `query` (`String`) - The query to obtain data to be displayed.
