Sentry 迁移

## 1. 确认数据量
```sql
SELECT
    concat(database, '.', table) AS table,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts_all
WHERE active AND (database = 'sentry_default')
GROUP BY
    table,
    database;

---Result

┌─table────────────────────────────────────────────────────────────────┬─size───────┐
│ sentry_default.sessions_hourly_local                                 │ 961.51 MiB │ done
│ sentry_default.generic_metric_sets_local_replicated_local_replicated │ 116.33 GiB │
│ sentry_default.generic_metric_sets_raw_local                         │ 577.42 MiB │ done
│ sentry_default.outcomes_raw_local                                    │ 12.10 GiB  │ done
│ sentry_default.generic_metric_distributions_raw_local                │ 31.24 GiB  │ done
│ sentry_default.transactions_local                                    │ 846.62 GiB │
│ sentry_default.generic_metric_sets_local                             │ 25.37 GiB  │
│ sentry_default.transactions_local_replicated                         │ 484.02 MiB │
│ sentry_default.metrics_sets_v2_local                                 │ 3.22 GiB   │
│ sentry_default.generic_metric_sets_local_replicated                  │ 9.52 MiB   │
│ sentry_default.sessions_raw_local                                    │ 31.43 GiB  │
│ sentry_default.migrations_local                                      │ 5.76 KiB   │
│ sentry_default.spans_local_replicated                                │ 2.00 GiB   │
│ sentry_default.spans_local                                           │ 3.02 TiB   │
│ sentry_default.metrics_raw_v2_local                                  │ 631.27 MiB │
│ sentry_default.errors_local                                          │ 217.18 GiB │
│ sentry_default.errors_local_replicated                               │ 136.67 MiB │
│ sentry_default.generic_metric_distributions_aggregated_local         │ 601.36 GiB │
│ sentry_default.outcomes_hourly_local                                 │ 13.81 MiB  │
│ sentry_default.metrics_counters_v2_local                             │ 461.73 MiB │
│ sentry_default.replays_local                                         │ 12.49 GiB  │
└──────────────────────────────────────────────────────────────────────┴────────────┘

```
这就确定了是这21张表有数据  

## 2. 按照表进行迁移

- sessions_hourly_local

nohup clickhouse-client -udefault --password cSnb92Ajb9a3N -mn --query="insert into sentry_default.sessions_hourly_local select * from remote('10.171.175.105','sentry_default', 'sessions_hourly_dist', 'default', 'cSnb92Ajb9a3N') where started  > toDate('2024-05-11') - Interval 15 Day;" > sessions_hourly_dist 2>&1 &

- generic_metric_sets_raw_local 

nohup clickhouse-client -udefault --password cSnb92Ajb9a3N -mn --query="insert into sentry_default.generic_metric_sets_raw_local select * from remote('10.171.175.105','sentry_default', 'generic_metric_sets_raw_dist', 'default', 'cSnb92Ajb9a3N') where timestamp  > toDate('2024-05-11') - Interval 15 Day;" > generic_metric_sets_raw_local 2>&1 &

- outcomes_raw_local
nohup clickhouse-client -udefault --password cSnb92Ajb9a3N -mn --query="insert into sentry_default.outcomes_raw_local select * from remote('10.171.175.105','sentry_default', 'outcomes_raw_dist', 'default', 'cSnb92Ajb9a3N') where timestamp > toDate('2024-05-11') - Interval 15 Day;" > outcomes_raw_dist 2>&1 & 

- generic_metric_distributions_raw_local
nohup clickhouse-client -udefault --password cSnb92Ajb9a3N -mn --query="insert into sentry_default.generic_metric_distributions_raw_local select * from remote('10.171.175.105','sentry_default', 'generic_metric_distributions_raw_dist', 'default', 'cSnb92Ajb9a3N') where timestamp > toDate('2024-05-11') - Interval 15 Day;" > generic_metric_distributions_raw_local 2>&1 & 

- transactions_local
nohup clickhouse-client -udefault --password cSnb92Ajb9a3N -mn --query="insert into sentry_default.transactions_local select project_id,event_id,trace_id,span_id,transaction_name,transaction_op,transaction_status,start_ts,start_ms,finish_ts,finish_ms,duration,platform,environment,release,dist,sdk_name,sdk_version,http_method,http_referer,ip_address_v4,ip_address_v6,user,user_id,user_name,user_email,tags.key,tags.value,contexts.key,contexts.value,_contexts_flattened,measurements.key,measurements.value,span_op_breakdowns.key,span_op_breakdowns.value,spans.op,spans.group,spans.exclusive_time_32,spans.exclusive_time,partition,offset,message_timestamp,retention_days,deleted,group_ids,app_start_type,profile_id,replay_id,transaction_source from remote('10.171.175.105','sentry_default', 'transactions_dist', 'default', 'cSnb92Ajb9a3N') where finish_ts > toDate('2024-05-11') - Interval 15 Day;" > generic_metric_distributions_raw_local 2>&1 & 