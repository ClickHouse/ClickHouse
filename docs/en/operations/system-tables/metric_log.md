# system.metric\_log {#system_tables-metric_log}

Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.
To turn on metrics history collection on `system.metric_log`, create `/etc/clickhouse-server/config.d/metric_log.xml` with following content:

``` xml
<yandex>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</yandex>
```

**Example**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:                                                 2020-02-18
event_time:                                                 2020-02-18 07:15:33
milliseconds:                                               554
ProfileEvent_Query:                                         0
ProfileEvent_SelectQuery:                                   0
ProfileEvent_InsertQuery:                                   0
ProfileEvent_FileOpen:                                      0
ProfileEvent_Seek:                                          0
ProfileEvent_ReadBufferFromFileDescriptorRead:              1
ProfileEvent_ReadBufferFromFileDescriptorReadFailed:        0
ProfileEvent_ReadBufferFromFileDescriptorReadBytes:         0
ProfileEvent_WriteBufferFromFileDescriptorWrite:            1
ProfileEvent_WriteBufferFromFileDescriptorWriteFailed:      0
ProfileEvent_WriteBufferFromFileDescriptorWriteBytes:       56
...
CurrentMetric_Query:                                        0
CurrentMetric_Merge:                                        0
CurrentMetric_PartMutation:                                 0
CurrentMetric_ReplicatedFetch:                              0
CurrentMetric_ReplicatedSend:                               0
CurrentMetric_ReplicatedChecks:                             0
...
```

**See also**

-   [system.asynchronous\_metrics](../../operations/system-tables/asynchronous_metrics.md) — Contains periodically calculated metrics.
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that occurred.
-   [system.metrics](../../operations/system-tables/metrics.md) — Contains instantly calculated metrics.
-   [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
