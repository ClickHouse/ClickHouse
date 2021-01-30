# system.metric_log {#system_tables-metric_log}

Содержит историю значений метрик из таблиц `system.metrics` и `system.events`, периодически сбрасываемую на диск.

Столбцы:
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата события.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время события.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время события в микросекундах.

**Пример**

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

**Смотрите также**

-   [Настройка metric_log](../../operations/server-configuration-parameters/settings.md#metric_log) — как включить и выключить запись истории.
-   [system.asynchronous_metrics](#system_tables-asynchronous_metrics) — таблица с периодически вычисляемыми метриками.
-   [system.events](#system_tables-events) — таблица с количеством произошедших событий.
-   [system.metrics](#system_tables-metrics) — таблица с мгновенно вычисляемыми метриками.
-   [Мониторинг](../../operations/monitoring.md) — основы мониторинга в ClickHouse.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/metric_log) <!--hide-->
