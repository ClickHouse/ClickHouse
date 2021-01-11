# system.table_engines {#system-table-engines}

Содержит информацию про движки таблиц, поддерживаемые сервером, а также об их возможностях.

Эта таблица содержит следующие столбцы (тип столбца показан в скобках):

-   `name` (String) — имя движка.
-   `supports_settings` (UInt8) — флаг, показывающий поддержку секции `SETTINGS`.
-   `supports_skipping_indices` (UInt8) — флаг, показывающий поддержку [индексов пропуска данных](table_engines/mergetree/#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — флаг, показывающий поддержку [TTL](table_engines/mergetree/#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — флаг, показывающий поддержку секций `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` и `SAMPLE_BY`.
-   `supports_replication` (UInt8) — флаг, показывающий поддержку [репликации](../../engines/table-engines/mergetree-family/replication.md).
-   `supports_deduplication` (UInt8) — флаг, показывающий наличие в движке дедупликации данных.

Пример:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**Смотрите также**

-   [Секции движка](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses) семейства MergeTree
-   [Настройки](../../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table) Kafka
-   [Настройки](../../engines/table-engines/special/join.md#join-limitations-and-settings) Join

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/table_engines) <!--hide-->
