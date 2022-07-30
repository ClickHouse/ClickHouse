---
sidebar_position: 52
sidebar_label: "Системные таблицы"
---

# Системные таблицы {#system-tables}

## Введение {#system-tables-introduction}

Системные таблицы содержат информацию о:

-   состоянии сервера, процессов и окружении.
-   внутренних процессах сервера.

Системные таблицы:

-   находятся в базе данных `system`.
-   доступны только для чтения данных.
-   не могут быть удалены или изменены, но их можно отсоединить.

Большинство системных таблиц хранят свои данные в оперативной памяти. Сервер ClickHouse создает эти системные таблицы при старте.

В отличие от других системных таблиц, таблицы с системными логами [metric_log](../../operations/system-tables/metric_log.md), [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), [trace_log](../../operations/system-tables/trace_log.md), [part_log](../../operations/system-tables/part_log.md), [crash_log](../../operations/system-tables/crash-log.md) и [text_log](../../operations/system-tables/text_log.md) используют движок таблиц [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) и по умолчанию хранят свои данные в файловой системе. Если удалить таблицу из файловой системы, сервер ClickHouse снова создаст пустую таблицу во время следующей записи данных. Если схема системной таблицы изменилась в новом релизе, то ClickHouse переименует текущую таблицу и создаст новую.

Таблицы с системными логами `log` можно настроить, создав конфигурационный файл с тем же именем, что и таблица в разделе `/etc/clickhouse-server/config.d/`, или указав соответствующие элементы в `/etc/clickhouse-server/config.xml`. Настраиваться могут следующие элементы:

-   `database` — база данных, к которой принадлежит системная таблица. Эта опция на текущий момент устарела. Все системные таблицы находятся в базе данных `system`.
-   `table` — таблица для добавления данных.
-   `partition_by` — [ключ партиционирования](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `ttl` — [время жизни](../../sql-reference/statements/alter/ttl.md) записей в таблице.
-   `flush_interval_milliseconds` — интервал сброса данных на диск, в миллисекундах.
-   `engine` — полное имя движка (начиная с `ENGINE =` ) с параметрами. Эта опция противоречит `partition_by` и `ttl`. Если указать оба параметра вместе, сервер вернет ошибку и завершит работу.

Пример:

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>
</clickhouse>
```

По умолчанию размер таблицы не ограничен. Управлять размером таблицы можно используя [TTL](../../sql-reference/statements/alter/ttl.md#manipuliatsii-s-ttl-tablitsy) для удаления устаревших записей журнала. Также вы можете использовать функцию партиционирования для таблиц `MergeTree`.

### Источники системных показателей

Для сбора системных показателей сервер ClickHouse использует:

-   возможности `CAP_NET_ADMIN`.
-   [procfs](https://ru.wikipedia.org/wiki/Procfs) (только Linux).


Если для сервера ClickHouse не включено `CAP_NET_ADMIN`, он пытается обратиться к `ProcfsMetricsProvider`. `ProcfsMetricsProvider` позволяет собирать системные показатели для каждого запроса (для CPU и I/O).

Если procfs поддерживается и включена в системе, то сервер ClickHouse собирает следующие системные показатели:

-   `OSCPUVirtualTimeMicroseconds`
-   `OSCPUWaitMicroseconds`
-   `OSIOWaitMicroseconds`
-   `OSReadChars`
-   `OSWriteChars`
-   `OSReadBytes`
-   `OSWriteBytes`

