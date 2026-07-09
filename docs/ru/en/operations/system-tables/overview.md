---
description: 'Обзор системных таблиц и их назначения.'
keywords: ['системные таблицы', 'обзор']
sidebar_label: 'Обзор'
sidebar_position: 52
slug: /operations/system-tables/overview
title: 'Обзор системных таблиц'
doc_type: 'reference'
---

<div id="system-tables-introduction">
  ## Обзор системных таблиц
</div>

Системные таблицы предоставляют информацию о:

* Состояниях сервера, процессах и окружении.
* Внутренних процессах сервера.
* Параметрах, использованных при сборке бинарного файла ClickHouse.

Системные таблицы:

* Находятся в базе данных `system`.
* Доступны только для чтения данных.
* Не могут быть удалены или изменены, но могут быть отсоединены.

Большинство системных таблиц хранят свои данные в оперативной памяти. Сервер ClickHouse создает такие системные таблицы при запуске.

В отличие от других системных таблиц, системные таблицы логов [metric&#95;log](../../operations/system-tables/metric_log.md), [query&#95;log](../../operations/system-tables/query_log.md), [query&#95;thread&#95;log](../../operations/system-tables/query_thread_log.md), [trace&#95;log](../../operations/system-tables/trace_log.md), [part&#95;log](../../operations/system-tables/part_log.md), [crash&#95;log](../../operations/system-tables/crash_log.md), [text&#95;log](../../operations/system-tables/text_log.md) и [backup&#95;log](../../operations/system-tables/backup_log.md) используют движок таблицы [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) и по умолчанию хранят свои данные в файловой системе. Если удалить таблицу из файловой системы, сервер ClickHouse при следующей записи данных снова создаст пустую таблицу. Если в новом релизе изменилась схема системной таблицы, ClickHouse переименует текущую таблицу и создаст новую.

Системные таблицы логов можно настроить, создав файл конфигурации с тем же именем, что и у таблицы, в каталоге `/etc/clickhouse-server/config.d/`, или задав соответствующие элементы в `/etc/clickhouse-server/config.xml`. Можно настраивать следующие элементы:

* `database`: база данных, к которой относится системная таблица логов. Сейчас этот параметр устарел. Все системные таблицы логов находятся в базе данных `system`.
* `table`: таблица для вставки данных.
* `partition_by`: указывает выражение [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
* `ttl`: указывает выражение [TTL](../../sql-reference/statements/alter/ttl.md) таблицы.
* `flush_interval_milliseconds`: интервал сброса данных на диск.
* `engine`: задает полное выражение движка (начиная с `ENGINE =`) с параметрами. Этот параметр конфликтует с `partition_by` и `ttl`. Если задать их вместе, сервер сгенерирует исключение и завершит работу.

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
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

По умолчанию рост таблицы не ограничен. Чтобы контролировать размер таблицы, можно использовать настройки [TTL](/ru/sql-reference/statements/alter/ttl) для удаления устаревших записей логов. Также можно использовать партиционирование таблиц с движком `MergeTree`.

<div id="system-tables-sources-of-system-metrics">
  ## Источники системных метрик
</div>

Для сбора системных метрик сервер ClickHouse использует:

* возможность `CAP_NET_ADMIN`.
* [procfs](https://en.wikipedia.org/wiki/Procfs) (только в Linux).

**procfs**

Если у сервера ClickHouse нет возможности `CAP_NET_ADMIN`, он пытается переключиться на `ProcfsMetricsProvider`. `ProcfsMetricsProvider` позволяет собирать системные метрики на уровне отдельных запросов (для CPU и I/O).

Если procfs поддерживается и включён в системе, сервер ClickHouse собирает следующие метрики:

* `OSCPUVirtualTimeMicroseconds`
* `OSCPUWaitMicroseconds`
* `OSIOWaitMicroseconds`
* `OSReadChars`
* `OSWriteChars`
* `OSReadBytes`
* `OSWriteBytes`

:::note
`OSIOWaitMicroseconds` по умолчанию отключён в ядрах Linux, начиная с версии 5.14.x.
Вы можете включить его с помощью `sudo sysctl kernel.task_delayacct=1` или создав файл `.conf` в `/etc/sysctl.d/` с `kernel.task_delayacct = 1`
:::

<div id="system-tables-in-clickhouse-cloud">
  ## Системные таблицы в ClickHouse Cloud
</div>

В ClickHouse Cloud системные таблицы дают важную информацию о состоянии и производительности сервиса, так же как и в самоуправляемых развертываниях. Некоторые системные таблицы работают на уровне всего кластера, особенно те, которые получают данные от узлов Keeper, управляющих распределенными метаданными. Эти таблицы отражают общее состояние кластера и должны быть согласованными при запросе с отдельных узлов. Например, [`parts`](/ru/operations/system-tables/parts) должна быть согласованной независимо от того, с какого узла к ней выполняется запрос:

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

В отличие от этого, другие системные таблицы привязаны к конкретному узлу, например хранятся в памяти или сохраняют свои данные с помощью движка таблицы MergeTree. Это типично для таких данных, как журналы и метрики. Такое хранение гарантирует, что исторические данные остаются доступными для анализа. Однако эти таблицы, привязанные к узлу, по своей природе уникальны для каждого узла.

В общем случае при определении того, привязана ли системная таблица к узлу, можно применять следующие правила:

* Системные таблицы с суффиксом `_log`.
* Системные таблицы, которые предоставляют метрики, например `metrics`, `asynchronous_metrics`, `events`.
* Системные таблицы, которые отражают текущие процессы, например `processes`, `merges`.

Кроме того, новые версии системных таблиц могут создаваться в результате обновлений или изменений их схемы. Эти версии именуются с использованием числового суффикса.

Например, рассмотрим таблицы `system.query_log`, которые содержат строку для каждого запроса, выполненного на узле:

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

<div id="querying-multiple-versions">
  ### Запросы к нескольким версиям
</div>

Можно выполнять запросы к этим таблицам с помощью функции [`merge`](/ru/sql-reference/table-functions/merge). Например, приведённый ниже запрос находит последний запрос, отправленный на целевой узел, в каждой таблице `query_log`:

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note Не полагайтесь на числовой суффикс при определении порядка
Хотя числовой суффикс в имени таблицы может указывать на порядок данных, полагаться на него нельзя. Поэтому при работе с конкретными диапазонами дат всегда используйте табличную функцию `merge` в сочетании с фильтром по дате.
:::

Важно, что эти таблицы по-прежнему **локальны для каждого узла**.

<div id="querying-across-nodes">
  ### Выполнение запросов на всех узлах
</div>

Чтобы получить полное представление обо всём кластере, можно использовать функцию [`clusterAllReplicas`](/ru/sql-reference/table-functions/cluster) в сочетании с функцией `merge`. Функция `clusterAllReplicas` позволяет выполнять запросы к системным таблицам на всех репликах в кластере &quot;default&quot;, объединяя данные отдельных узлов в единый результат. В сочетании с функцией `merge` её можно использовать для обращения ко всем системным данным конкретной таблицы в кластере.

Этот подход особенно полезен для мониторинга и отладки операций в масштабе всего кластера, помогая эффективно анализировать состояние и производительность развертывания ClickHouse Cloud.

:::note
ClickHouse Cloud предоставляет кластеры из нескольких реплик для резервирования и аварийного переключения. Это обеспечивает такие возможности, как динамическое автомасштабирование и обновления без простоя. В определённый момент новые узлы могут находиться в процессе добавления в кластер или удаления из него. Чтобы пропустить такие узлы, добавьте `SETTINGS skip_unavailable_shards = 1` в запросы с использованием `clusterAllReplicas`, как показано ниже.
:::

Например, рассмотрим разницу при выполнении запроса к таблице `query_log` — это часто важно для анализа.

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

<div id="querying-across-nodes-and-versions">
  ### Выполнение запросов по всем узлам и версиям
</div>

Из-за версионирования системных таблиц это по-прежнему не отражает все данные в кластере. Если объединить это с функцией `merge`, мы получим точный результат для выбранного диапазона дат:

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

<div id="related-content">
  ## Материалы по теме
</div>

* Блог: [Системные таблицы и взгляд на внутреннее устройство ClickHouse](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
* Блог: [Основные запросы для мониторинга — часть 1 — запросы INSERT](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
* Блог: [Основные запросы для мониторинга — часть 2 — запросы SELECT](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)