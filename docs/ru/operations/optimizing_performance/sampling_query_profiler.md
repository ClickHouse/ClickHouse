---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# Выборки Профилировщик Запросов  {#sampling-query-profiler}

ClickHouse запускает профилировщик выборок, который позволяет анализировать выполнение запросов. С помощью profiler можно найти подпрограммы исходного кода, которые наиболее часто используются во время выполнения запроса. Вы можете отслеживать процессорное время и время работы настенных часов, включая время простоя.

Чтобы использовать профилировщик:

-   Настройка программы [журнал трассировки](../server_configuration_parameters/settings.md#server_configuration_parameters-trace_log) раздел конфигурации сервера.

    В этом разделе настраиваются следующие параметры: [журнал трассировки](../../operations/optimizing_performance/sampling_query_profiler.md#system_tables-trace_log) системная таблица, содержащая результаты работы профилировщика. Он настроен по умолчанию. Помните, что данные в этой таблице действительны только для работающего сервера. После перезагрузки сервера ClickHouse не очищает таблицу, и все сохраненные адреса виртуальной памяти могут стать недействительными.

-   Настройка программы [query\_profiler\_cpu\_time\_period\_ns](../settings/settings.md#query_profiler_cpu_time_period_ns) или [query\_profiler\_real\_time\_period\_ns](../settings/settings.md#query_profiler_real_time_period_ns) настройки. Обе настройки можно использовать одновременно.

    Эти параметры позволяют настроить таймеры профилировщика. Поскольку это параметры сеанса, вы можете получить различную частоту дискретизации для всего сервера, отдельных пользователей или профилей пользователей, для вашего интерактивного сеанса и для каждого отдельного запроса.

Частота дискретизации по умолчанию составляет одну выборку в секунду, и включены как ЦП, так и реальные таймеры. Эта частота позволяет собрать достаточно информации о кластере ClickHouse. В то же время, работая с такой частотой, профилировщик не влияет на производительность сервера ClickHouse. Если вам нужно профилировать каждый отдельный запрос, попробуйте использовать более высокую частоту дискретизации.

Для того чтобы проанализировать `trace_log` системная таблица:

-   Установите устройство `clickhouse-common-static-dbg` пакет. Видеть [Установка из пакетов DEB](../../getting_started/install.md#install-from-deb-packages).

-   Разрешить функции самоанализа с помощью [allow\_introspection\_functions](../settings/settings.md#settings-allow_introspection_functions) установка.

    По соображениям безопасности функции самоанализа по умолчанию отключены.

-   Используйте `addressToLine`, `addressToSymbol` и `demangle` [функции самоанализа](../../operations/optimizing_performance/sampling_query_profiler.md) чтобы получить имена функций и их позиции в коде ClickHouse. Чтобы получить профиль для какого-либо запроса, вам необходимо агрегировать данные из `trace_log` стол. Вы можете агрегировать данные по отдельным функциям или по всем трассировкам стека.

Если вам нужно визуализировать `trace_log` информация, попробуйте [огнемет](../../interfaces/third-party/gui/#clickhouse-flamegraph) и [speedscope](https://github.com/laplab/clickhouse-speedscope).

## Пример {#example}

В этом примере мы:

-   Фильтрация `trace_log` данные по идентификатору запроса и текущей дате.

-   Агрегирование по трассировке стека.

-   Используя функции интроспекции, мы получим отчет о:

    -   Имена символов и соответствующие им функции исходного кода.
    -   Расположение исходных кодов этих функций.

<!-- -->

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```

``` text
{% include "operations/performance/sampling_query_profiler_example_result.txt" %}
```
