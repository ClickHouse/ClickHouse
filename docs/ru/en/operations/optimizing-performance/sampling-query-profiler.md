---
description: 'Документация по выборочному профилировщику запросов в ClickHouse'
sidebar_label: 'Профилирование запросов'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: 'Выборочный профилировщик запросов'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="sampling-query-profiler">
  # Выборочный профилировщик запросов
</div>

В ClickHouse работает выборочный профилировщик, который позволяет анализировать выполнение запроса.
С помощью профилировщика можно найти процедуры исходного кода, которые чаще всего используются при выполнении запроса.
Можно отслеживать затраченное время CPU и фактическое время, включая время простоя.

В ClickHouse Cloud профилировщик запросов включен автоматически.
Следующий пример запроса находит наиболее частые трассировки стека для профилируемого запроса с разрешенными именами функций и указанием мест в исходном коде:

:::tip
Замените значение `query_id` на идентификатор запроса, который вы хотите профилировать.
:::

<Tabs groupId="deployment">
  <TabItem value="cloud" label="ClickHouse Cloud">
    В ClickHouse Cloud идентификатор запроса можно получить, нажав **&quot;...&quot;** в крайней правой части панели над таблицей результатов запроса (рядом с переключателем таблица/диаграмма). Откроется контекстное меню, в котором можно нажать **&quot;Copy query ID&quot;**.

    Используйте `clusterAllReplicas(default, system.trace_log)`, чтобы выполнить выборку со всех узлов кластера:

    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM clusterAllReplicas(default, system.trace_log)
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>

  <TabItem value="self-managed" label="Самоуправляемый">
    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>
</Tabs>

<div id="self-managed-query-profiler">
  ## Использование профилировщика запросов в самоуправляемых развертываниях
</div>

Чтобы использовать профилировщик запросов в самоуправляемых развертываниях, выполните следующие шаги:

<VerticalStepper headerLevel="h3">
  ### Установите ClickHouse с отладочной информацией

  Установите пакет `clickhouse-common-static-dbg`:

  1. Следуйте инструкциям из шага [&quot;Настройка репозитория Debian&quot;](/ru/install/debian_ubuntu#setup-the-debian-repository)
  2. Выполните `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg`, чтобы установить двоичные файлы ClickHouse, скомпилированные с отладочной информацией
  3. Выполните `sudo service clickhouse-server start`, чтобы запустить сервер
  4. Выполните `clickhouse-client`. Символы отладки из `clickhouse-common-static-dbg` будут автоматически подхвачены сервером — дополнительно включать их не нужно

  ### Проверьте конфигурацию сервера

  Убедитесь, что раздел [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) в вашем [файле конфигурации сервера](/ru/operations/configuration-files) настроен. По умолчанию он включен:

  ```xml
  <!-- Трассировочный лог. Хранит трассировки стека, собранные профилировщиками запросов.
       См. настройки query_profiler_real_time_period_ns и query_profiler_cpu_time_period_ns. -->
  <trace_log>
      <database>system</database>
      <table>trace_log</table>

      <partition_by>toYYYYMM(event_date)</partition_by>
      <flush_interval_milliseconds>7500</flush_interval_milliseconds>
      <max_size_rows>1048576</max_size_rows>
      <reserved_size_rows>8192</reserved_size_rows>
      <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
      <!-- Указывает, следует ли сбрасывать логи на диск в случае сбоя -->
      <flush_on_crash>false</flush_on_crash>
      <symbolize>true</symbolize>
  </trace_log>
  ```

  Этот раздел настраивает системную таблицу [trace&#95;log](/ru/operations/system-tables/trace_log), содержащую результаты работы профилировщика.
  Помните, что данные в этой таблице актуальны только пока сервер работает.
  После перезапуска сервера ClickHouse не очищает таблицу, и все сохраненные адреса виртуальной памяти могут стать недействительными.

  ### Настройте таймеры профилирования

  Настройте параметры [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) или [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns).
  Оба параметра можно использовать одновременно.

  Эти параметры позволяют настроить таймеры профилировщика.
  Поскольку это настройки сеанса, вы можете задать разную частоту сэмплирования для всего сервера, отдельных пользователей или профилей пользователей, для вашего интерактивного сеанса и для каждого отдельного запроса.

  Частота сэмплирования по умолчанию — один сэмпл в секунду; при этом включены и CPU-таймеры, и таймеры реального времени.
  Такая частота позволяет собрать достаточно информации о вашем кластере ClickHouse, не влияя при этом на производительность сервера.
  Если вам нужно профилировать каждый отдельный запрос, используйте более высокую частоту сэмплирования.

  ### Анализируйте системную таблицу `trace_log`

  Чтобы анализировать системную таблицу `trace_log`, включите функции интроспекции с помощью настройки [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions):

  ```sql
  SET allow_introspection_functions=1
  ```

  :::note
  По соображениям безопасности функции интроспекции по умолчанию отключены
  :::

  Используйте [функции интроспекции](../../sql-reference/functions/introspection.md) `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` и `demangle`, чтобы получить имена функций и их позиции в коде ClickHouse.
  Чтобы получить профиль для некоторого запроса, нужно агрегировать данные из таблицы `trace_log`.
  Вы можете агрегировать данные по отдельным функциям или по полным трассировкам стека.

  :::tip
  Если вам нужно визуализировать данные из `trace_log`, попробуйте [flamegraph](/ru/interfaces/third-party/gui#clickhouse-flamegraph) и [speedscope](https://www.speedscope.app).
  :::
</VerticalStepper>

<div id="flamegraph">
  ## Построение флеймграфов с помощью функции `flameGraph`
</div>

ClickHouse предоставляет агрегатную функцию [`flameGraph`](/ru/sql-reference/aggregate-functions/reference/flame_graph), которая строит флеймграф напрямую на основе трассировок стека, хранящихся в `trace_log`.
Результат представляет собой массив строк в формате, совместимом с [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

**Синтаксис:**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**Аргументы:**

* `traces` — трассировка стека. [`Array(UInt64)`](/ru/sql-reference/data-types/array).
* `size` — размер выделения памяти для профилирования. [`Int64`](/ru/sql-reference/data-types/int-uint).
* `ptr` — адрес выделения памяти. [`UInt64`](/ru/sql-reference/data-types/int-uint).

Если `ptr` не равен нулю, `flameGraph` сопоставляет выделения (`size > 0`) и освобождения памяти (`size < 0`) с одинаковыми размером и указателем.
Показываются только выделения, которые не были освобождены.
Несопоставленные освобождения памяти игнорируются.

<div id="cpu-flame-graph">
  ### CPU-флеймграф
</div>

:::note
Для выполнения приведённых ниже запросов у вас должен быть установлен [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

Для этого выполните:

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

Замените `flamegraph.pl` в следующих запросах на путь к файлу `flamegraph.pl` на вашей машине
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

Выполните запрос, затем постройте флеймграф:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

<div id="memory-flame-graph-all">
  ### Флеймграф памяти — все выделения
</div>

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

Выполните запрос, затем постройте флеймграф:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

<div id="memory-flame-graph-unfreed">
  ### Флеймграф памяти — неосвобождённые выделения
</div>

Этот вариант сопоставляет выделения и освобождения памяти по указателю и показывает только память, которая не была освобождена в ходе выполнения запроса.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

Выполните следующий запрос, чтобы построить флеймграф:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

<div id="memory-flame-graph-time-point">
  ### Флеймграф памяти — активные выделения в определённый момент времени
</div>

Этот подход позволяет найти пиковое потребление памяти и визуализировать, какие выделения были активны в этот момент.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

<div id="find-memory-usage-over-time">
  #### Найдите использование памяти в динамике
</div>

```sql
SELECT
    event_time,
    formatReadableSize(max(s)) AS m
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
)
GROUP BY event_time
ORDER BY event_time;
```

<div id="find-time-point-maximum-memory-usage">
  #### Найдите момент времени с пиковым использованием памяти
</div>

```sql
SELECT
    argMax(event_time, s),
    max(s)
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
);
```

<div id="build-flame-graph">
  #### Постройте флеймграф активных выделений в этот момент времени
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time <= '<time_point>'
            ORDER BY event_time
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

<div id="build-flame-graph-deallocations">
  #### Постройте флеймграф освобождения памяти после этого момента времени (чтобы понять, что было освобождено позже)
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, -size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time > '<time_point>'
            ORDER BY event_time DESC
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```

<div id="example">
  ## Пример
</div>

Фрагмент кода ниже:

* Фильтрует данные `trace_log` по идентификатору запроса и текущей дате.
* Выполняет агрегацию по трассировке стека.
* Использует функции интроспекции, чтобы получить отчёт о:
  * именах символов и соответствующих им функциях исходного кода;
  * местах в исходном коде, где определены эти функции.

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = '<query_id>') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```