---
description: 'Страница с описанием профилирования выделения памяти в ClickHouse'
sidebar_label: 'Профилирование выделения памяти'
slug: /operations/allocation-profiling
title: 'Профилирование выделения памяти'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling">
  # Профилирование выделения памяти
</div>

ClickHouse использует [jemalloc](https://github.com/jemalloc/jemalloc) в качестве глобального аллокатора. Jemalloc поставляется с инструментами для сэмплирования выделений памяти и профилирования.

ClickHouse и Keeper позволяют управлять сэмплированием с помощью конфигурации, настроек запроса, команд `SYSTEM` и четырёхбуквенных команд (4LW) в Keeper. Есть несколько способов просмотреть результаты:

* Собирать образцы в `system.trace_log` с типом `JemallocSample` для анализа отдельных запросов.
* Просматривать статистику памяти в реальном времени и получать профили кучи через встроенный [веб-интерфейс jemalloc](#jemalloc-web-ui) (26.2+).
* Запрашивать текущий профиль кучи напрямую из SQL с помощью [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql) (26.2+).
* Сбрасывать профили кучи на диск и анализировать их с помощью [`jeprof`](#analyzing-heap-profile-files-with-jeprof).

:::note

Это руководство применимо к версиям 25.9+.
Для более старых версий см. [профилирование выделения памяти для версий до 25.9](/ru/operations/allocation-profiling-old.md).

:::

<div id="sampling-allocations">
  ## Сэмплирование выделений памяти
</div>

Чтобы включить сэмплирование и профилирование выделений памяти, запустите ClickHouse/Keeper с включенным параметром конфигурации `jemalloc_enable_global_profiler`:

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc` будет выполнять сэмплирование выделений памяти и сохранять эту информацию у себя.

Вы также можете включить сэмплирование для каждого запроса с помощью настройки `jemalloc_enable_profiler`.

:::warning Предупреждение
Поскольку ClickHouse активно использует выделение памяти, сэмплирование jemalloc может снижать производительность.
:::

<div id="storing-jemalloc-samples-in-system-trace-log">
  ## Хранение образцов jemalloc в `system.trace_log`
</div>

Образцы jemalloc можно сохранять в `system.trace_log` с типом `JemallocSample`.
Чтобы включить это глобально, используйте параметр конфигурации `jemalloc_collect_global_profile_samples_in_trace_log`:

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning Предупреждение
Поскольку ClickHouse — приложение, активно работающее с выделением памяти, сбор всех образцов в system.trace&#95;log может создавать высокую нагрузку.
:::

Это также можно включить для отдельных запросов с помощью настройки `jemalloc_collect_profile_samples_in_trace_log`.

<div id="example-analyzing-memory-usage-trace-log">
  ### Пример: анализ использования памяти в запросе
</div>

Сначала выполните запрос с включенным профилировщиком jemalloc и соберите образцы в `system.trace_log`:

```sql
SELECT *
FROM numbers(1000000)
ORDER BY number DESC
SETTINGS max_bytes_ratio_before_external_sort = 0
FORMAT `Null`
SETTINGS jemalloc_enable_profiler = 1, jemalloc_collect_profile_samples_in_trace_log = 1

Query id: 8678d8fe-62c5-48b8-b0cd-26851c62dd75

Ok.

0 rows in set. Elapsed: 0.009 sec. Processed 1.00 million rows, 8.00 MB (108.58 million rows/s., 868.61 MB/s.)
Peak memory usage: 12.65 MiB.
```

:::note
Если ClickHouse был запущен с `jemalloc_enable_global_profiler`, включать `jemalloc_enable_profiler` не нужно.
То же самое относится к `jemalloc_collect_global_profile_samples_in_trace_log` и `jemalloc_collect_profile_samples_in_trace_log`.
:::

Выполните сброс `system.trace_log`:

```sql
SYSTEM FLUSH LOGS trace_log
```

Затем выполните запрос, чтобы получить накопительное использование памяти во времени:

```sql
WITH per_bucket AS
(
    SELECT
        event_time_microseconds AS bucket_time,
        sum(size) AS bucket_sum
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
    GROUP BY bucket_time
)
SELECT
    bucket_time,
    sum(bucket_sum) OVER (
        ORDER BY bucket_time ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_size,
    formatReadableSize(cumulative_size) AS cumulative_size_readable
FROM per_bucket
ORDER BY bucket_time
```

Найдите время, когда использование памяти было максимальным:

```sql
SELECT
    argMax(bucket_time, cumulative_size),
    max(cumulative_size)
FROM
(
    WITH per_bucket AS
    (
        SELECT
            event_time_microseconds AS bucket_time,
            sum(size) AS bucket_sum
        FROM system.trace_log
        WHERE trace_type = 'JemallocSample'
          AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
        GROUP BY bucket_time
    )
    SELECT
        bucket_time,
        sum(bucket_sum) OVER (
            ORDER BY bucket_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_size,
        formatReadableSize(cumulative_size) AS cumulative_size_readable
    FROM per_bucket
    ORDER BY bucket_time
)
```

По этому результату посмотрите, какие стеки вызовов выделения памяти были наиболее активны в пиковый момент:

```sql
SELECT
    concat(
        '\n',
        arrayStringConcat(
            arrayMap(
                (x, y) -> concat(x, ': ', y),
                arrayMap(x -> addressToLine(x), allocation_trace),
                arrayMap(x -> demangle(addressToSymbol(x)), allocation_trace)
            ),
            '\n'
        )
    ) AS symbolized_trace,
    sum(s) AS per_trace_sum
FROM
(
    SELECT
        ptr,
        sum(size) AS s,
        argMax(trace, event_time_microseconds) AS allocation_trace
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
      AND event_time_microseconds <= '2025-09-04 11:56:21.737139'
    GROUP BY ptr
    HAVING s > 0
)
GROUP BY ALL
ORDER BY per_trace_sum ASC
```

<div id="jemalloc-web-ui">
  ## Веб-интерфейс jemalloc
</div>

:::note
Этот раздел относится к версиям 26.2+.
:::

ClickHouse предоставляет встроенный веб-интерфейс для просмотра статистики памяти jemalloc по HTTP-конечной точке `/jemalloc`.
Он отображает метрики памяти в реальном времени в виде диаграмм, включая allocated, active, resident и mapped memory, а также статистику по аренам и bin.
Вы также можете получать глобальные и относящиеся к отдельным запросам профили кучи напрямую из интерфейса.

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```text
    http://localhost:8123/jemalloc
    ```

    Интерфейс сервера включает все вкладки: Summary, Allocations, Arenas, Operations, Global Profiler, Query Profiler и Raw Output.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```text
    http://localhost:9182/jemalloc
    ```

    Интерфейс Keeper доступен через порт управления по HTTP. Этот порт **по умолчанию отключен** и должен быть явно включен с помощью настройки `keeper_server.http_control.port` в конфигурации Keeper:

    ```xml
    <clickhouse>
        <keeper_server>
            <http_control>
                <port>9182</port>
            </http_control>
        </keeper_server>
    </clickhouse>
    ```

    После включения интерфейс предоставляет те же визуализации, что и сервер, — Summary, Allocations, Arenas, Operations, Global Profiler и Raw Output — за исключением вкладки Query Profiler, для которой требуются SQL и `system.trace_log`.

    :::warning Безопасность
    Порт управления по HTTP у Keeper не имеет аутентификации на уровне приложения. В отличие от jemalloc UI сервера ClickHouse, где все запросы данных проходят через обработчик SQL HTTP и требуют имени пользователя и пароля, конечные точки REST API у Keeper не требуют аутентификации. Это соответствует другим конечным точкам управления по HTTP в Keeper (commands, storage, dashboard).

    Ограничьте доступ к этому порту с помощью средств контроля на уровне сети: привяжите Keeper к localhost, используйте правила firewall или разместите его за обратным прокси с аутентификацией. Если `listen_host` не настроен, Keeper по умолчанию слушает только localhost.
    :::

    Keeper также предоставляет конечные точки REST API для программного доступа:

    * `GET /jemalloc/stats` — необработанный вывод `malloc_stats_print`
    * `GET /jemalloc/status` — состояние профилирования в формате JSON (`prof_enabled`, `prof_active`, `thread_active_init`, `lg_sample`)
    * `GET /jemalloc/profile?format={collapsed|raw}` — выгружает профиль кучи с символизацией на стороне сервера и возвращает свёрнутые стеки, подходящие для построения флеймграфа (по умолчанию), либо необработанный дамп jemalloc
  </TabItem>
</Tabs>

<div id="fetching-heap-profiles-from-sql">
  ## Получение профилей кучи через SQL
</div>

:::note
Этот раздел относится к версиям 26.2+.
:::

Системная таблица `system.jemalloc_profile_text` позволяет получать и просматривать текущий профиль кучи jemalloc напрямую из SQL, без использования внешних инструментов и без предварительной выгрузки на диск.

Таблица содержит один столбец:

| Столбец | Тип    | Описание                                         |
| ------- | ------ | ------------------------------------------------ |
| `line`  | String | Строка символизированного профиля кучи jemalloc. |

Таблицу можно запрашивать напрямую — предварительно выгружать профиль кучи на диск не требуется:

```sql
SELECT * FROM system.jemalloc_profile_text
```

<div id="output-format">
  ### Формат вывода
</div>

Формат вывода задаётся настройкой `jemalloc_profile_text_output_format`, которая поддерживает три значения:

* `raw` — необработанный профиль кучи, созданный jemalloc.
* `symbolized` — символизированный формат, совместимый с jeprof, со встроенными символами функций. Поскольку символы уже встроены, `jeprof` может анализировать вывод без бинарного файла ClickHouse.
* `collapsed` (по умолчанию) — свёрнутые стеки, совместимые с FlameGraph: по одному стеку в строке с количеством байтов.

Например, чтобы получить необработанный профиль:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

Чтобы получить символизированный вывод:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

<div id="fetching-heap-profiles-settings">
  ### Дополнительные настройки
</div>

* `jemalloc_profile_text_symbolize_with_inline` (Bool, по умолчанию: `true`) — Включать ли инлайн-фреймы при символизации. Отключение этого параметра значительно ускоряет символизацию, но снижает точность, поскольку встроенные вызовы функций не будут отображаться в стеках вызовов. Влияет только на форматы `symbolized` и `collapsed`.
* `jemalloc_profile_text_collapsed_use_count` (Bool, по умолчанию: `false`) — При использовании формата `collapsed` выполнять агрегирование по числу аллокаций, а не по количеству байт.

<div id="example-flamegraph-from-sql">
  ### Пример: создание флеймграфа из SQL
</div>

Поскольку формат вывода по умолчанию — `collapsed`, вы можете передать вывод напрямую в FlameGraph:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Чтобы сгенерировать флеймграф по числу аллокаций, а не по объёму в байтах:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

<div id="flushing-heap-profiles">
  ## Сброс профилей кучи на диск
</div>

Если вам нужно сохранить профили кучи в файлы для офлайн-анализа с помощью `jeprof`, их можно сбросить на диск.

По умолчанию файл профиля кучи создается в `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`, где `_pid_` — PID процесса ClickHouse, а `_seqnum_` — глобальный порядковый номер текущего профиля кучи.
Для Keeper файлом по умолчанию будет `/tmp/jemalloc_keeper._pid_._seqnum_.heap`; для него действуют те же правила.

Чтобы сбросить текущий профиль:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```

    Команда вернет путь к профилю, сброшенному на диск.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Другой путь можно задать, дополнив переменную окружения `MALLOC_CONF` параметром `prof_prefix`.
Например, если вы хотите создавать профили в каталоге `/data`, где префиксом имени файла будет `my_current_profile`, вы можете запустить ClickHouse/Keeper со следующей переменной окружения:

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

К имени сгенерированного файла будут добавлены префикс, PID и порядковый номер.

<div id="analyzing-heap-profile-files-with-jeprof">
  ## Анализ файлов профиля кучи с помощью `jeprof`
</div>

После сброса профилей кучи на диск их можно анализировать с помощью инструмента `jemalloc` под названием [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). Его можно установить несколькими способами:

* С помощью системного менеджера пакетов
* Клонировав [репозиторий jemalloc](https://github.com/jemalloc/jemalloc) и запустив `autogen.sh` из корневой папки. В результате вы получите скрипт `jeprof` в папке `bin`

Доступно множество различных форматов вывода. Выполните `jeprof --help`, чтобы получить полный список параметров.

<div id="symbolized-heap-profiles">
  ### Символизированные профили кучи
</div>

Начиная с версии 26.1+, ClickHouse автоматически создает символизированные профили кучи при вызове `SYSTEM JEMALLOC FLUSH PROFILE`.
Символизированный профиль (с расширением `.symbolized`) содержит встроенную символьную информацию о функциях и может анализироваться с помощью `jeprof` без использования бинарного файла ClickHouse.

Например, если выполнить:

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

ClickHouse вернёт путь к symbolized profile (например, `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`).

Затем вы можете сразу проанализировать его с помощью `jeprof`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**Бинарный файл не нужен**: При использовании символизированных профилей (файлов `.symbolized`) не нужно указывать `jeprof` путь к бинарному файлу ClickHouse. Это значительно упрощает анализ профилей на разных машинах или после обновления бинарного файла.

:::

Если у вас есть более старый несимволизированный профиль кучи и у вас по-прежнему есть доступ к бинарному файлу ClickHouse, вы можете воспользоваться традиционным подходом:

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

Для профилей без символизации `jeprof` использует `addr2line` для построения трассировок стека, а это может работать очень медленно.
В этом случае рекомендуется установить [альтернативную реализацию](https://github.com/gimli-rs/addr2line) этого инструмента.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

В качестве альтернативы можно использовать `llvm-addr2line` — он работает ничуть не хуже (но обратите внимание, что `llvm-objdump` несовместим с `jeprof`)

А затем использовать его так: `jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

При сравнении двух профилей можно использовать аргумент `--base`:

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

<div id="examples">
  ### Примеры
</div>

Использование символизированных профилей (рекомендуется):

* Создайте текстовый файл, записав каждую процедуру в отдельной строке:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

* Сгенерируйте PDF-файл с графом вызовов:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

Использование профилей без символизации (требуется бинарный файл):

* Сгенерируйте текстовый файл, в котором каждая процедура записана на отдельной строке:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

* Сгенерируйте PDF-файл с графом вызовов:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### Генерация флеймграфа
</div>

`jeprof` позволяет генерировать свёрнутые стеки для построения флеймграфов.

Для этого нужно использовать аргумент `--collapsed`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

Или с профилем без символизации:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

После этого можно использовать множество разных инструментов для визуализации свёрнутых стеков.

Самый популярный — [FlameGraph](https://github.com/brendangregg/FlameGraph), в котором есть скрипт `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Ещё один интересный инструмент — [speedscope](https://www.speedscope.app/), который позволяет анализировать собранные стеки в более интерактивном режиме.

<div id="additional-options-for-profiler">
  ## Дополнительные параметры профилировщика
</div>

В `jemalloc` доступно множество параметров, связанных с профилировщиком. Ими можно управлять, изменяя переменную окружения `MALLOC_CONF`.
Например, интервал между образцами выделения памяти можно настроить с помощью `lg_prof_sample`.
Если вы хотите сохранять профиль кучи каждые N байт, это можно включить с помощью `lg_prof_interval`.

Полный список параметров рекомендуется смотреть на [справочной странице](https://jemalloc.net/jemalloc.3.html) `jemalloc`.

<div id="other-resources">
  ## Другие ресурсы
</div>

ClickHouse/Keeper предоставляют метрики, связанные с `jemalloc`, разными способами.

:::warning Предупреждение
Важно понимать, что эти метрики не синхронизированы между собой, и их значения могут расходиться.
:::

<div id="system-table-asynchronous_metrics">
  ### Системная таблица `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[Справочник](/ru/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### Системная таблица `jemalloc_bins`
</div>

Содержит информацию о выделениях памяти, выполняемых аллокатором jemalloc в различных классах размеров (bins), агрегированную по всем аренам.

[Справочник](/ru/operations/system-tables/jemalloc_bins)

<div id="system-table-jemalloc_stats">
  ### Системная таблица `jemalloc_stats` (26.2+)
</div>

Возвращает полный вывод `malloc_stats_print()` в виде одной текстовой строки. Эквивалентно команде `SYSTEM JEMALLOC STATS`.

```sql
SELECT * FROM system.jemalloc_stats
```

<div id="prometheus">
  ### Prometheus
</div>

Все связанные с `jemalloc` метрики из `asynchronous_metrics` также экспортируются через конечную точку Prometheus как в ClickHouse, так и в Keeper.

[Справочник](/ru/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Команда 4LW `jmst` в Keeper
</div>

Keeper поддерживает команду 4LW `jmst`, которая возвращает [базовую статистику аллокатора](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```