---
description: 'Страница с описанием профилирования выделения памяти в ClickHouse'
sidebar_label: 'Профилирование выделения памяти для версий до 25.9'
slug: /operations/allocation-profiling-old
title: 'Профилирование выделения памяти для версий до 25.9'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # Профилирование выделения памяти для версий до 25.9
</div>

ClickHouse использует [jemalloc](https://github.com/jemalloc/jemalloc) в качестве глобального аллокатора. В jemalloc есть инструменты для сэмплирования и профилирования выделения.
Чтобы упростить профилирование выделения памяти, доступны команды `SYSTEM`, а в Keeper — также команды из четырёх букв (4LW).

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## Сэмплирование выделения памяти и сброс профилей кучи
</div>

Если вы хотите выполнять сэмплирование и профилирование выделения памяти в `jemalloc`, необходимо запускать ClickHouse/Keeper с включенным профилированием, используя переменную окружения `MALLOC_CONF`:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

`jemalloc` будет выполнять выборку выделения памяти и хранить эту информацию внутри.

Вы можете указать `jemalloc` сбросить текущий профиль, выполнив:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

По умолчанию файл профиля кучи будет создан в `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`, где `_pid_` — это PID ClickHouse, а `_seqnum_` — глобальный порядковый номер текущего профиля кучи.
Для Keeper файл по умолчанию — `/tmp/jemalloc_keeper._pid_._seqnum_.heap`; для него действуют те же правила.

Другое расположение можно задать, добавив к переменной окружения `MALLOC_CONF` параметр `prof_prefix`.
Например, если вы хотите создавать профили в папке `/data`, где префиксом имени файла будет `my_current_profile`, вы можете запустить ClickHouse/Keeper со следующей переменной окружения:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

К имени сгенерированного файла будут добавлены префикс PID и номер последовательности.

<div id="analyzing-heap-profiles">
  ## Анализ профилей кучи
</div>

После создания профили кучи необходимо проанализировать.
Для этого можно использовать инструмент [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) из `jemalloc`. Его можно установить несколькими способами:

* С помощью системного менеджера пакетов
* Клонировать [репозиторий jemalloc](https://github.com/jemalloc/jemalloc) и запустить `autogen.sh` из корневой директории. В результате скрипт `jeprof` появится в каталоге `bin`

:::note
`jeprof` использует `addr2line` для генерации трассировок стека, что может работать довольно медленно.
Если это ваш случай, рекомендуется установить [альтернативную реализацию](https://github.com/gimli-rs/addr2line) этого инструмента.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

С помощью `jeprof` из профиля кучи можно получить данные во множестве различных форматов.
Рекомендуется выполнить `jeprof --help`, чтобы получить информацию по использованию и различным параметрам, доступным в этом инструменте.

В общем случае команда `jeprof` используется следующим образом:

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

Если вы хотите сравнить, какие выделения произошли между двумя профилями, можно задать аргумент `base`:

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### Примеры
</div>

* если вы хотите создать текстовый файл, где каждая процедура записана с новой строки:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* если вы хотите создать PDF-файл с графом вызовов:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### Генерация флеймграфа
</div>

`jeprof` позволяет генерировать свернутые стеки для построения флеймграфов.

Для этого нужно использовать аргумент `--collapsed`:

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

После этого вы можете использовать множество инструментов для визуализации свёрнутых стеков.

Самый популярный — [FlameGraph](https://github.com/brendangregg/FlameGraph), в состав которого входит скрипт `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Ещё один интересный инструмент — [speedscope](https://www.speedscope.app/), который позволяет более интерактивно анализировать собранные стеки.

<div id="controlling-allocation-profiler-during-runtime">
  ## Управление профилировщиком выделения во время выполнения
</div>

Если ClickHouse/Keeper запущен с включенным профилировщиком, поддерживаются дополнительные команды для включения и отключения профилирования выделения во время выполнения.
С помощью этих команд проще профилировать только определенные интервалы.

Чтобы отключить профилировщик:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC DISABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmdp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Чтобы включить профилировщик:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC ENABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmep | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Также можно управлять начальным состоянием профилировщика, задав параметр `prof_active`, который по умолчанию включен.
Например, если вы не хотите выполнять выборку выделения во время запуска, а только после него, можно включить профилировщик позже. Вы можете запустить ClickHouse/Keeper со следующей переменной окружения:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

Профилировщик можно включить позже.

<div id="additional-options-for-profiler">
  ## Дополнительные параметры для профилировщика
</div>

В `jemalloc` доступно множество параметров, связанных с профилировщиком. Ими можно управлять, изменяя переменную окружения `MALLOC_CONF`.
Например, интервал между выборками выделения памяти можно задать с помощью `lg_prof_sample`.
Если вы хотите сохранять профиль кучи каждые N байт, это можно включить с помощью `lg_prof_interval`.

Рекомендуется ознакомиться со [справочной страницей](https://jemalloc.net/jemalloc.3.html) `jemalloc`, где приведён полный список параметров.

<div id="other-resources">
  ## Другие ресурсы
</div>

ClickHouse/Keeper предоставляют метрики, связанные с `jemalloc`, разными способами.

:::warning Предупреждение
Важно учитывать, что ни одна из этих метрик не синхронизирована с остальными, поэтому их значения могут расходиться.
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

Содержит информацию о выделениях памяти через аллокатор jemalloc в различных классах размеров (bins), агрегированную по всем аренам.

[Справочник](/ru/operations/system-tables/jemalloc_bins)

<div id="prometheus">
  ### Prometheus
</div>

Все метрики, связанные с `jemalloc`, из `asynchronous_metrics` также публикуются через конечную точку Prometheus как в ClickHouse, так и в Keeper.

[Справочник](/ru/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Команда `jmst` 4LW в Keeper
</div>

Keeper поддерживает команду `jmst` 4LW, которая возвращает [базовую статистику аллокатора](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```