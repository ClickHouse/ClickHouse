# Системные таблицы {#sistemnye-tablitsy}

Системные таблицы используются для реализации части функциональности системы, а также предоставляют доступ к информации о работе системы.
Вы не можете удалить системную таблицу (хотя можете сделать DETACH).
Для системных таблиц нет файлов с данными на диске и файлов с метаданными. Сервер создаёт все системные таблицы при старте.
В системные таблицы нельзя записывать данные - можно только читать.
Системные таблицы расположены в базе данных system.

## system.asynchronous\_metrics {#system_tables-asynchronous_metrics}

Содержит метрики, которые периодически вычисляются в фоновом режиме. Например, объём используемой оперативной памяти.

Столбцы:

-   `metric` ([String](../sql-reference/data-types/string.md)) — название метрики.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — значение метрики.

**Пример**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┐
│ jemalloc.background_thread.run_interval │          0 │
│ jemalloc.background_thread.num_runs     │          0 │
│ jemalloc.background_thread.num_threads  │          0 │
│ jemalloc.retained                       │  422551552 │
│ jemalloc.mapped                         │ 1682989056 │
│ jemalloc.resident                       │ 1656446976 │
│ jemalloc.metadata_thp                   │          0 │
│ jemalloc.metadata                       │   10226856 │
│ UncompressedCacheCells                  │          0 │
│ MarkCacheFiles                          │          0 │
└─────────────────────────────────────────┴────────────┘
```

**Смотрите также**

-   [Мониторинг](monitoring.md) — основы мониторинга в ClickHouse.
-   [system.metrics](#system_tables-metrics) — таблица с мгновенно вычисляемыми метриками.
-   [system.events](#system_tables-events) — таблица с количеством произошедших событий.
-   [system.metric\_log](#system_tables-metric_log) — таблица фиксирующая историю значений метрик из `system.metrics` и `system.events`.

## system.clusters {#system-clusters}

Содержит информацию о доступных в конфигурационном файле кластерах и серверах, которые в них входят.
Столбцы:

``` text
cluster String — имя кластера.
shard_num UInt32 — номер шарда в кластере, начиная с 1.
shard_weight UInt32 — относительный вес шарда при записи данных
replica_num UInt32  — номер реплики в шарде, начиная с 1.
host_name String — хост, указанный в конфигурации.
host_address String — IP-адрес хоста, полученный из DNS.
port UInt16 — порт, на который обращаться для соединения с сервером.
user String — имя пользователя, которого использовать для соединения с сервером.
```

## system.columns {#system-columns}

Содержит информацию о столбцах всех таблиц.

С помощью этой таблицы можно получить информацию аналогично запросу [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table), но для многих таблиц сразу.

Таблица `system.columns` содержит столбцы (тип столбца указан в скобках):

-   `database` (String) — имя базы данных.
-   `table` (String) — имя таблицы.
-   `name` (String) — имя столбца.
-   `type` (String) — тип столбца.
-   `default_kind` (String) — тип выражения (`DEFAULT`, `MATERIALIZED`, `ALIAS`) значения по умолчанию, или пустая строка.
-   `default_expression` (String) — выражение для значения по умолчанию или пустая строка.
-   `data_compressed_bytes` (UInt64) — размер сжатых данных в байтах.
-   `data_uncompressed_bytes` (UInt64) — размер распакованных данных в байтах.
-   `marks_bytes` (UInt64) — размер засечек в байтах.
-   `comment` (String) — комментарий к столбцу или пустая строка.
-   `is_in_partition_key` (UInt8) — флаг, показывающий включение столбца в ключ партиционирования.
-   `is_in_sorting_key` (UInt8) — флаг, показывающий включение столбца в ключ сортировки.
-   `is_in_primary_key` (UInt8) — флаг, показывающий включение столбца в первичный ключ.
-   `is_in_sampling_key` (UInt8) — флаг, показывающий включение столбца в ключ выборки.

## system.contributors {#system-contributors}

Содержит информацию о контрибьютерах. Контрибьютеры расположены в таблице в случайном порядке. Порядок определяется заново при каждом запросе.

Столбцы:

-   `name` (String) — Имя контрибьютера (автора коммита) из git log.

**Пример**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

Чтобы найти себя в таблице, выполните запрос:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## system.databases {#system-databases}

Таблица содержит один столбец name типа String - имя базы данных.
Для каждой базы данных, о которой знает сервер, будет присутствовать соответствующая запись в таблице.
Эта системная таблица используется для реализации запроса `SHOW DATABASES`.

## system.detached\_parts {#system_tables-detached_parts}

Содержит информацию об отсоединённых кусках таблиц семейства [MergeTree](../engines/table-engines/mergetree-family/mergetree.md). Столбец `reason` содержит причину, по которой кусок был отсоединён. Для кусов, отсоединённых пользователем, `reason` содержит пустую строку.
Такие куски могут быть присоединены с помощью [ALTER TABLE ATTACH PARTITION\|PART](../sql_reference/alter/#alter_attach-partition). Остальные столбцы описаны в [system.parts](#system_tables-parts).
Если имя куска некорректно, значения некоторых столбцов могут быть `NULL`. Такие куски могут быть удалены с помощью [ALTER TABLE DROP DETACHED PART](../sql_reference/alter/#alter_drop-detached).

## system.dictionaries {#system_tables-dictionaries}

Содержит информацию о [внешних словарях](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Столбцы:

-   `database` ([String](../sql-reference/data-types/string.md)) — Имя базы данных, в которой находится словарь, созданный с помощью DDL-запроса. Пустая строка для других словарей.
-   `name` ([String](../sql-reference/data-types/string.md)) — [Имя словаря](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../sql-reference/data-types/enum.md)) — Статус словаря. Возможные значения:
    -   `NOT_LOADED` — Словарь не загружен, потому что не использовался.
    -   `LOADED` — Словарь загружен успешно.
    -   `FAILED` — Словарь не загружен в результате ошибки.
    -   `LOADING` — Словарь в процессе загрузки.
    -   `LOADED_AND_RELOADING` — Словарь загружен успешно, сейчас перезагружается (частые причины: запрос [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary), таймаут, изменение настроек словаря).
    -   `FAILED_AND_RELOADING` — Словарь не загружен в результате ошибки, сейчас перезагружается.
-   `origin` ([String](../sql-reference/data-types/string.md)) — Путь к конфигурационному файлу, описывающему словарь.
-   `type` ([String](../sql-reference/data-types/string.md)) — Тип размещения словаря. [Хранение словарей в памяти](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [Тип ключа](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): Числовой ключ ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) или Составной ключ ([String](../sql-reference/data-types/string.md)) — строка вида “(тип 1, тип 2, …, тип n)”.
-   `attribute.names` ([Array](../sql-reference/data-types/array.md)([String](../sql-reference/data-types/string.md))) — Массив [имен атрибутов](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), предоставляемых справочником.
-   `attribute.types` ([Array](../sql-reference/data-types/array.md)([String](../sql-reference/data-types/string.md))) — Соответствующий массив [типов атрибутов](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), предоставляемых справочником.
-   `bytes_allocated` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Объем оперативной памяти, используемый словарем.
-   `query_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество запросов с момента загрузки словаря или с момента последней успешной перезагрузки.
-   `hit_rate` ([Float64](../sql-reference/data-types/float.md)) — Для cache-словарей — процент закэшированных значений.
-   `element_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество элементов, хранящихся в словаре.
-   `load_factor` ([Float64](../sql-reference/data-types/float.md)) — Процент заполнения словаря (для хэшированного словаря — процент заполнения хэш-таблицы).
-   `source` ([String](../sql-reference/data-types/string.md)) — Текст, описывающий [источник данных](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) для словаря.
-   `lifetime_min` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Минимальное [время обновления](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) словаря в памяти, по истечении которого Clickhouse попытается перезагрузить словарь (если задано `invalidate_query`, то только если он изменился). Задается в секундах.
-   `lifetime_max` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Максимальное [время обновления](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) словаря в памяти, по истечении которого Clickhouse попытается перезагрузить словарь (если задано `invalidate_query`, то только если он изменился). Задается в секундах.
-   `loading_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Время начала загрузки словаря.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Время, затраченное на загрузку словаря.
-   `last_exception` ([String](../sql-reference/data-types/string.md)) — Текст ошибки, возникающей при создании или перезагрузке словаря, если словарь не удалось создать.

**Пример**

Настройте словарь.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

Убедитесь, что словарь загружен.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## system.events {#system_tables-events}

Содержит информацию о количестве событий, произошедших в системе. Например, в таблице можно найти, сколько запросов `SELECT` обработано с момента запуска сервера ClickHouse.

Столбцы:

-   `event` ([String](../sql-reference/data-types/string.md)) — имя события.
-   `value` ([UInt64](../sql-reference/data-types/int-uint.md)) — количество произошедших событий.
-   `description` ([String](../sql-reference/data-types/string.md)) — описание события.

**Пример**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   [system.asynchronous\_metrics](#system_tables-asynchronous_metrics) — таблица с периодически вычисляемыми метриками.
-   [system.metrics](#system_tables-metrics) — таблица с мгновенно вычисляемыми метриками.
-   [system.metric\_log](#system_tables-metric_log) — таблица фиксирующая историю значений метрик из `system.metrics` и `system.events`.
-   [Мониторинг](monitoring.md) — основы мониторинга в ClickHouse.

## system.functions {#system-functions}

Содержит информацию об обычных и агрегатных функциях.

Столбцы:

-   `name` (`String`) – Имя функции.
-   `is_aggregate` (`UInt8`) – Признак, является ли функция агрегатной.

## system.graphite\_retentions {#system-graphite-retentions}

Содержит информацию о том, какие параметры [graphite\_rollup](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) используются в таблицах с движками [\*GraphiteMergeTree](../engines/table-engines/mergetree-family/graphitemergetree.md).

Столбцы:

-   `config_name` (String) - Имя параметра, используемого для `graphite_rollup`.
-   `regexp` (String) - Шаблон имени метрики.
-   `function` (String) - Имя агрегирующей функции.
-   `age` (UInt64) - Минимальный возраст данных в секундах.
-   `precision` (UInt64) - Точность определения возраста данных в секундах.
-   `priority` (UInt16) - Приоритет раздела pattern.
-   `is_default` (UInt8) - Является ли раздел pattern дефолтным.
-   `Tables.database` (Array(String)) - Массив имён баз данных таблиц, использующих параметр `config_name`.
-   `Tables.table` (Array(String)) - Массив имён таблиц, использующих параметр `config_name`.

## system.merges {#system-merges}

Содержит информацию о производящихся прямо сейчас слияниях и мутациях кусков для таблиц семейства MergeTree.

Столбцы:

-   `database String` — Имя базы данных, в которой находится таблица.
-   `table String` — Имя таблицы.
-   `elapsed Float64` — Время в секундах, прошедшее от начала выполнения слияния.
-   `progress Float64` — Доля выполненной работы от 0 до 1.
-   `num_parts UInt64` — Количество сливаемых кусков.
-   `result_part_name String` — Имя куска, который будет образован в результате слияния.
-   `is_mutation UInt8` - Является ли данный процесс мутацией куска.
-   `total_size_bytes_compressed UInt64` — Суммарный размер сжатых данных сливаемых кусков.
-   `total_size_marks UInt64` — Суммарное количество засечек в сливаемых кусках.
-   `bytes_read_uncompressed UInt64` — Количество прочитанных байт, разжатых.
-   `rows_read UInt64` — Количество прочитанных строк.
-   `bytes_written_uncompressed UInt64` — Количество записанных байт, несжатых.
-   `rows_written UInt64` — Количество записанных строк.

## system.metrics {#system_tables-metrics}

Содержит метрики, которые могут быть рассчитаны мгновенно или имеют текущее значение. Например, число одновременно обрабатываемых запросов или текущее значение задержки реплики. Эта таблица всегда актуальна.

Столбцы:

-   `metric` ([String](../sql-reference/data-types/string.md)) — название метрики.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — значение метрики.
-   `description` ([String](../sql-reference/data-types/string.md)) — описание метрики.

Список поддержанных метрик смотрите в файле [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp).

**Пример**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   [system.asynchronous\_metrics](#system_tables-asynchronous_metrics) — таблица с периодически вычисляемыми метриками.
-   [system.events](#system_tables-events) — таблица с количеством произошедших событий.
-   [system.metric\_log](#system_tables-metric_log) — таблица фиксирующая историю значений метрик из `system.metrics` и `system.events`.
-   [Мониторинг](monitoring.md) — основы мониторинга в ClickHouse.

## system.metric\_log {#system_tables-metric_log}

Содержит историю значений метрик из таблиц `system.metrics` и `system.events`, периодически сбрасываемую на диск.
Для включения сбора истории метрик в таблице `system.metric_log` создайте `/etc/clickhouse-server/config.d/metric_log.xml` следующего содержания:

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

-   [system.asynchronous\_metrics](#system_tables-asynchronous_metrics) — таблица с периодически вычисляемыми метриками.
-   [system.events](#system_tables-events) — таблица с количеством произошедших событий.
-   [system.metrics](#system_tables-metrics) — таблица с мгновенно вычисляемыми метриками.
-   [Мониторинг](monitoring.md) — основы мониторинга в ClickHouse.

## system.numbers {#system-numbers}

Таблица содержит один столбец с именем number типа UInt64, содержащим почти все натуральные числа, начиная с нуля.
Эту таблицу можно использовать для тестов, а также если вам нужно сделать перебор.
Чтения из этой таблицы не распараллеливаются.

## system.numbers\_mt {#system-numbers-mt}

То же самое, что и system.numbers, но чтение распараллеливается. Числа могут возвращаться в произвольном порядке.
Используется для тестов.

## system.one {#system-one}

Таблица содержит одну строку с одним столбцом dummy типа UInt8, содержащим значение 0.
Эта таблица используется, если в SELECT запросе не указана секция FROM.
То есть, это - аналог таблицы DUAL, которую можно найти в других СУБД.

## system.parts {#system_tables-parts}

Содержит информацию о кусках данных таблиц семейства [MergeTree](../engines/table-engines/mergetree-family/mergetree.md).

Каждая строка описывает один кусок данных.

Столбцы:

-   `partition` (`String`) – Имя партиции. Что такое партиция можно узнать из описания запроса [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter).

    Форматы:

    -   `YYYYMM` для автоматической схемы партиционирования по месяцам.
    -   `any_string` при партиционировании вручную.

-   `name` (`String`) – имя куска.

-   `active` (`UInt8`) – признак активности. Если кусок активен, то он используется таблицей, в противном случает он будет удален. Неактивные куски остаются после слияний.

-   `marks` (`UInt64`) – количество засечек. Чтобы получить примерное количество строк в куске, умножьте `marks` на гранулированность индекса (обычно 8192).

-   `rows` (`UInt64`) – количество строк.

-   `bytes_on_disk` (`UInt64`) – общий размер всех файлов кусков данных в байтах.

-   `data_compressed_bytes` (`UInt64`) – общий размер сжатой информации в куске данных. Размер всех дополнительных файлов (например, файлов с засечками) не учитывается.

-   `data_uncompressed_bytes` (`UInt64`) – общий размер распакованной информации куска данных. Размер всех дополнительных файлов (например, файлов с засечками) не учитывается.

-   `marks_bytes` (`UInt64`) – размер файла с засечками.

-   `modification_time` (`DateTime`) – время модификации директории с куском данных. Обычно соответствует времени создания куска.

-   `remove_time` (`DateTime`) – время, когда кусок стал неактивным.

-   `refcount` (`UInt32`) – количество мест, в котором кусок используется. Значение больше 2 говорит о том, что кусок участвует в запросах или в слияниях.

-   `min_date` (`Date`) – минимальное значение ключа даты в куске данных.

-   `max_date` (`Date`) – максимальное значение ключа даты в куске данных.

-   `min_time` (`DateTime`) – минимальное значение даты и времени в куске данных.

-   `max_time`(`DateTime`) – максимальное значение даты и времени в куске данных.

-   `partition_id` (`String`) – ID партиции.

-   `min_block_number` (`UInt64`) – минимальное число кусков, из которых состоит текущий после слияния.

-   `max_block_number` (`UInt64`) – максимальное число кусков, из которых состоит текущий после слияния.

-   `level` (`UInt32`) - глубина дерева слияний. Если слияний не было, то `level=0`.

-   `data_version` (`UInt64`) – число, которое используется для определения того, какие мутации необходимо применить к куску данных (мутации с версией большей, чем `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – объём памяти (в байтах), занимаемой значениями первичных ключей.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – объём памяти (в байтах) выделенный для размещения первичных ключей.

-   `is_frozen` (`UInt8`) – Признак, показывающий существование бэкапа партиции. 1, бэкап есть. 0, бэкапа нет. Смотрите раздел [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition).

-   `database` (`String`) – имя базы данных.

-   `table` (`String`) – имя таблицы.

-   `engine` (`String`) – имя движка таблицы, без параметров.

-   `path` (`String`) – абсолютный путь к папке с файлами кусков данных.

-   `disk` (`String`) – имя диска, на котором находится кусок данных.

-   `hash_of_all_files` (`String`) – значение [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) для сжатых файлов.

-   `hash_of_uncompressed_files` (`String`) – значение [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) несжатых файлов (файлы с засечками, первичным ключом и пр.)

-   `uncompressed_hash_of_compressed_files` (`String`) – значение [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) данных в сжатых файлах как если бы они были разжатыми.

-   `bytes` (`UInt64`) – алиас для `bytes_on_disk`.

-   `marks_size` (`UInt64`) – алиас для `marks_bytes`.

## system.part\_log {#system_tables-part-log}

Системная таблица `system.part_log` создается только в том случае, если задана серверная настройка [part\_log](server-configuration-parameters/settings.md#server_configuration_parameters-part-log).

Содержит информацию о всех событиях, произошедших с [кусками данных](../engines/table-engines/mergetree-family/custom-partitioning-key.md) таблиц семейства [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) (например, события добавления, удаления или слияния данных).

Столбцы:

-   `event_type` (Enum) — тип события. Столбец может содержать одно из следующих значений:
    -   `NEW_PART` — вставка нового куска.
    -   `MERGE_PARTS` — слияние кусков.
    -   `DOWNLOAD_PART` — загрузка с реплики.
    -   `REMOVE_PART` — удаление или отсоединение из таблицы с помощью [DETACH PARTITION](../sql-reference/statements/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — изменение куска.
    -   `MOVE_PART` — перемещение куска между дисками.
-   `event_date` (Date) — дата события.
-   `event_time` (DateTime) — время события.
-   `duration_ms` (UInt64) — длительность.
-   `database` (String) — имя базы данных, в которой находится кусок.
-   `table` (String) — имя таблицы, в которой находится кусок.
-   `part_name` (String) — имя куска.
-   `partition_id` (String) — идентификатор партиции, в которую был добавлен кусок. В столбце будет значение ‘all’, если таблица партициируется по выражению `tuple()`.
-   `rows` (UInt64) — число строк в куске.
-   `size_in_bytes` (UInt64) — размер куска данных в байтах.
-   `merged_from` (Array(String)) — массив имён кусков, из которых образован текущий кусок в результате слияния (также столбец заполняется в случае скачивания уже смерженного куска).
-   `bytes_uncompressed` (UInt64) — количество прочитанных разжатых байт.
-   `read_rows` (UInt64) — сколько было прочитано строк при слиянии кусков.
-   `read_bytes` (UInt64) — сколько было прочитано байт при слиянии кусков.
-   `error` (UInt16) — код ошибки, возникшей при текущем событии.
-   `exception` (String) — текст ошибки.

Системная таблица `system.part_log` будет создана после первой вставки данных в таблицу `MergeTree`.

## system.processes {#system_tables-processes}

Используется для реализации запроса `SHOW PROCESSLIST`.

Столбцы:

-   `user` (String) – пользователь, инициировавший запрос. При распределённом выполнении запросы отправляются на удалённые серверы от имени пользователя `default`. Поле содержит имя пользователя для конкретного запроса, а не для запроса, который иницировал этот запрос.
-   `address` (String) – IP-адрес, с которого пришёл запрос. При распределённой обработке запроса аналогично. Чтобы определить откуда запрос пришел изначально, необходимо смотреть таблицу `system.processes` на сервере-источнике запроса.
-   `elapsed` (Float64) – время в секундах с начала обработки запроса.
-   `rows_read` (UInt64) – количество прочитанных строк. При распределённой обработке запроса на сервере-инициаторе запроса представляет собой сумму по всем удалённым серверам.
-   `bytes_read` (UInt64) – количество прочитанных из таблиц байт, в несжатом виде. При распределённой обработке запроса на сервере-инициаторе запроса представляет собой сумму по всем удалённым серверам.
-   `total_rows_approx` (UInt64) – приблизительная оценка общего количества строк, которые должны быть прочитаны. При распределённой обработке запроса, на сервере-инициаторе запроса, представляет собой сумму по всем удалённым серверам. Может обновляться в процессе выполнения запроса, когда становятся известны новые источники для обработки.
-   `memory_usage` (UInt64) – потребление памяти запросом. Может не учитывать некоторые виды выделенной памяти. Смотрите описание настройки [max\_memory\_usage](../operations/settings/query-complexity.md#settings_max_memory_usage).
-   `query` (String) – текст запроса. Для запросов `INSERT` не содержит встаявляемые данные.
-   `query_id` (String) – идентификатор запроса, если был задан.

## system.query\_log {#system_tables-query_log}

Содержит информацию о выполнении запросов. Для каждого запроса вы можете увидеть время начала обработки, продолжительность обработки, сообщения об ошибках и другую информацию.

!!! note "Внимание"
    Таблица не содержит входных данных для запросов `INSERT`.

ClickHouse создаёт таблицу только в том случае, когда установлен конфигурационный параметр сервера [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log). Параметр задаёт правила ведения лога, такие как интервал логирования или имя таблицы, в которую будут логгироваться запросы.

Чтобы включить логирование, задайте значение параметра [log\_queries](settings/settings.md#settings-log-queries) равным 1. Подробности смотрите в разделе [Настройки](settings/settings.md#settings).

Таблица `system.query_log` содержит информацию о двух видах запросов:

1.  Первоначальные запросы, которые были выполнены непосредственно клиентом.
2.  Дочерние запросы, инициированные другими запросами (для выполнения распределенных запросов). Для дочерних запросов информация о первоначальном запросе содержится в столбцах `initial_*`.

Столбцы:

-   `type` (`Enum8`) — тип события, произошедшего при выполнении запроса. Значения:
    -   `'QueryStart' = 1` — успешное начало выполнения запроса.
    -   `'QueryFinish' = 2` — успешное завершение выполнения запроса.
    -   `'ExceptionBeforeStart' = 3` — исключение перед началом обработки запроса.
    -   `'ExceptionWhileProcessing' = 4` — исключение во время обработки запроса.
-   `event_date` (Date) — дата начала запроса.
-   `event_time` (DateTime) — время начала запроса.
-   `query_start_time` (DateTime) — время начала обработки запроса.
-   `query_duration_ms` (UInt64) — длительность обработки запроса.
-   `read_rows` (UInt64) — количество прочитанных строк.
-   `read_bytes` (UInt64) — количество прочитанных байтов.
-   `written_rows` (UInt64) — количество записанных строк для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `written_bytes` (UInt64) — объём записанных данных в байтах для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `result_rows` (UInt64) — количество строк в результате.
-   `result_bytes` (UInt64) — объём результата в байтах.
-   `memory_usage` (UInt64) — потребление RAM запросом.
-   `query` (String) — текст запроса.
-   `exception` (String) — сообщение исключения, если запрос завершился по исключению.
-   `stack_trace` (String) — трассировка (список функций, последовательно вызванных перед ошибкой). Пустая строка, если запрос успешно завершен.
-   `is_initial_query` (UInt8) — вид запроса. Возможные значения:
    -   1 — запрос был инициирован клиентом.
    -   0 — запрос был инициирован другим запросом при распределенном запросе.
-   `user` (String) — пользователь, запустивший текущий запрос.
-   `query_id` (String) — ID запроса.
-   `address` (IPv6) — IP адрес, с которого пришел запрос.
-   `port` (UInt16) — порт, с которого клиент сделал запрос
-   `initial_user` (String) — пользователь, запустивший первоначальный запрос (для распределенных запросов).
-   `initial_query_id` (String) — ID родительского запроса.
-   `initial_address` (IPv6) — IP адрес, с которого пришел родительский запрос.
-   `initial_port` (UInt16) — порт, с которого клиент сделал родительский запрос.
-   `interface` (UInt8) — интерфейс, с которого ушёл запрос. Возможные значения:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — имя пользователя в OS, который запустил [clickhouse-client](../interfaces/cli.md).
-   `client_hostname` (String) — имя сервера, с которого присоединился [clickhouse-client](../interfaces/cli.md) или другой TCP клиент.
-   `client_name` (String) — [clickhouse-client](../interfaces/cli.md) или другой TCP клиент.
-   `client_revision` (UInt32) — ревизия [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `client_version_major` (UInt32) — старшая версия [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `client_version_minor` (UInt32) — младшая версия [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `client_version_patch` (UInt32) — патч [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `http_method` (UInt8) — HTTP метод, инициировавший запрос. Возможные значения:
    -   0 — запрос запущен с интерфейса TCP.
    -   1 — `GET`.
    -   2 — `POST`.
-   `http_user_agent` (String) — HTTP заголовок `UserAgent`.
-   `quota_key` (String) — «ключ квоты» из настроек [квот](quotas.md) (см. `keyed`).
-   `revision` (UInt32) — ревизия ClickHouse.
-   `thread_numbers` (Array(UInt32)) — количество потоков, участвующих в обработке запросов.
-   `ProfileEvents.Names` (Array(String)) — Счетчики для изменения различных метрик. Описание метрик можно получить из таблицы [system.events](#system_tables-events)(\#system\_tables-events
-   `ProfileEvents.Values` (Array(UInt64)) — метрики, перечисленные в столбце `ProfileEvents.Names`.
-   `Settings.Names` (Array(String)) — имена настроек, которые меняются, когда клиент выполняет запрос. Чтобы разрешить логирование изменений настроек, установите параметр `log_query_settings` равным 1.
-   `Settings.Values` (Array(String)) — Значения настроек, которые перечислены в столбце `Settings.Names`.

Каждый запрос создаёт одну или две строки в таблице `query_log`, в зависимости от статуса запроса:

1.  Если запрос выполнен успешно, создаются два события типа 1 и 2 (смотрите столбец `type`).
2.  Если во время обработки запроса произошла ошибка, создаются два события с типами 1 и 4.
3.  Если ошибка произошла до запуска запроса, создается одно событие с типом 3.

По умолчанию, строки добавляются в таблицу логирования с интервалом в 7,5 секунд. Можно задать интервал в конфигурационном параметре сервера [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) (смотрите параметр `flush_interval_milliseconds`). Чтобы принудительно записать логи из буффера памяти в таблицу, используйте запрос `SYSTEM FLUSH LOGS`.

Если таблицу удалить вручную, она пересоздастся автоматически «на лету». При этом все логи на момент удаления таблицы будут удалены.

!!! note "Примечание"
    Срок хранения логов не ограничен. Логи не удаляются из таблицы автоматически. Вам необходимо самостоятельно организовать удаление устаревших логов.

Можно указать произвольный ключ партиционирования для таблицы `system.query_log` в конфигурации [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) (параметр `partition_by`).

## system.query\_log {#system_tables-query_log}

Contains information about execution of queries. For each query, you can see processing start time, duration of processing, error messages and other information.

!!! note "Note"
    The table doesn’t contain input data for `INSERT` queries.

ClickHouse creates this table only if the [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) server parameter is specified. This parameter sets the logging rules, such as the logging interval or the name of the table the queries will be logged in.

To enable query logging, set the [log\_queries](settings/settings.md#settings-log-queries) parameter to 1. For details, see the [Settings](settings/settings.md) section.

The `system.query_log` table registers two kinds of queries:

1.  Initial queries that were run directly by the client.
2.  Child queries that were initiated by other queries (for distributed query execution). For these types of queries, information about the parent queries is shown in the `initial_*` columns.

Columns:

-   `type` (`Enum8`) — Type of event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` (Date) — Query starting date.
-   `event_time` (DateTime) — Query starting time.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` queries, the number of written rows. For other queries, the column value is 0.
-   `written_bytes` (UInt64) — For `INSERT` queries, the number of written bytes. For other queries, the column value is 0.
-   `result_rows` (UInt64) — Number of rows in the result.
-   `result_bytes` (UInt64) — Number of bytes in the result.
-   `memory_usage` (UInt64) — Memory consumption by the query.
-   `query` (String) — Query string.
-   `exception` (String) — Exception message.
-   `stack_trace` (String) — Stack trace (a list of methods called before the error occurred). An empty string, if the query is completed successfully.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS’s username who runs [clickhouse-client](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [clickhouse-client](../interfaces/cli.md) or another TCP client is run.
-   `client_name` (String) — The [clickhouse-client](../interfaces/cli.md) or another TCP client name.
-   `client_revision` (UInt32) — Revision of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_major` (UInt32) — Major version of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_minor` (UInt32) — Minor version of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_patch` (UInt32) — Patch component of the [clickhouse-client](../interfaces/cli.md) or another TCP client version.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` method was used.
    -   2 — `POST` method was used.
-   `http_user_agent` (String) — The `UserAgent` header passed in the HTTP request.
-   `quota_key` (String) — The «quota key» specified in the [quotas](quotas.md) setting (see `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `thread_numbers` (Array(UInt32)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics. The description of them could be found in the table [system.events](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics that are listed in the `ProfileEvents.Names` column.
-   `Settings.Names` (Array(String)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` parameter to 1.
-   `Settings.Values` (Array(String)) — Values of settings that are listed in the `Settings.Names` column.

Each query creates one or two rows in the `query_log` table, depending on the status of the query:

1.  If the query execution is successful, two events with types 1 and 2 are created (see the `type` column).
2.  If an error occurred during query processing, two events with types 1 and 4 are created.
3.  If an error occurred before launching the query, a single event with type 3 is created.

By default, logs are added to the table at intervals of 7.5 seconds. You can set this interval in the [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) server setting (see the `flush_interval_milliseconds` parameter). To flush the logs forcibly from the memory buffer into the table, use the `SYSTEM FLUSH LOGS` query.

When the table is deleted manually, it will be automatically created on the fly. Note that all the previous logs will be deleted.

!!! note "Note"
    The storage period for logs is unlimited. Logs aren’t automatically deleted from the table. You need to organize the removal of outdated logs yourself.

You can specify an arbitrary partitioning key for the `system.query_log` table in the [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) server setting (see the `partition_by` parameter).
\#\# system.query\_thread\_log {\#system\_tables-query-thread-log}

Содержит информацию о каждом потоке выполняемых запросов.

ClickHouse создаёт таблицу только в том случае, когда установлен конфигурационный параметр сервера [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log). Параметр задаёт правила ведения лога, такие как интервал логирования или имя таблицы, в которую будут логгироваться запросы.

Чтобы включить логирование, задайте значение параметра [log\_query\_threads](settings/settings.md#settings-log-query-threads) равным 1. Подробности смотрите в разделе [Настройки](settings/settings.md#settings).

Столбцы:

-   `event_date` (Date) — дата завершения выполнения запроса потоком.
-   `event_time` (DateTime) — дата и время завершения выполнения запроса потоком.
-   `query_start_time` (DateTime) — время начала обработки запроса.
-   `query_duration_ms` (UInt64) — длительность обработки запроса в миллисекундах.
-   `read_rows` (UInt64) — количество прочитанных строк.
-   `read_bytes` (UInt64) — количество прочитанных байтов.
-   `written_rows` (UInt64) — количество записанных строк для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `written_bytes` (UInt64) — объём записанных данных в байтах для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `memory_usage` (Int64) — разница между выделенной и освобождённой памятью в контексте потока.
-   `peak_memory_usage` (Int64) — максимальная разница между выделенной и освобождённой памятью в контексте потока.
-   `thread_name` (String) — Имя потока.
-   `thread_id` (UInt64) — tid (ID потока операционной системы).
-   `master_thread_id` (UInt64) — tid (ID потока операционной системы) главного потока.
-   `query` (String) — текст запроса.
-   `is_initial_query` (UInt8) — вид запроса. Возможные значения:
    -   1 — запрос был инициирован клиентом.
    -   0 — запрос был инициирован другим запросом при распределенном запросе.
-   `user` (String) — пользователь, запустивший текущий запрос.
-   `query_id` (String) — ID запроса.
-   `address` (IPv6) — IP адрес, с которого пришел запрос.
-   `port` (UInt16) — порт, с которого пришел запрос.
-   `initial_user` (String) — пользователь, запустивший первоначальный запрос (для распределенных запросов).
-   `initial_query_id` (String) — ID родительского запроса.
-   `initial_address` (IPv6) — IP адрес, с которого пришел родительский запрос.
-   `initial_port` (UInt16) — порт, пришел родительский запрос.
-   `interface` (UInt8) — интерфейс, с которого ушёл запрос. Возможные значения:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — имя пользователя в OS, который запустил [clickhouse-client](../interfaces/cli.md).
-   `client_hostname` (String) — hostname клиентской машины, с которой присоединился [clickhouse-client](../interfaces/cli.md) или другой TCP клиент.
-   `client_name` (String) — [clickhouse-client](../interfaces/cli.md) или другой TCP клиент.
-   `client_revision` (UInt32) — ревизия [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `client_version_major` (UInt32) — старшая версия [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `client_version_minor` (UInt32) — младшая версия [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `client_version_patch` (UInt32) — патч [clickhouse-client](../interfaces/cli.md) или другого TCP клиента.
-   `http_method` (UInt8) — HTTP метод, инициировавший запрос. Возможные значения:
    -   0 — запрос запущен с интерфейса TCP.
    -   1 — `GET`.
    -   2 — `POST`.
-   `http_user_agent` (String) — HTTP заголовок `UserAgent`.
-   `quota_key` (String) — «ключ квоты» из настроек [квот](quotas.md) (см. `keyed`).
-   `revision` (UInt32) — ревизия ClickHouse.
-   `ProfileEvents.Names` (Array(String)) — Счетчики для изменения различных метрик для данного потока. Описание метрик можно получить из таблицы [system.events](#system_tables-events)(\#system\_tables-events
-   `ProfileEvents.Values` (Array(UInt64)) — метрики для данного потока, перечисленные в столбце `ProfileEvents.Names`.

По умолчанию, строки добавляются в таблицу логирования с интервалом в 7,5 секунд. Можно задать интервал в конфигурационном параметре сервера [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) (смотрите параметр `flush_interval_milliseconds`). Чтобы принудительно записать логи из буффера памяти в таблицу, используйте запрос `SYSTEM FLUSH LOGS`.

Если таблицу удалить вручную, она пересоздастся автоматически «на лету». При этом все логи на момент удаления таблицы будут удалены.

!!! note "Примечание"
    Срок хранения логов не ограничен. Логи не удаляются из таблицы автоматически. Вам необходимо самостоятельно организовать удаление устаревших логов.

Можно указать произвольный ключ партиционирования для таблицы `system.query_log` в конфигурации [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) (параметр `partition_by`).

## system.query_thread_log {#system_tables-query-thread-log}

Содержит информацию о каждом потоке исполнения запроса.

## system.trace\_log {#system_tables-trace_log}

Contains stack traces collected by the sampling query profiler.

ClickHouse creates this table when the [trace\_log](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) server configuration section is set. Also the [query\_profiler\_real\_time\_period\_ns](settings/settings.md#query_profiler_real_time_period_ns) and [query\_profiler\_cpu\_time\_period\_ns](settings/settings.md#query_profiler_cpu_time_period_ns) settings should be set.

To analyze logs, use the `addressToLine`, `addressToSymbol` and `demangle` introspection functions.

Columns:

-   `event_date`([Date](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time`([DateTime](../sql-reference/data-types/datetime.md)) — Timestamp of sampling moment.

-   `revision`([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

        When connecting to server by `clickhouse-client`, you see the string similar to `Connected to ClickHouse server version 19.18.1 revision 54429.`. This field contains the `revision`, but not the `version` of a server.

-   `timer_type`([Enum8](../sql-reference/data-types/enum.md)) — Timer type:

        - `Real` represents wall-clock time.
        - `CPU` represents CPU time.

-   `thread_number`([UInt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id`([String](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query\_log](#system_tables-query_log) system table.

-   `trace`([Array(UInt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**Example**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

## system.replicas {#system_tables-replicas}

Содержит информацию и статус для реплицируемых таблиц, расположенных на локальном сервере.
Эту таблицу можно использовать для мониторинга. Таблица содержит по строчке для каждой Replicated\*-таблицы.

Пример:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        0000-00-00 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 0000-00-00 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

Столбцы:

-   `database` (`String`) - имя БД.
-   `table` (`String`) - имя таблицы.
-   `engine` (`String`) - имя движка таблицы.
-   `is_leader` (`UInt8`) - является ли реплика лидером.
    В один момент времени, не более одной из реплик является лидером. Лидер отвечает за выбор фоновых слияний, которые следует произвести.
    Замечу, что запись можно осуществлять на любую реплику (доступную и имеющую сессию в ZK), независимо от лидерства.
-   `can_become_leader` (`UInt8`) - может ли реплика быть выбрана лидером.
-   `is_readonly` (`UInt8`) - находится ли реплика в режиме «только для чтения»
    Этот режим включается, если в конфиге нет секции с ZK; если при переинициализации сессии в ZK произошла неизвестная ошибка; во время переинициализации сессии с ZK.
-   `is_session_expired` (`UInt8`) - истекла ли сессия с ZK. В основном, то же самое, что и `is_readonly`.
-   `future_parts` (`UInt32`) - количество кусков с данными, которые появятся в результате INSERT-ов или слияний, которых ещё предстоит сделать
-   `parts_to_check` (`UInt32`) - количество кусков с данными в очереди на проверку. Кусок помещается в очередь на проверку, если есть подозрение, что он может быть битым.
-   `zookeeper_path` (`String`) - путь к данным таблицы в ZK.
-   `replica_name` (`String`) - имя реплики в ZK; разные реплики одной таблицы имеют разное имя.
-   `replica_path` (`String`) - путь к данным реплики в ZK. То же самое, что конкатенация zookeeper\_path/replicas/replica\_path.
-   `columns_version` (`Int32`) - номер версии структуры таблицы. Обозначает, сколько раз был сделан ALTER. Если на репликах разные версии, значит некоторые реплики сделали ещё не все ALTER-ы.
-   `queue_size` (`UInt32`) - размер очереди действий, которые предстоит сделать. К действиям относятся вставки блоков данных, слияния, и некоторые другие действия. Как правило, совпадает с future\_parts.
-   `inserts_in_queue` (`UInt32`) - количество вставок блоков данных, которые предстоит сделать. Обычно вставки должны быстро реплицироваться. Если величина большая - значит что-то не так.
-   `merges_in_queue` (`UInt32`) - количество слияний, которые предстоит сделать. Бывают длинные слияния - то есть, это значение может быть больше нуля продолжительное время.
-   `part_mutations_in_queue` (`UInt32`) - количество мутаций, которые предстоит сделать.
-   `queue_oldest_time` (`DateTime`) - если `queue_size` больше 0, показывает, когда была добавлена в очередь самая старая операция.
-   `inserts_oldest_time` (`DateTime`) - см. `queue_oldest_time`.
-   `merges_oldest_time` (`DateTime`) - см. `queue_oldest_time`.
-   `part_mutations_oldest_time` (`DateTime`) - см. `queue_oldest_time`.

Следующие 4 столбца имеют ненулевое значение только если активна сессия с ZK.

-   `log_max_index` (`UInt64`) - максимальный номер записи в общем логе действий.
-   `log_pointer` (`UInt64`) - максимальный номер записи из общего лога действий, которую реплика скопировала в свою очередь для выполнения, плюс единица. Если log\_pointer сильно меньше log\_max\_index, значит что-то не так.
-   `last_queue_update` (`DateTime`) - When the queue was updated last time.
-   `absolute_delay` (`UInt64`) - How big lag in seconds the current replica has.
-   `total_replicas` (`UInt8`) - общее число известных реплик этой таблицы.
-   `active_replicas` (`UInt8`) - число реплик этой таблицы, имеющих сессию в ZK; то есть, число работающих реплик.

Если запрашивать все столбцы, то таблица может работать слегка медленно, так как на каждую строчку делается несколько чтений из ZK.
Если не запрашивать последние 4 столбца (log\_max\_index, log\_pointer, total\_replicas, active\_replicas), то таблица работает быстро.

Например, так можно проверить, что всё хорошо:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

Если этот запрос ничего не возвращает - значит всё хорошо.

## system.settings {#system-tables-system-settings}

Содержит информацию о сессионных настройках для текущего пользователя.

Столбцы:

-   `name` ([String](../sql-reference/data-types/string.md)) — имя настройки.
-   `value` ([String](../sql-reference/data-types/string.md)) — значение настройки.
-   `changed` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — показывает, изменена ли настройка по отношению к значению по умолчанию.
-   `description` ([String](../sql-reference/data-types/string.md)) — краткое описание настройки.
-   `min` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — минимальное значение настройки, если задано [ограничение](settings/constraints-on-settings.md#constraints-on-settings). Если нет, то поле содержит [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — максимальное значение настройки, если задано [ограничение](settings/constraints-on-settings.md#constraints-on-settings). Если нет, то поле содержит [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Показывает, может ли пользователь изменять настройку:
    -   `0` — Текущий пользователь может изменять настройку.
    -   `1` — Текущий пользователь не может изменять настройку.

**Пример**

Пример показывает как получить информацию о настройках, имена которых содержат `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

Использование `WHERE changed` может быть полезно, например, если необходимо проверить:

-   Что настройки корректно загрузились из конфигурационного файла и используются.
-   Настройки, изменённые в текущей сессии.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**Cм. также**

-   [Настройки](settings/index.md#settings)
-   [Разрешения для запросов](settings/permissions-for-queries.md#settings_readonly)
-   [Ограничения для значений настроек](settings/constraints-on-settings.md)

## system.table\_engines {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┬─changed─┐
│ max_threads            │ 8           │       1 │
│ use_uncompressed_cache │ 0           │       1 │
│ load_balancing         │ random      │       1 │
│ max_memory_usage       │ 10000000000 │       1 │
└────────────────────────┴─────────────┴─────────┘
```

## system.table\_engines {#system-table-engines}

Содержит информацию про движки таблиц, поддерживаемые сервером, а также об их возможностях.

Эта таблица содержит следующие столбцы (тип столбца показан в скобках):

-   `name` (String) — имя движка.
-   `supports_settings` (UInt8) — флаг, показывающий поддержку секции `SETTINGS`.
-   `supports_skipping_indices` (UInt8) — флаг, показывающий поддержку [индексов пропуска данных](table_engines/mergetree/#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — флаг, показывающий поддержку [TTL](table_engines/mergetree/#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — флаг, показывающий поддержку секций `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` и `SAMPLE_BY`.
-   `supports_replication` (UInt8) — флаг, показывающий поддержку [репликации](../engines/table-engines/mergetree-family/replication.md).
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

-   [Секции движка](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses) семейства MergeTree
-   [Настройки](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table) Kafka
-   [Настройки](../engines/table-engines/special/join.md#join-limitations-and-settings) Join

## system.tables {#system-tables}

Содержит метаданные каждой таблицы, о которой знает сервер. Отсоединённые таблицы не отображаются в `system.tables`.

Эта таблица содержит следующие столбцы (тип столбца показан в скобках):

-   `database String` — имя базы данных, в которой находится таблица.
-   `name` (String) — имя таблицы.
-   `engine` (String) — движок таблицы (без параметров).
-   `is_temporary` (UInt8) — флаг, указывающий на то, временная это таблица или нет.
-   `data_path` (String) — путь к данным таблицы в файловой системе.
-   `metadata_path` (String) — путь к табличным метаданным в файловой системе.
-   `metadata_modification_time` (DateTime) — время последней модификации табличных метаданных.
-   `dependencies_database` (Array(String)) — зависимости базы данных.
-   `dependencies_table` (Array(String)) — табличные зависимости (таблицы [MaterializedView](../engines/table-engines/special/materializedview.md), созданные на базе текущей таблицы).
-   `create_table_query` (String) — запрос, которым создавалась таблица.
-   `engine_full` (String) — параметры табличного движка.
-   `partition_key` (String) — ключ партиционирования таблицы.
-   `sorting_key` (String) — ключ сортировки таблицы.
-   `primary_key` (String) - первичный ключ таблицы.
-   `sampling_key` (String) — ключ сэмплирования таблицы.

Таблица `system.tables` используется при выполнении запроса `SHOW TABLES`.

## system.zookeeper {#system-zookeeper}

Таблицы не существует, если ZooKeeper не сконфигурирован. Позволяет читать данные из ZooKeeper кластера, описанного в конфигурации.
В запросе обязательно в секции WHERE должно присутствовать условие на равенство path - путь в ZooKeeper, для детей которого вы хотите получить данные.

Запрос `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` выведет данные по всем детям узла `/clickhouse`.
Чтобы вывести данные по всем узлам в корне, напишите path = ‘/’.
Если узла, указанного в path не существует, то будет брошено исключение.

Столбцы:

-   `name String` — Имя узла.
-   `path String` — Путь к узлу.
-   `value String` — Значение узла.
-   `dataLength Int32` — Размер значения.
-   `numChildren Int32` — Количество детей.
-   `czxid Int64` — Идентификатор транзакции, в которой узел был создан.
-   `mzxid Int64` — Идентификатор транзакции, в которой узел был последний раз изменён.
-   `pzxid Int64` — Идентификатор транзакции, последний раз удаливший или добавивший детей.
-   `ctime DateTime` — Время создания узла.
-   `mtime DateTime` — Время последней модификации узла.
-   `version Int32` — Версия узла - количество раз, когда узел был изменён.
-   `cversion Int32` — Количество добавлений или удалений детей.
-   `aversion Int32` — Количество изменений ACL.
-   `ephemeralOwner Int64` — Для эфемерных узлов - идентификатор сессии, которая владеет этим узлом.

Пример:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```

## system.mutations {#system_tables-mutations}

Таблица содержит информацию о ходе выполнения [мутаций](../sql-reference/statements/alter.md#alter-mutations) MergeTree-таблиц. Каждой команде мутации соответствует одна строка. В таблице есть следующие столбцы:

**database**, **table** - имя БД и таблицы, к которой была применена мутация.

**mutation\_id** - ID запроса. Для реплицированных таблиц эти ID соответствуют именам записей в директории `<table_path_in_zookeeper>/mutations/` в ZooKeeper, для нереплицированных - именам файлов в директории с данными таблицы.

**command** - Команда мутации (часть запроса после `ALTER TABLE [db.]table`).

**create\_time** - Время создания мутации.

**block\_numbers.partition\_id**, **block\_numbers.number** - Nested-столбец. Для мутаций реплицированных таблиц для каждой партиции содержит номер блока, полученный этой мутацией (в каждой партиции будут изменены только куски, содержащие блоки с номерами, меньшими номера, полученного мутацией в этой партиции). Для нереплицированных таблиц нумерация блоков сквозная по партициям, поэтому столбец содержит одну запись с единственным номером блока, полученным мутацией.

**parts\_to\_do** - Количество кусков таблицы, которые ещё предстоит изменить.

**is\_done** - Завершена ли мутация. Замечание: даже если `parts_to_do = 0`, для реплицированной таблицы возможна ситуация, когда мутация ещё не завершена из-за долго выполняющейся вставки, которая добавляет данные, которые нужно будет мутировать.

Если во время мутации какого-либо куска возникли проблемы, заполняются следующие столбцы:

**latest\_failed\_part** - Имя последнего куска, мутация которого не удалась.

**latest\_fail\_time** — время последней ошибки мутации.

**latest\_fail\_reason** — причина последней ошибки мутации.

## system.disks {#system_tables-disks}

Cодержит информацию о дисках, заданных в [конфигурации сервера](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Столбцы:

-   `name` ([String](../sql-reference/data-types/string.md)) — имя диска в конфигурации сервера.
-   `path` ([String](../sql-reference/data-types/string.md)) — путь к точке монтирования в файловой системе.
-   `free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — свободное место на диске в байтах.
-   `total_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — объём диска в байтах.
-   `keep_free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — место, которое должно остаться свободным на диске в байтах. Задаётся значением параметра `keep_free_space_bytes` конфигурации дисков.

## system.storage\_policies {#system_tables-storage_policies}

Содержит информацию о политиках хранения и томах, заданных в [конфигурации сервера](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Столбцы:

-   `policy_name` ([String](../sql-reference/data-types/string.md)) — имя политики хранения.
-   `volume_name` ([String](../sql-reference/data-types/string.md)) — имя тома, который содержится в политике хранения.
-   `volume_priority` ([UInt64](../sql-reference/data-types/int-uint.md)) — порядковый номер тома согласно конфигурации.
-   `disks` ([Array(String)](../sql-reference/data-types/array.md)) — имена дисков, содержащихся в политике хранения.
-   `max_data_part_size` ([UInt64](../sql-reference/data-types/int-uint.md)) — максимальный размер куска данных, который может храниться на дисках тома (0 — без ограничений).
-   `move_factor` ([Float64](../sql-reference/data-types/float.md))\` — доля свободного места, при превышении которой данные начинают перемещаться на следующий том.

Если политика хранения содержит несколько томов, то каждому тому соответствует отдельная запись в таблице.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/) <!--hide-->
