---
sidebar_position: 60
sidebar_label: "Настройки"
slug: /ru/operations/settings/settings
---

# Настройки {#settings}

## distributed_product_mode {#distributed-product-mode}

Изменяет поведение [распределенных подзапросов](../../sql-reference/operators/index.md).

ClickHouse применяет настройку в тех случаях, когда запрос содержит произведение распределённых таблиц, т.е. когда запрос к распределенной таблице содержит не-GLOBAL подзапрос к также распределенной таблице.

Условия применения:

-   Только подзапросы для IN, JOIN.
-   Только если в секции FROM используется распределённая таблица, содержащая более одного шарда.
-   Если подзапрос касается распределенной таблицы, содержащей более одного шарда.
-   Не используется в случае табличной функции [remote](../../sql-reference/table-functions/remote.md).

Возможные значения:

-   `deny` — значение по умолчанию. Запрещает использование таких подзапросов (При попытке использование вернет исключение «Double-distributed IN/JOIN subqueries is denied»);
-   `local` — заменяет базу данных и таблицу в подзапросе на локальные для конечного сервера (шарда), оставив обычный `IN`/`JOIN.`
-   `global` — заменяет запрос `IN`/`JOIN` на `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — разрешает использование таких подзапросов.

## prefer_global_in_and_join {#prefer-global-in-and-join}

Заменяет запрос `IN`/`JOIN` на `GLOBAL IN`/`GLOBAL JOIN`.

Возможные значения:

-   0 — выключена. Операторы `IN`/`JOIN` не заменяются на `GLOBAL IN`/`GLOBAL JOIN`.
-   1 — включена. Операторы `IN`/`JOIN` заменяются на `GLOBAL IN`/`GLOBAL JOIN`.

Значение по умолчанию: `0`.

**Использование**

Настройка `SET distributed_product_mode=global` меняет поведение запросов для распределенных таблиц, но она не подходит для локальных таблиц или таблиц из внешних источников. В этих случаях удобно использовать настройку `prefer_global_in_and_join`.

Например, если нужно объединить все данные из локальных таблиц, которые находятся на разных узлах — для распределенной обработки необходим `GLOBAL JOIN`.

Другой вариант использования настройки `prefer_global_in_and_join` — регулирование обращений к таблицам из внешних источников.
Эта настройка помогает уменьшить количество обращений к внешним ресурсам при объединении внешних таблиц: только один вызов на весь распределенный запрос.

**См. также:**

-   [Распределенные подзапросы](../../sql-reference/operators/in.md#select-distributed-subqueries) `GLOBAL IN`/`GLOBAL JOIN`

## enable_optimize_predicate_expression {#enable-optimize-predicate-expression}

Включает пробрасывание предикатов в подзапросы для запросов `SELECT`.

Пробрасывание предикатов может существенно уменьшить сетевой трафик для распределенных запросов.

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 1.

Использование

Рассмотрим следующие запросы:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

Если `enable_optimize_predicate_expression = 1`, то время выполнения запросов одинаковое, так как ClickHouse применяет `WHERE` к подзапросу сразу при его обработке.

Если `enable_optimize_predicate_expression = 0`, то время выполнения второго запроса намного больше, потому что секция `WHERE` применяется к данным уже после завершения подзапроса.

## fallback_to_stale_replicas_for_distributed_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

Форсирует запрос в устаревшую реплику в случае, если актуальные данные недоступны. См. [Репликация](../../engines/table-engines/mergetree-family/replication.md).

Из устаревших реплик таблицы ClickHouse выбирает наиболее актуальную.

Используется при выполнении `SELECT` из распределенной таблицы, которая указывает на реплицированные таблицы.

По умолчанию - 1 (включена).

## force_index_by_date {#settings-force_index_by_date}

Запрещает выполнение запросов, если использовать индекс по дате невозможно.

Работает с таблицами семейства MergeTree.

При `force_index_by_date=1` ClickHouse проверяет, есть ли в запросе условие на ключ даты, которое может использоваться для отсечения диапазонов данных. Если подходящего условия нет - кидается исключение. При этом не проверяется, действительно ли условие уменьшает объём данных для чтения. Например, условие `Date != '2000-01-01'` подходит даже в том случае, когда соответствует всем данным в таблице (т.е. для выполнения запроса требуется full scan). Подробнее про диапазоны данных в таблицах MergeTree читайте в разделе [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force_primary_key {#settings-force_primary_key}

Запрещает выполнение запросов, если использовать индекс по первичному ключу невозможно.

Работает с таблицами семейства MergeTree.

При `force_primary_key=1` ClickHouse проверяет, есть ли в запросе условие на первичный ключ, которое может использоваться для отсечения диапазонов данных. Если подходящего условия нет - кидается исключение. При этом не проверяется, действительно ли условие уменьшает объём данных для чтения. Подробнее про диапазоны данных в таблицах MergeTree читайте в разделе [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## format_schema {#format-schema}

Параметр применяется в том случае, когда используются форматы, требующие определения схемы, например [Cap’n Proto](https://capnproto.org/) или [Protobuf](https://developers.google.com/protocol-buffers/). Значение параметра зависит от формата.

## fsync_metadata {#fsync-metadata}

Включает или отключает [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) при записи `.sql` файлов. По умолчанию включено.

Имеет смысл выключать, если на сервере миллионы мелких таблиц-чанков, которые постоянно создаются и уничтожаются.

## function_range_max_elements_in_block {#settings-function_range_max_elements_in_block}

Устанавливает порог безопасности для объема данных, создаваемого функцией [range](../../sql-reference/functions/array-functions.md#range). Задаёт максимальное количество значений, генерируемых функцией на блок данных (сумма размеров массивов для каждой строки в блоке).

Возможные значения:

-   Положительное целое.

Значение по умолчанию: `500 000 000`.

**См. также**

-   [max_block_size](#setting-max_block_size)
-   [min_insert_block_size_rows](#min-insert-block-size-rows)

## enable_http_compression {#settings-enable_http_compression}

Включает или отключает сжатие данных в ответе на HTTP-запрос.

Для получения дополнительной информации, читайте [Описание интерфейса HTTP](../../interfaces/http.md).

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

## http_zlib_compression_level {#settings-http_zlib_compression_level}

Задаёт уровень сжатия данных в ответе на HTTP-запрос, если [enable_http_compression = 1](#settings-enable_http_compression).

Возможные значения: числа от 1 до 9.

Значение по умолчанию: 3.

## http_native_compression_disable_checksumming_on_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

Включает или отключает проверку контрольной суммы при распаковке данных HTTP POST от клиента. Используется только для собственного (`Navite`) формата сжатия ClickHouse (ни `gzip`, ни `deflate`).

Для получения дополнительной информации, читайте [Описание интерфейса HTTP](../../interfaces/http.md).

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

## http_max_uri_size {#http-max-uri-size}

Устанавливает максимальную длину URI в HTTP-запросе.

Возможные значения:

-   Положительное целое.

Значение по умолчанию: 1048576.

## table_function_remote_max_addresses {#table_function_remote_max_addresses}

Задает максимальное количество адресов, которые могут быть сгенерированы из шаблонов для функции [remote](../../sql-reference/table-functions/remote.md).

Возможные значения:

-   Положительное целое.

Значение по умолчанию: `1000`.

##  glob_expansion_max_elements  {#glob_expansion_max_elements }

Задает максимальное количество адресов, которые могут быть сгенерированы из шаблонов при использовании внешних хранилищ и при вызове табличных функциях (например, [url](../../sql-reference/table-functions/url.md)), кроме функции `remote`.

Возможные значения:

-   Положительное целое.

Значение по умолчанию: `1000`.

## send_progress_in_http_headers {#settings-send_progress_in_http_headers}

Включает или отключает HTTP-заголовки `X-ClickHouse-Progress` в ответах `clickhouse-server`.

Для получения дополнительной информации, читайте [Описание интерфейса HTTP](../../interfaces/http.md).

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

## max_http_get_redirects {#setting-max_http_get_redirects}

Ограничивает максимальное количество переходов по редиректам в таблицах с движком [URL](../../engines/table-engines/special/url.md) при выполнении HTTP запросов методом GET. Настройка применяется для обоих типов таблиц: созданных запросом [CREATE TABLE](../../sql-reference/statements/create/table.md#create-table-query) и с помощью табличной функции [url](../../sql-reference/table-functions/url.md).

Возможные значения:

-   Положительное целое число переходов.
-   0 — переходы запрещены.

Значение по умолчанию: 0.

## input_format_allow_errors_num {#input-format-allow-errors-num}

Устанавливает максимальное количество допустимых ошибок при чтении из текстовых форматов (CSV, TSV и т.п.).

Значение по умолчанию: 0.

Используйте обязательно в паре с `input_format_allow_errors_ratio`. Для пропуска ошибок, значения обеих настроек должны быть больше 0.

Если при чтении строки возникла ошибка, но при этом счетчик ошибок меньше `input_format_allow_errors_num`, то ClickHouse игнорирует строку и переходит к следующей.

В случае превышения `input_format_allow_errors_num` ClickHouse генерирует исключение.

## input_format_allow_errors_ratio {#input-format-allow-errors-ratio}

Устанавливает максимальную долю допустимых ошибок при чтении из текстовых форматов (CSV, TSV и т.п.).
Доля ошибок задаётся в виде числа с плавающей запятой от 0 до 1.

Значение по умолчанию: 0.

Используйте обязательно в паре с `input_format_allow_errors_num`. Для пропуска ошибок, значения обеих настроек должны быть больше 0.

Если при чтении строки возникла ошибка, но при этом текущая доля ошибок меньше `input_format_allow_errors_ratio`, то ClickHouse игнорирует строку и переходит к следующей.

В случае превышения `input_format_allow_errors_ratio` ClickHouse генерирует исключение.

## input_format_parquet_import_nested {#input_format_parquet_import_nested}

Включает или отключает возможность вставки данных в колонки типа [Nested](../../sql-reference/data-types/nested-data-structures/nested.md) в виде массива структур  в формате ввода [Parquet](../../interfaces/formats.md#data-format-parquet).

Возможные значения:

-   0 — данные не могут быть вставлены в колонки типа `Nested` в виде массива структур.
-   0 — данные могут быть вставлены в колонки типа `Nested` в виде массива структур.

Значение по умолчанию: `0`.

## input_format_arrow_import_nested {#input_format_arrow_import_nested}

Включает или отключает возможность вставки данных в колонки типа [Nested](../../sql-reference/data-types/nested-data-structures/nested.md) в виде массива структур в формате ввода [Arrow](../../interfaces/formats.md#data_types-matching-arrow).

Возможные значения:

-   0 — данные не могут быть вставлены в колонки типа `Nested` в виде массива структур.
-   0 — данные могут быть вставлены в колонки типа `Nested` в виде массива структур.

Значение по умолчанию: `0`.

## input_format_orc_import_nested {#input_format_orc_import_nested}

Включает или отключает возможность вставки данных в колонки типа [Nested](../../sql-reference/data-types/nested-data-structures/nested.md) в виде массива структур в формате ввода [ORC](../../interfaces/formats.md#data-format-orc).

Возможные значения:

-   0 — данные не могут быть вставлены в колонки типа `Nested` в виде массива структур.
-   0 — данные могут быть вставлены в колонки типа `Nested` в виде массива структур.

Значение по умолчанию: `0`.

## input_format_values_interpret_expressions {#settings-input_format_values_interpret_expressions}

Включает или отключает парсер SQL, если потоковый парсер не может проанализировать данные. Этот параметр используется только для формата [Values](../../interfaces/formats.md#data-format-values) при вставке данных. Дополнительные сведения о парсерах читайте в разделе [Синтаксис](../../sql-reference/syntax.md).

Возможные значения:

-   0 — выключена.

        В этом случае необходимо вставлять форматированные данные. Смотрите раздел [Форматы](../../interfaces/formats.md).

-   1 — включена.

        В этом случае вы можете использовать выражение SQL в качестве значения, но вставка данных намного медленнее. Если вы вставляете только форматированные данные, ClickHouse ведет себя так, как будто значение параметра равно 0.

Значение по умолчанию: 1.

Пример использования:

Вставим значение типа [DateTime](../../sql-reference/data-types/datetime.md) при разных значения настройки.

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

Последний запрос эквивалентен следующему:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## input_format_values_deduce_templates_of_expressions {#settings-input_format_values_deduce_templates_of_expressions}

Включает или отключает попытку вычисления шаблона для выражений SQL в формате [Values](../../interfaces/formats.md#data-format-values). Это позволяет гораздо быстрее парсить и интерпретировать выражения в `Values`, если выражения в последовательных строках имеют одинаковую структуру. ClickHouse пытается вычислить шаблон выражения, распарсить следующие строки с помощью этого шаблона и вычислить выражение в пачке успешно проанализированных строк.

Возможные значения:

-   0 — Выключена.
-   1 — Включена.

Значение по умолчанию: 1.

Для следующего запроса:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   Если `input_format_values_interpret_expressions=1` и `format_values_deduce_templates_of_expressions=0`, выражения интерпретируются отдельно для каждой строки (это очень медленно для большого количества строк).
-   Если `input_format_values_interpret_expressions=0` и `format_values_deduce_templates_of_expressions=1`, выражения в первой, второй и третьей строках парсятся с помощью шаблона `lower(String)` и интерпретируется вместе, выражение в четвертой строке парсится с другим шаблоном (`upper(String)`).
-   Если `input_format_values_interpret_expressions=1` и `format_values_deduce_templates_of_expressions=1`, то же самое, что и в предыдущем случае, но также позволяет выполнять резервную интерпретацию выражений отдельно, если невозможно вычислить шаблон.

## input_format_values_accurate_types_of_literals {#settings-input-format-values-accurate-types-of-literals}

Эта настройка используется, только когда `input_format_values_deduce_templates_of_expressions = 1`. Выражения для некоторых столбцов могут иметь одинаковую структуру, но содержат числовые литералы разных типов, например:

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

Возможные значения:

-   0 — Выключена.

    В этом случае, ClickHouse может использовать более общий тип для некоторых литералов (например, `Float64` или `Int64` вместо `UInt64` для `42`), но это может привести к переполнению и проблемам с точностью.

-   1 — Включена.

    В этом случае, ClickHouse проверяет фактический тип литерала и использует шаблон выражения соответствующего типа. В некоторых случаях это может значительно замедлить оценку выажения в `Values`.

Значение по умолчанию: 1.

## input_format_defaults_for_omitted_fields {#session_settings-input_format_defaults_for_omitted_fields}

При вставке данных запросом `INSERT`, заменяет пропущенные поля значениям по умолчанию для типа данных столбца.

Поддерживаемые форматы вставки:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSV](../../interfaces/formats.md#csv)
-   [TabSeparated](../../interfaces/formats.md#tabseparated)

    :::note "Примечание"
    Когда опция включена, сервер отправляет клиенту расширенные метаданные. Это требует дополнительных вычислительных ресурсов на сервере и может снизить производительность.
    :::
Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 1.

## input_format_tsv_empty_as_default {#settings-input-format-tsv-empty-as-default}

Если эта настройка включена, все пустые поля во входящем TSV заменяются значениями по умолчанию. Для сложных выражений по умолчанию также должна быть включена настройка `input_format_defaults_for_omitted_fields`.

По умолчанию отключена.

## input_format_tsv_enum_as_number {#settings-input_format_tsv_enum_as_number}

Включает или отключает парсинг значений перечислений как порядковых номеров.

Если режим включен, то во входящих данных в формате `TCV` значения перечисления (тип `ENUM`) всегда трактуются как порядковые номера, а не как элементы перечисления. Эту настройку рекомендуется включать для оптимизации парсинга, если данные типа `ENUM` содержат только порядковые номера, а не сами элементы перечисления.

Возможные значения:

-   0 — входящие значения типа `ENUM` сначала сопоставляются с элементами перечисления, а если совпадений не найдено, то трактуются как порядковые номера.
-   1 — входящие значения типа `ENUM` сразу трактуются как порядковые номера.

Значение по умолчанию: 0.

**Пример**

Рассмотрим таблицу:

```sql
CREATE TABLE table_with_enum_column_for_tsv_insert (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
```

При включенной настройке `input_format_tsv_enum_as_number`:

Запрос:

```sql
SET input_format_tsv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 102	2;
SELECT * FROM table_with_enum_column_for_tsv_insert;
```

Результат:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
```

Запрос:

```sql
SET input_format_tsv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 103	'first';
```

сгенерирует исключение.

При отключенной настройке `input_format_tsv_enum_as_number`:

Запрос:

```sql
SET input_format_tsv_enum_as_number = 0;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 102	2;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 103	'first';
SELECT * FROM table_with_enum_column_for_tsv_insert;
```

Результат:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
┌──Id─┬─Value──┐
│ 103 │ first  │
└─────┴────────┘
```

## input_format_null_as_default {#settings-input-format-null-as-default}

Включает или отключает инициализацию [значениями по умолчанию](../../sql-reference/statements/create/table.md#create-default-values) ячеек с [NULL](../../sql-reference/syntax.md#null-literal), если тип данных столбца не позволяет [хранить NULL](../../sql-reference/data-types/nullable.md#data_type-nullable).
Если столбец не позволяет хранить `NULL` и эта настройка отключена, то вставка `NULL` приведет к возникновению исключения. Если столбец позволяет хранить `NULL`, то значения `NULL` вставляются независимо от этой настройки.

Эта настройка используется для запросов [INSERT ... VALUES](../../sql-reference/statements/insert-into.md) для текстовых входных форматов.

Возможные значения:

-   0 — вставка `NULL` в столбец, не позволяющий хранить `NULL`, приведет к возникновению исключения.
-   1 — ячейки с `NULL` инициализируются значением столбца по умолчанию.

Значение по умолчанию: `1`.

## insert_null_as_default {#insert_null_as_default}

Включает или отключает вставку [значений по умолчанию](../../sql-reference/statements/create/table.md#create-default-values) вместо [NULL](../../sql-reference/syntax.md#null-literal) в столбцы, которые не позволяют [хранить NULL](../../sql-reference/data-types/nullable.md#data_type-nullable).
Если столбец не позволяет хранить `NULL` и эта настройка отключена, то вставка `NULL` приведет к возникновению исключения. Если столбец позволяет хранить `NULL`, то значения `NULL` вставляются независимо от этой настройки.

Эта настройка используется для запросов [INSERT ... SELECT](../../sql-reference/statements/insert-into.md#insert_query_insert-select). При этом подзапросы `SELECT` могут объединяться с помощью `UNION ALL`.

Возможные значения:

-   0 — вставка `NULL` в столбец, не позволяющий хранить `NULL`, приведет к возникновению исключения.
-   1 — вместо `NULL` вставляется значение столбца по умолчанию.

Значение по умолчанию: `1`.

## input_format_skip_unknown_fields {#settings-input-format-skip-unknown-fields}

Включает или отключает пропускание вставки неизвестных данных.

При записи данных, если входные данные содержат столбцы, которых нет в целевой таблице, ClickHouse генерирует исключение. Если пропускание вставки включено, ClickHouse не вставляет неизвестные данные и не генерирует исключение.

Поддерживаемые форматы:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

## input_format_import_nested_json {#settings-input_format_import_nested_json}

Включает или отключает вставку данных JSON с вложенными объектами.

Поддерживаемые форматы:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

См. также:

-   [Использование вложенных структур](../../interfaces/formats.md#jsoneachrow-nested) with the `JSONEachRow` format.

## input_format_with_names_use_header {#input_format_with_names_use_header}

Включает или отключает проверку порядка столбцов при вставке данных.

Чтобы повысить эффективность вставки данных, рекомендуем отключить эту проверку, если вы уверены, что порядок столбцов входных данных такой же, как в целевой таблице.

Поддерживаемые форматы:

- [CSVWithNames](../../interfaces/formats.md#csvwithnames)
- [CSVWithNamesAndTypes](../../interfaces/formats.md#csvwithnamesandtypes)
- [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
- [TabSeparatedWithNamesAndTypes](../../interfaces/formats.md#tabseparatedwithnamesandtypes)
- [JSONCompactEachRowWithNames](../../interfaces/formats.md#jsoncompacteachrowwithnames)
- [JSONCompactEachRowWithNamesAndTypes](../../interfaces/formats.md#jsoncompacteachrowwithnamesandtypes)
- [JSONCompactStringsEachRowWithNames](../../interfaces/formats.md#jsoncompactstringseachrowwithnames)
- [JSONCompactStringsEachRowWithNamesAndTypes](../../interfaces/formats.md#jsoncompactstringseachrowwithnamesandtypes)
- [RowBinaryWithNames](../../interfaces/formats.md#rowbinarywithnames)
- [RowBinaryWithNamesAndTypes](../../interfaces/formats.md#rowbinarywithnamesandtypes)
- [CustomSeparatedWithNames](../../interfaces/formats.md#customseparatedwithnames)
- [CustomSeparatedWithNamesAndTypes](../../interfaces/formats.md#customseparatedwithnamesandtypes)

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 1.

## input_format_with_types_use_header {#input_format_with_types_use_header}

Определяет, должен ли синтаксический анализатор формата проверять, соответствуют ли типы данных из входных данных типам данных из целевой таблицы.

Поддерживаемые форматы:

- [CSVWithNamesAndTypes](../../interfaces/formats.md#csvwithnamesandtypes)
- [TabSeparatedWithNamesAndTypes](../../interfaces/formats.md#tabseparatedwithnamesandtypes)
- [JSONCompactEachRowWithNamesAndTypes](../../interfaces/formats.md#jsoncompacteachrowwithnamesandtypes)
- [JSONCompactStringsEachRowWithNamesAndTypes](../../interfaces/formats.md#jsoncompactstringseachrowwithnamesandtypes)
- [RowBinaryWithNamesAndTypes](../../interfaces/formats.md#rowbinarywithnamesandtypes-rowbinarywithnamesandtypes)
- [CustomSeparatedWithNamesAndTypes](../../interfaces/formats.md#customseparatedwithnamesandtypes)

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 1.

## date_time_input_format {#settings-date_time_input_format}

Выбор парсера для текстового представления дат и времени при обработке входного формата.

Настройка не применяется к [функциям для работы с датой и временем](../../sql-reference/functions/date-time-functions.md).

Возможные значения:

-   `best_effort` — включает расширенный парсинг.

ClickHouse может парсить базовый формат `YYYY-MM-DD HH:MM:SS` и все форматы [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). Например, `2018-06-08T01:02:03.000Z`.

-   `basic` — используется базовый парсер.

ClickHouse может парсить только базовый формат `YYYY-MM-DD HH:MM:SS` или `YYYY-MM-DD`. Например, `2019-08-20 10:18:56` или `2019-08-20`.

Значение по умолчанию: `basic`.

См. также:

-   [Тип данных DateTime.](../../sql-reference/data-types/datetime.md)
-   [Функции для работы с датой и временем.](../../sql-reference/functions/date-time-functions.md)

## date_time_output_format {#settings-date_time_output_format}

Позволяет выбрать разные выходные форматы текстового представления даты и времени.

Возможные значения:

-   `simple` - простой выходной формат.

    Выходные дата и время Clickhouse в формате `YYYY-MM-DD hh:mm:ss`. Например, `2019-08-20 10:18:56`. Расчет выполняется в соответствии с часовым поясом типа данных (если он есть) или часовым поясом сервера.

-   `iso` - выходной формат ISO.

    Выходные дата и время Clickhouse в формате [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) `YYYY-MM-DDThh:mm:ssZ`. Например, `2019-08-20T10:18:56Z`. Обратите внимание, что выходные данные отображаются в формате UTC (`Z` означает UTC).

-   `unix_timestamp` - выходной формат Unix.

    Выходные дата и время в формате [Unix](https://en.wikipedia.org/wiki/Unix_time). Например `1566285536`.

Значение по умолчанию: `simple`.

См. также:

-   [Тип данных DateTime](../../sql-reference/data-types/datetime.md)
-   [Функции для работы с датой и временем](../../sql-reference/functions/date-time-functions.md)

## join_default_strictness {#settings-join_default_strictness}

Устанавливает строгость по умолчанию для [JOIN](../../sql-reference/statements/select/join.md#select-join).

Возможные значения:

-   `ALL` — если в правой таблице несколько совпадающих строк, данные умножаются на количество этих строк. Это нормальное поведение `JOIN` как в стандартном SQL.
-   `ANY` — если в правой таблице несколько соответствующих строк, то соединяется только первая найденная. Если в «правой» таблице есть не более одной подходящей строки, то результаты `ANY` и `ALL` совпадают.
-   `Пустая строка` — если `ALL` или `ANY` не указаны в запросе, то ClickHouse генерирует исключение.

Значение по умолчанию: `ALL`.

## join_algorithm {#settings-join_algorithm}

Определяет алгоритм выполнения запроса [JOIN](../../sql-reference/statements/select/join.md).

Возможные значения:

- `hash` — используется [алгоритм соединения хешированием](https://ru.wikipedia.org/wiki/Алгоритм_соединения_хешированием).
- `partial_merge` — используется [алгоритм соединения слиянием сортированных списков](https://ru.wikipedia.org/wiki/Алгоритм_соединения_слиянием_сортированных_списков).
- `prefer_partial_merge` — используется алгоритм соединения слиянием сортированных списков, когда это возможно.
- `auto` — сервер ClickHouse пытается на лету заменить алгоритм `hash` на `merge`, чтобы избежать переполнения памяти.

Значение по умолчанию: `hash`.

При использовании алгоритма `hash` правая часть `JOIN` загружается в оперативную память.

При использовании алгоритма `partial_merge` сервер сортирует данные и сбрасывает их на диск. Работа алгоритма `merge` в ClickHouse немного отличается от классической реализации. Сначала ClickHouse сортирует правую таблицу по блокам на основе [ключей соединения](../../sql-reference/statements/select/join.md#select-join) и для отсортированных блоков строит индексы min-max. Затем он сортирует куски левой таблицы на основе ключей соединения и объединяет их с правой таблицей операцией `JOIN`. Созданные min-max индексы используются для пропуска тех блоков из правой таблицы, которые не участвуют в данной операции `JOIN`.

## join_any_take_last_row {#settings-join_any_take_last_row}

Изменяет поведение операций, выполняемых со строгостью `ANY`.

:::warning "Внимание"
    Настройка применяется только для операций `JOIN`, выполняемых над таблицами с движком [Join](../../engines/table-engines/special/join.md).
:::

Возможные значения:

-   0 — если в правой таблице несколько соответствующих строк, то присоединяется только первая найденная строка.
-   1 — если в правой таблице несколько соответствующих строк, то присоединяется только последняя найденная строка.

Значение по умолчанию: 0.

См. также:

-   [Секция JOIN](../../sql-reference/statements/select/join.md#select-join)
-   [Движок таблиц Join](../../engines/table-engines/special/join.md)
-   [join_default_strictness](#settings-join_default_strictness)

## join_use_nulls {#join_use_nulls}

Устанавливает тип поведения [JOIN](../../sql-reference/statements/select/join.md). При объединении таблиц могут появиться пустые ячейки. ClickHouse заполняет их по-разному в зависимости от настроек.

Возможные значения:

-   0 — пустые ячейки заполняются значением по умолчанию соответствующего типа поля.
-   1 — `JOIN` ведёт себя как в стандартном SQL. Тип соответствующего поля преобразуется в [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable), а пустые ячейки заполняются значениями [NULL](../../sql-reference/syntax.md).

## partial_merge_join_optimizations {#partial_merge_join_optimizations}

Отключает все оптимизации для запросов [JOIN](../../sql-reference/statements/select/join.md) с частичным MergeJoin алгоритмом.

По умолчанию оптимизации включены, что может привести к неправильным результатам. Если вы видите подозрительные результаты в своих запросах, отключите оптимизацию с помощью этого параметра. В различных версиях сервера ClickHouse, оптимизация может отличаться.

Возможные значения:

-   0 — Оптимизация отключена.
-   1 — Оптимизация включена.

Значение по умолчанию: 1.

## partial_merge_join_rows_in_right_blocks {#partial_merge_join_rows_in_right_blocks}

Устанавливает предельные размеры блоков данных «правого» соединения, для запросов [JOIN](../../sql-reference/statements/select/join.md) с частичным MergeJoin алгоритмом.

Сервер ClickHouse:

1. Разделяет данные правого соединения на блоки с заданным числом строк.
2. Индексирует для каждого блока минимальное и максимальное значение.
3. Выгружает подготовленные блоки на диск, если это возможно.

Возможные значения:

-  Положительное целое число. Рекомендуемый диапазон значений [1000, 100000].

Значение по умолчанию: 65536.

## join_on_disk_max_files_to_merge {#join_on_disk_max_files_to_merge}

Устанавливет количество файлов, разрешенных для параллельной сортировки, при выполнении операций MergeJoin на диске.

Чем больше значение параметра, тем больше оперативной памяти используется и тем меньше используется диск (I/O).

Возможные значения:

-   Положительное целое число, больше 2.

Значение по умолчанию: 64.

## temporary_files_codec {#temporary_files_codec}

Устанавливает метод сжатия для временных файлов на диске, используемых при сортировки и объединения.

Возможные значения:

-   LZ4 — применять сжатие, используя алгоритм [LZ4](https://ru.wikipedia.org/wiki/LZ4)
-   NONE — не применять сжатие.

Значение по умолчанию: LZ4.

## any_join_distinct_right_table_keys {#any_join_distinct_right_table_keys}

Включает устаревшее поведение сервера ClickHouse при выполнении операций `ANY INNER|LEFT JOIN`.

    :::note "Внимание"
    Используйте этот параметр только в целях обратной совместимости, если ваши варианты использования требуют устаревшего поведения `JOIN`.
    :::
Когда включено устаревшее поведение:

-   Результаты операций "t1 ANY LEFT JOIN t2" и "t2 ANY RIGHT JOIN t1" не равны, поскольку ClickHouse использует логику с сопоставлением ключей таблицы "многие к одному слева направо".
-   Результаты операций `ANY INNER JOIN` содержат все строки из левой таблицы, аналогично операции `SEMI LEFT JOIN`.

Когда устаревшее поведение отключено:

-   Результаты операций `t1 ANY LEFT JOIN t2` и `t2 ANY RIGHT JOIN t1` равно, потому что ClickHouse использует логику сопоставления ключей один-ко-многим в операциях `ANY RIGHT JOIN`.
-   Результаты операций `ANY INNER JOIN` содержат по одной строке на ключ из левой и правой таблиц.

Возможные значения:

-   0 — Устаревшее поведение отключено.
-   1 — Устаревшее поведение включено.

Значение по умолчанию: 0.

См. также:

-   [JOIN strictness](../../sql-reference/statements/select/join.md#join-settings)

## max_block_size {#setting-max_block_size}

Данные в ClickHouse обрабатываются по блокам (наборам кусочков столбцов). Внутренние циклы обработки для одного блока достаточно эффективны, но есть заметные издержки на каждый блок. Настройка `max_block_size` — это рекомендация, какой размер блока (в количестве строк) загружать из таблиц. Размер блока не должен быть слишком маленьким, чтобы затраты на каждый блок были заметны, но не слишком велики, чтобы запрос с LIMIT, который завершается после первого блока, обрабатывался быстро. Цель состоит в том, чтобы не использовалось слишком много оперативки при вынимании большого количества столбцов в несколько потоков; чтобы оставалась хоть какая-нибудь кэш-локальность.

Значение по умолчанию: 65,536.

Из таблицы не всегда загружаются блоки размера `max_block_size`. Если ясно, что нужно прочитать меньше данных, то будет считан блок меньшего размера.

## preferred_block_size_bytes {#preferred-block-size-bytes}

Служит для тех же целей что и `max_block_size`, но задает рекомендуемый размер блоков в байтах, выбирая адаптивное количество строк в блоке.
При этом размер блока не может быть более `max_block_size` строк.
По умолчанию: 1,000,000. Работает только при чтении из MergeTree-движков.

## merge_tree_uniform_read_distribution {#setting-merge-tree-uniform-read-distribution}

При чтении из таблиц [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) ClickHouse использует несколько потоков. Этот параметр включает/выключает равномерное распределение заданий по рабочим потокам. Алгоритм равномерного распределения стремится сделать время выполнения всех потоков примерно равным для одного запроса `SELECT`.

Возможные значения:

-   0 — не использовать равномерное распределение заданий на чтение.
-   1 — использовать равномерное распределение заданий на чтение.

Значение по умолчанию: 1.

## merge_tree_min_rows_for_concurrent_read {#setting-merge-tree-min-rows-for-concurrent-read}

Если количество строк, считываемых из файла таблицы [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) превышает `merge_tree_min_rows_for_concurrent_read`, то ClickHouse пытается выполнить одновременное чтение из этого файла в несколько потоков.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `163840`.


## merge_tree_min_rows_for_concurrent_read_for_remote_filesystem {#merge-tree-min-rows-for-concurrent-read-for-remote-filesystem}

Минимальное количество строк для чтения из одного файла, прежде чем движок [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) может выполнять параллельное чтение из удаленной файловой системы.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `163840`.

## merge_tree_min_bytes_for_concurrent_read {#setting-merge-tree-min-bytes-for-concurrent-read}

Если число байтов, которое должно быть прочитано из одного файла таблицы с движком [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), превышает значение `merge_tree_min_bytes_for_concurrent_read`, то ClickHouse выполняет одновременное чтение в несколько потоков из этого файла.

Возможное значение:

-   Положительное целое число.

Значение по умолчанию: `251658240`.

## merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem {#merge-tree-min-bytes-for-concurrent-read-for-remote-filesystem}

Минимальное количество байтов для чтения из одного файла, прежде чем движок [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) может выполнять параллельное чтение из удаленной файловой системы.

Возможное значение:

-   Положительное целое число.

Значение по умолчанию: `251658240`.

## merge_tree_min_rows_for_seek {#setting-merge-tree-min-rows-for-seek}

Если расстояние между двумя блоками данных для чтения в одном файле меньше, чем `merge_tree_min_rows_for_seek` строк, то ClickHouse не перескакивает (seek) через блоки, а считывает данные последовательно.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: 0.

## merge_tree_min_bytes_for_seek {#setting-merge-tree-min-bytes-for-seek}

Если расстояние между двумя блоками данных для чтения в одном файле меньше, чем `merge_tree_min_bytes_for_seek` байтов, то ClickHouse не перескакивает (seek) через блоки, а считывает данные последовательно.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: 0.

## merge_tree_coarse_index_granularity {#setting-merge-tree-coarse-index-granularity}

При поиске данных ClickHouse проверяет засечки данных в файле индекса. Если ClickHouse обнаруживает, что требуемые ключи находятся в некотором диапазоне, он делит этот диапазон на `merge_tree_coarse_index_granularity` поддиапазонов и выполняет в них рекурсивный поиск нужных ключей.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: 8.

## merge_tree_max_rows_to_use_cache {#setting-merge-tree-max-rows-to-use-cache}

Если требуется прочитать более, чем `merge_tree_max_rows_to_use_cache` строк в одном запросе, ClickHouse не используют кэш несжатых блоков.

Кэш несжатых блоков хранит данные, извлечённые при выполнении запросов. ClickHouse использует этот кэш для ускорения ответов на повторяющиеся небольшие запросы. Настройка защищает кэш от замусоривания запросами, для выполнения которых необходимо извлечь большое количество данных. Настройка сервера [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) определяет размер кэша несжатых блоков.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: 128 ✕ 8192.

## merge_tree_max_bytes_to_use_cache {#setting-merge-tree-max-bytes-to-use-cache}

Если требуется прочитать более, чем `merge_tree_max_bytes_to_use_cache` байтов в одном запросе, ClickHouse не используют кэш несжатых блоков.

Кэш несжатых блоков хранит данные, извлечённые при выполнении запросов. ClickHouse использует кэш для ускорения ответов на повторяющиеся небольшие запросы. Настройка защищает кэш от переполнения. Настройка сервера [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) определяет размер кэша несжатых блоков.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: 2013265920.

## min_bytes_to_use_direct_io {#settings-min-bytes-to-use-direct-io}

Минимальный объём данных, необходимый для прямого (небуферизованного) чтения/записи (direct I/O) на диск.

ClickHouse использует этот параметр при чтении данных из таблиц. Если общий объём хранения всех данных для чтения превышает `min_bytes_to_use_direct_io` байт, тогда ClickHouse использует флаг `O_DIRECT` при чтении данных с диска.

Возможные значения:

-   0 — прямой ввод-вывод отключен.
-   Положительное целое число.

Значение по умолчанию: 0.

## network_compression_method {#network_compression_method}

Устанавливает метод сжатия данных, который используется для обмена данными между серверами и между сервером и [clickhouse-client](../../interfaces/cli.md).

Возможные значения:

-   `LZ4` — устанавливает метод сжатия LZ4.
-   `ZSTD` — устанавливает метод сжатия ZSTD.

Значение по умолчанию: `LZ4`.

**См. также**

-   [network_zstd_compression_level](#network_zstd_compression_level)

## network_zstd_compression_level {#network_zstd_compression_level}

Регулирует уровень сжатия ZSTD. Используется только тогда, когда [network_compression_method](#network_compression_method) установлен на `ZSTD`.

Возможные значения:

-   Положительное целое число от 1 до 15.

Значение по умолчанию: `1`.

## log_queries {#settings-log-queries}

Установка логирования запроса.

Запросы, переданные в ClickHouse с этой настройкой, логируются согласно правилам конфигурационного параметра сервера [query_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log).

Пример:

``` text
log_queries=1
```

## log_queries_min_query_duration_ms {#settings-log-queries-min-query-duration-ms}

Минимальное время выполнения запроса для логгирования в системные таблицы:

- `system.query_log`
- `system.query_thread_log`

В случае ненулевого порога `log_queries_min_query_duration_ms`, в лог будут записываться лишь события об окончании выполнения запроса:

- `QUERY_FINISH`
- `EXCEPTION_WHILE_PROCESSING`

-   Тип: milliseconds
-   Значение по умолчанию: 0 (логгировать все запросы)

## log_queries_min_type {#settings-log-queries-min-type}

Задаёт минимальный уровень логирования в `query_log`.

Возможные значения:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

Значение по умолчанию: `QUERY_START`.

Можно использовать для ограничения того, какие объекты будут записаны в `query_log`, например, если вас интересуют ошибки, тогда вы можете использовать `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## log_query_threads {#settings-log-query-threads}

Управляет логированием информации о потоках выполнения запросов.

Информация о потоках выполнения запросов сохраняется в системной таблице [system.query_thread_log](../../operations/system-tables/query_thread_log.md). Работает только в том случае, если включена настройка [log_queries](#settings-log-queries). Лог информации о потоках выполнения запросов, переданных в ClickHouse с этой установкой, записывается согласно правилам конфигурационного параметра сервера [query_thread_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log).

Возможные значения:

-   0 — отключено.
-   1 — включено.

Значение по умолчанию: `1`.

**Пример**

``` text
log_query_threads=1
```

## log_formatted_queries {#settings-log-formatted-queries}

Позволяет регистрировать отформатированные запросы в системной таблице [system.query_log](../../operations/system-tables/query_log.md).

Возможные значения:

-   0 — отформатированные запросы не регистрируются в системной таблице.
-   1 — отформатированные запросы регистрируются в системной таблице.

Значение по умолчанию: `0`.

## log_comment {#settings-log-comment}

Задаёт значение поля `log_comment` таблицы [system.query_log](../system-tables/query_log.md) и текст комментария в логе сервера.

Может быть использована для улучшения читабельности логов сервера. Кроме того, помогает быстро выделить связанные с тестом запросы из `system.query_log` после запуска [clickhouse-test](../../development/tests.md).

Возможные значения:

-   Любая строка не длиннее [max_query_size](#settings-max_query_size). При превышении длины сервер сгенерирует исключение.

Значение по умолчанию: пустая строка.

**Пример**

Запрос:

``` sql
SET log_comment = 'log_comment test', log_queries = 1;
SELECT 1;
SYSTEM FLUSH LOGS;
SELECT type, query FROM system.query_log WHERE log_comment = 'log_comment test' AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 2;
```

Результат:

``` text
┌─type────────┬─query─────┐
│ QueryStart  │ SELECT 1; │
│ QueryFinish │ SELECT 1; │
└─────────────┴───────────┘
```

## max_insert_block_size {#settings-max_insert_block_size}

Формировать блоки указанного размера, при вставке в таблицу.
Эта настройка действует только в тех случаях, когда сервер сам формирует такие блоки.
Например, при INSERT-е через HTTP интерфейс, сервер парсит формат данных, и формирует блоки указанного размера.
А при использовании clickhouse-client, клиент сам парсит данные, и настройка max_insert_block_size на сервере не влияет на размер вставляемых блоков.
При использовании INSERT SELECT, настройка так же не имеет смысла, так как данные будут вставляться теми блоками, которые вышли после SELECT-а.

Значение по умолчанию: 1,048,576.

Это значение намного больше, чем `max_block_size`. Это сделано, потому что некоторые движки таблиц (`*MergeTree`) будут на каждый вставляемый блок формировать кусок данных на диске, что является довольно большой сущностью. Также, в таблицах типа `*MergeTree`, данные сортируются при вставке, и достаточно большой размер блока позволяет отсортировать больше данных в оперативке.

## min_insert_block_size_rows {#min-insert-block-size-rows}

Устанавливает минимальное количество строк в блоке, который может быть вставлен в таблицу запросом `INSERT`. Блоки меньшего размера склеиваются в блоки большего размера.

Возможные значения:

-   Целое положительное число.
-   0 — Склейка блоков выключена.

Значение по умолчанию: 1048576.

## min_insert_block_size_bytes {#min-insert-block-size-bytes}

Устанавливает минимальное количество байтов в блоке, который может быть вставлен в таблицу запросом `INSERT`. Блоки меньшего размера склеиваются в блоки большего размера.

Возможные значения:

-   Целое положительное число.
-   0 — Склейка блоков выключена.

Значение по умолчанию: 268435456.

## max_replica_delay_for_distributed_queries {#settings-max_replica_delay_for_distributed_queries}

Отключает отстающие реплики при распределенных запросах. См. [Репликация](../../engines/table-engines/mergetree-family/replication.md).

Устанавливает время в секундах. Если отставание реплики больше установленного значения, то реплика не используется.

Значение по умолчанию: 300.

Используется при выполнении `SELECT` из распределенной таблицы, которая указывает на реплицированные таблицы.

## max_threads {#settings-max_threads}

Максимальное количество потоков обработки запроса без учёта потоков для чтения данных с удалённых серверов (смотрите параметр max_distributed_connections).

Этот параметр относится к потокам, которые выполняют параллельно одни стадии конвейера выполнения запроса.
Например, при чтении из таблицы, если есть возможность вычислять выражения с функциями, фильтровать с помощью WHERE и предварительно агрегировать для GROUP BY параллельно, используя хотя бы количество потоков max_threads, то используются max_threads.

Значение по умолчанию: количество процессорных ядер без учёта Hyper-Threading.

Для запросов, которые быстро завершаются из-за LIMIT-а, имеет смысл выставить max_threads поменьше. Например, если нужное количество записей находится в каждом блоке, то при max_threads = 8 будет считано 8 блоков, хотя достаточно было прочитать один.

Чем меньше `max_threads`, тем меньше будет использоваться оперативки.

## max_insert_threads {#settings-max-insert-threads}

Максимальное количество потоков для выполнения запроса `INSERT SELECT`.

Возможные значения:

-   0 (или 1) — `INSERT SELECT` не выполняется параллельно.
-   Положительное целое число, больше 1.

Значение по умолчанию: 0.

Параллельный `INSERT SELECT` действует только в том случае, если часть SELECT выполняется параллельно, см. настройку [max_threads](#settings-max_threads).
Чем больше значение `max_insert_threads`, тем больше потребление оперативной памяти.

## max_compress_block_size {#max-compress-block-size}

Максимальный размер блоков несжатых данных перед сжатием при записи в таблицу. По умолчанию - 1 048 576 (1 MiB). При уменьшении размера, незначительно уменьшается коэффициент сжатия, незначительно возрастает скорость сжатия и разжатия за счёт кэш-локальности, и уменьшается потребление оперативной памяти.

    :::note "Предупреждение"
    Эта настройка экспертного уровня, не используйте ее, если вы только начинаете работать с Clickhouse.
    :::
Не путайте блоки для сжатия (кусок памяти, состоящий из байт) и блоки для обработки запроса (пачка строк из таблицы).

## min_compress_block_size {#min-compress-block-size}

Для таблиц типа [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). В целях уменьшения задержек при обработке запросов, блок сжимается при записи следующей засечки, если его размер не меньше `min_compress_block_size`. По умолчанию - 65 536.

Реальный размер блока, если несжатых данных меньше `max_compress_block_size`, будет не меньше этого значения и не меньше объёма данных на одну засечку.

Рассмотрим пример. Пусть `index_granularity`, указанная при создании таблицы - 8192.

Пусть мы записываем столбец типа UInt32 (4 байта на значение). При записи 8192 строк, будет всего 32 КБ данных. Так как `min_compress_block_size` = 65 536, сжатый блок будет сформирован на каждые две засечки.

Пусть мы записываем столбец URL типа String (средний размер - 60 байт на значение). При записи 8192 строк, будет, в среднем, чуть меньше 500 КБ данных. Так как это больше 65 536 строк, то сжатый блок будет сформирован на каждую засечку. В этом случае, при чтении с диска данных из диапазона в одну засечку, не будет разжато лишних данных.

    :::note "Предупреждение"
    Эта настройка экспертного уровня, не используйте ее, если вы только начинаете работать с Clickhouse.
    :::
## max_query_size {#settings-max_query_size}

Максимальный кусок запроса, который будет считан в оперативку для разбора парсером языка SQL.
Запрос INSERT также содержит данные для INSERT-а, которые обрабатываются отдельным, потоковым парсером (расходующим O(1) оперативки), и не учитываются в этом ограничении.

Значение по умолчанию: 256 Кб.

## max_parser_depth {#max_parser_depth}

Ограничивает максимальную глубину рекурсии в парсере рекурсивного спуска. Позволяет контролировать размер стека.

Возможные значения:

- Положительное целое число.
- 0 — Глубина рекурсии не ограничена.

Значение по умолчанию: 1000.

## interactive_delay {#interactive-delay}

Интервал в микросекундах для проверки, не запрошена ли остановка выполнения запроса, и отправки прогресса.

Значение по умолчанию: 100,000 (проверять остановку запроса и отправлять прогресс десять раз в секунду).

## connect_timeout, receive_timeout, send_timeout {#connect-timeout-receive-timeout-send-timeout}

Таймауты в секундах на сокет, по которому идёт общение с клиентом.

Значение по умолчанию: 10, 300, 300.

## cancel_http_readonly_queries_on_client_close {#cancel-http-readonly-queries-on-client-close}

Отменяет HTTP readonly запросы (например, SELECT), когда клиент обрывает соединение до завершения получения данных.

Значение по умолчанию: 0

## poll_interval {#poll-interval}

Блокироваться в цикле ожидания запроса в сервере на указанное количество секунд.

Значение по умолчанию: 10.

## max_distributed_connections {#max-distributed-connections}

Максимальное количество одновременных соединений с удалёнными серверами при распределённой обработке одного запроса к одной таблице типа Distributed. Рекомендуется выставлять не меньше, чем количество серверов в кластере.

Значение по умолчанию: 1024.

Следующие параметры имеют значение только на момент создания таблицы типа Distributed (и при запуске сервера), поэтому их не имеет смысла менять в рантайме.

## distributed_connections_pool_size {#distributed-connections-pool-size}

Максимальное количество одновременных соединений с удалёнными серверами при распределённой обработке всех запросов к одной таблице типа Distributed. Рекомендуется выставлять не меньше, чем количество серверов в кластере.

Значение по умолчанию: 1024.

## max_distributed_depth {#max-distributed-depth}

Ограничивает максимальную глубину рекурсивных запросов для [Distributed](../../engines/table-engines/special/distributed.md) таблиц.

Если значение превышено, сервер генерирует исключение.

Возможные значения:

-   Положительное целое число.
-   0 — глубина не ограничена.

Значение по умолчанию: `5`.

## max_replicated_fetches_network_bandwidth_for_server {#max_replicated_fetches_network_bandwidth_for_server}

Ограничивает максимальную скорость обмена данными в сети (в байтах в секунду) для синхронизации между [репликами](../../engines/table-engines/mergetree-family/replication.md). Применяется только при запуске сервера. Можно также ограничить скорость для конкретной таблицы с помощью настройки [max_replicated_fetches_network_bandwidth](../../operations/settings/merge-tree-settings.md#max_replicated_fetches_network_bandwidth).

Значение настройки соблюдается неточно.

Возможные значения:

-   Любое целое положительное число.
-   0 — Скорость не ограничена.

Значение по умолчанию: `0`.

**Использование**

Может быть использована для ограничения скорости сети при репликации данных для добавления или замены новых узлов.

    :::note
    60000000 байт/с примерно соответствует 457 Мбит/с (60000000 / 1024 / 1024 * 8).
    :::

## max_replicated_sends_network_bandwidth_for_server {#max_replicated_sends_network_bandwidth_for_server}

Ограничивает максимальную скорость обмена данными в сети (в байтах в секунду) для [репликационных](../../engines/table-engines/mergetree-family/replication.md) отправок. Применяется только при запуске сервера. Можно также ограничить скорость для конкретной таблицы с помощью настройки [max_replicated_sends_network_bandwidth](../../operations/settings/merge-tree-settings.md#max_replicated_sends_network_bandwidth).

Значение настройки соблюдается неточно.

Возможные значения:

-   Любое целое положительное число.
-   0 — Скорость не ограничена.

Значение по умолчанию: `0`.

**Использование**

Может быть использована для ограничения скорости сети при репликации данных для добавления или замены новых узлов.

    :::note
    60000000 байт/с примерно соответствует 457 Мбит/с (60000000 / 1024 / 1024 * 8).
    :::

## connect_timeout_with_failover_ms {#connect-timeout-with-failover-ms}

Таймаут в миллисекундах на соединение с удалённым сервером, для движка таблиц Distributed, если используются секции shard и replica в описании кластера.
В случае неуспеха, делается несколько попыток соединений с разными репликами.

Значение по умолчанию: 50.

## connection_pool_max_wait_ms {#connection-pool-max-wait-ms}

Время ожидания соединения в миллисекундах, когда пул соединений заполнен.

Возможные значения:

- Положительное целое число.
- 0 — Бесконечный таймаут.

Значение по умолчанию: 0.

## connections_with_failover_max_tries {#connections-with-failover-max-tries}

Максимальное количество попыток соединения с каждой репликой, для движка таблиц Distributed.

Значение по умолчанию: 3.

## extremes {#extremes}

Считать ли экстремальные значения (минимумы и максимумы по столбцам результата запроса). Принимает 0 или 1. По умолчанию - 0 (выключено).
Подробнее смотрите раздел «Экстремальные значения».

## kafka_max_wait_ms {#kafka-max-wait-ms}

Время ожидания в миллисекундах для чтения сообщений из [Kafka](../../engines/table-engines/integrations/kafka.md#kafka) перед повторной попыткой.

Возможные значения:

- Положительное целое число.
- 0 — Бесконечный таймаут.

Значение по умолчанию: 5000.

См. также:

-   [Apache Kafka](https://kafka.apache.org/)

## use_uncompressed_cache {#setting-use_uncompressed_cache}

Использовать ли кэш разжатых блоков. Принимает 0 или 1. По умолчанию - 0 (выключено).

Использование кэша несжатых блоков (только для таблиц семейства MergeTree) может существенно сократить задержку и увеличить пропускную способность при работе с большим количеством коротких запросов. Включите эту настройку для пользователей, от которых идут частые короткие запросы. Также обратите внимание на конфигурационный параметр [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) (настраивается только в конфигурационном файле) – размер кэша разжатых блоков. По умолчанию - 8 GiB. Кэш разжатых блоков заполняется по мере надобности, а наиболее невостребованные данные автоматически удаляются.

Для запросов, читающих хоть немного приличный объём данных (миллион строк и больше), кэш разжатых блоков автоматически выключается, чтобы оставить место для действительно мелких запросов. Поэтому, можно держать настройку `use_uncompressed_cache` всегда выставленной в 1.

## replace_running_query {#replace-running-query}

При использовании интерфейса HTTP может быть передан параметр query_id. Это любая строка, которая служит идентификатором запроса.
Если в этот момент, уже существует запрос от того же пользователя с тем же query_id, то поведение определяется параметром replace_running_query.

`0` - (по умолчанию) кинуть исключение (не давать выполнить запрос, если запрос с таким же query_id уже выполняется);

`1` - отменить старый запрос и начать выполнять новый.

Эта настройка, выставленная в 1, используется в Яндекс.Метрике для реализации suggest-а значений для условий сегментации. После ввода очередного символа, если старый запрос ещё не выполнился, его следует отменить.

## replace_running_query_max_wait_ms {#replace-running-query-max-wait-ms}

Время ожидания завершения выполнения запроса с тем же `query_id`, когда активирована настройка [replace_running_query](#replace-running-query).

Возможные значения:

- Положительное целое число.
- 0 — Создание исключения, которое не позволяет выполнить новый запрос, если сервер уже выполняет запрос с тем же `query_id`.

Значение по умолчанию: 5000.

## stream_flush_interval_ms {#stream-flush-interval-ms}

Работает для таблиц со стриммингом в случае тайм-аута, или когда поток генерирует [max_insert_block_size](#settings-max_insert_block_size) строк.

Значение по умолчанию: 7500.

Чем меньше значение, тем чаще данные сбрасываются в таблицу. Установка слишком низкого значения приводит к снижению производительности.

## load_balancing {#settings-load_balancing}

Задает алгоритм выбора реплик, используемый при обработке распределенных запросов.

ClickHouse поддерживает следующие алгоритмы выбора реплик:

-   [Random](#load_balancing-random) (by default)
-   [Nearest hostname](#load_balancing-nearest_hostname)
-   [In order](#load_balancing-in_order)
-   [First or random](#load_balancing-first_or_random)
-   [Round robin](#load_balancing-round_robin)

См. также:

-   [distributed_replica_max_ignored_errors](#settings-distributed_replica_max_ignored_errors)

### Random (by Default) {#load_balancing-random}

``` sql
load_balancing = random
```

Для каждой реплики считается количество ошибок. Запрос отправляется на реплику с минимальным числом ошибок, а если таких несколько, то на случайную из них.
Недостатки: не учитывается близость серверов; если на репликах оказались разные данные, то вы будете получать так же разные данные.

### Nearest Hostname {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

Для каждой реплики считается количество ошибок. Каждые 5 минут, число ошибок целочисленно делится на 2. Таким образом, обеспечивается расчёт числа ошибок за недавнее время с экспоненциальным сглаживанием. Если есть одна реплика с минимальным числом ошибок (то есть, на других репликах недавно были ошибки) - запрос отправляется на неё. Если есть несколько реплик с одинаковым минимальным числом ошибок, то запрос отправляется на реплику, имя хоста которой в конфигурационном файле минимально отличается от имени хоста сервера (по количеству отличающихся символов на одинаковых позициях, до минимальной длины обеих имён хостов).

Для примера, example01-01-1 и example01-01-2.yandex.ru отличаются в одной позиции, а example01-01-1 и example01-02-2 - в двух.
Этот метод может показаться примитивным, но он не требует внешних данных о топологии сети и не сравнивает IP-адреса, что было бы сложно для наших IPv6-адресов.

Таким образом, если есть равнозначные реплики, предпочитается ближайшая по имени.
Также можно сделать предположение, что при отправке запроса на один и тот же сервер, в случае отсутствия сбоев, распределённый запрос будет идти тоже на одни и те же серверы. То есть, даже если на репликах расположены разные данные, запрос будет возвращать в основном одинаковые результаты.

### In Order {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

Реплики с одинаковым количеством ошибок опрашиваются в порядке, определённом конфигурацией.
Этот способ подходит для тех случаев, когда вы точно знаете, какая реплика предпочтительнее.

### First or Random {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

Алгоритм выбирает первую реплику или случайную реплику, если первая недоступна. Он эффективен в топологиях с перекрестной репликацией, но бесполезен в других конфигурациях.

Алгоритм `first or random` решает проблему алгоритма `in order`. При использовании `in order`, если одна реплика перестаёт отвечать, то следующая за ней принимает двойную нагрузку, в то время как все остальные обрабатываю свой обычный трафик. Алгоритм `first or random` равномерно распределяет нагрузку между репликами.

### Round Robin {#load_balancing-round_robin}

``` sql
load_balancing = round_robin
```

Этот алгоритм использует циклический перебор реплик с одинаковым количеством ошибок (учитываются только запросы с алгоритмом `round_robin`).

## prefer_localhost_replica {#settings-prefer-localhost-replica}

Включает или выключает предпочтительное использование localhost реплики при обработке распределенных запросов.

Возможные значения:

-   1 — ClickHouse всегда отправляет запрос на localhost реплику, если она существует.
-   0 — ClickHouse использует балансировку, заданную настройкой [load_balancing](#settings-load_balancing).

Значение по умолчанию: 1.

:::danger "Warning"
    Отключайте эту настройку при использовании [max_parallel_replicas](#settings-max_parallel_replicas).

## totals_mode {#totals-mode}

Каким образом вычислять TOTALS при наличии HAVING, а также при наличии max_rows_to_group_by и group_by_overflow_mode = ‘any’.
Смотрите раздел «Модификатор WITH TOTALS».

## totals_auto_threshold {#totals-auto-threshold}

Порог для `totals_mode = 'auto'`.
Смотрите раздел «Модификатор WITH TOTALS».

## max_parallel_replicas {#settings-max_parallel_replicas}

Максимальное количество используемых реплик каждого шарда при выполнении запроса.

Возможные значения:

-   Целое положительное число.

**Дополнительная информация**

Эта настройка полезна для реплицируемых таблиц с ключом сэмплирования. Запрос может обрабатываться быстрее, если он выполняется на нескольких серверах параллельно. Однако производительность обработки запроса, наоборот, может упасть в следующих ситуациях:

- Позиция ключа сэмплирования в ключе партиционирования не позволяет выполнять эффективное сканирование.
- Добавление ключа сэмплирования в таблицу делает фильтрацию по другим столбцам менее эффективной.
- Ключ сэмплирования является выражением, которое сложно вычисляется.
- У распределения сетевых задержек в кластере длинный «хвост», из-за чего при параллельных запросах к нескольким серверам увеличивается среднее время задержки.

:::danger "Предупреждение"
    Параллельное выполнение запроса может привести к неверному результату, если в запросе есть объединение или подзапросы и при этом таблицы не удовлетворяют определенным требованиям. Подробности смотрите в разделе [Распределенные подзапросы и max_parallel_replicas](../../sql-reference/operators/in.md#max_parallel_replica-subqueries).

## compile_expressions {#compile-expressions}

Включает или выключает компиляцию часто используемых функций и операторов. Компиляция производится в нативный код платформы с помощью LLVM во время выполнения.

Возможные значения:

- 0 — компиляция выключена.
- 1 — компиляция включена.

Значение по умолчанию: `1`.

## min_count_to_compile_expression {#min-count-to-compile-expression}

Минимальное количество выполнений одного и того же выражения до его компиляции.

Значение по умолчанию: `3`.

## compile_aggregate_expressions {#compile_aggregate_expressions}

Включает или отключает компиляцию агрегатных функций в нативный код во время выполнения запроса. Включение этой настройки может улучшить производительность выполнения запросов.

Возможные значения:

-   0 — агрегатные функции не компилируются в нативный код.
-   1 — агрегатные функции компилируются в нативный код в процессе выполнения запроса.

Значение по умолчанию: `1`.

**См. также**

-   [min_count_to_compile_aggregate_expression](#min_count_to_compile_aggregate_expression)

## min_count_to_compile_aggregate_expression {#min_count_to_compile_aggregate_expression}

Минимальное количество вызовов агрегатной функции с одинаковым выражением, при котором функция будет компилироваться в нативный код в ходе выполнения запроса. Работает только если включена настройка [compile_aggregate_expressions](#compile_aggregate_expressions).

Возможные значения:

-   Целое положительное число.
-   0 — агрегатные функциии всегда компилируются в ходе выполнения запроса.

Значение по умолчанию: `3`.

## input_format_skip_unknown_fields {#input-format-skip-unknown-fields}

Если значение равно true, то при выполнении INSERT входные данные из столбцов с неизвестными именами будут пропущены. В противном случае эта ситуация создаст исключение.
Работает для форматов JSONEachRow и TSKV.

## output_format_json_quote_64bit_integers {#session_settings-output_format_json_quote_64bit_integers}
Управляет кавычками при выводе 64-битных или более [целых чисел](../../sql-reference/data-types/int-uint.md) (например, `UInt64` или `Int128`) в формате [JSON](../../interfaces/formats.md#json).
По умолчанию такие числа заключаются в кавычки. Это поведение соответствует большинству реализаций JavaScript.

Возможные значения:

-   0 — числа выводятся без кавычек.
-   1 — числа выводятся в кавычках.

Значение по умолчанию: 1.

## output_format_json_quote_denormals {#settings-output_format_json_quote_denormals}

При выводе данных в формате [JSON](../../interfaces/formats.md#json) включает отображение значений `+nan`, `-nan`, `+inf`, `-inf`.

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

**Пример**

Рассмотрим следующую таблицу `account_orders`:

```text
┌─id─┬─name───┬─duration─┬─period─┬─area─┐
│  1 │ Andrew │       20 │      0 │  400 │
│  2 │ John   │       40 │      0 │    0 │
│  3 │ Bob    │       15 │      0 │ -100 │
└────┴────────┴──────────┴────────┴──────┘
```

Когда `output_format_json_quote_denormals = 0`, следующий запрос возвращает значения `null`.

```sql
SELECT area/period FROM account_orders FORMAT JSON;
```

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],
        "data":
        [
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                }
        ],
        "rows": 3,
        "statistics":
        {
                "elapsed": 0.003648093,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

Если `output_format_json_quote_denormals = 1`, то запрос вернет:

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],
        "data":
        [
                {
                        "divide(area, period)": "inf"
                },
                {
                        "divide(area, period)": "-nan"
                },
                {
                        "divide(area, period)": "-inf"
                }
        ],
        "rows": 3,
        "statistics":
        {
                "elapsed": 0.000070241,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```


## format_csv_delimiter {#settings-format_csv_delimiter}

Символ, интерпретируемый как разделитель в данных формата CSV. По умолчанию — `,`.

## input_format_csv_unquoted_null_literal_as_null {#settings-input_format_csv_unquoted_null_literal_as_null}

Для формата CSV включает или выключает парсинг неэкранированной строки `NULL` как литерала (синоним для `\N`)

## input_format_csv_enum_as_number {#settings-input_format_csv_enum_as_number}

Включает или отключает парсинг значений перечислений как порядковых номеров.
Если режим включен, то во входящих данных в формате `CSV` значения перечисления (тип `ENUM`) всегда трактуются как порядковые номера, а не как элементы перечисления. Эту настройку рекомендуется включать для оптимизации парсинга, если данные типа `ENUM` содержат только порядковые номера, а не сами элементы перечисления.

Возможные значения:

-   0 — входящие значения типа `ENUM` сначала сопоставляются с элементами перечисления, а если совпадений не найдено, то трактуются как порядковые номера.
-   1 — входящие значения типа `ENUM` сразу трактуются как порядковые номера.

Значение по умолчанию: 0.

**Пример**

Рассмотрим таблицу:

```sql
CREATE TABLE table_with_enum_column_for_csv_insert (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
```

При включенной настройке `input_format_csv_enum_as_number`:

Запрос:

```sql
SET input_format_csv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2;
```

Результат:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
```

Запрос:

```sql
SET input_format_csv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 103,'first'
```

сгенерирует исключение.

При отключенной настройке `input_format_csv_enum_as_number`:

Запрос:

```sql
SET input_format_csv_enum_as_number = 0;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 103,'first'
SELECT * FROM table_with_enum_column_for_csv_insert;
```

Результат:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
┌──Id─┬─Value─┐
│ 103 │ first │
└─────┴───────┘
```

## output_format_csv_crlf_end_of_line {#settings-output-format-csv-crlf-end-of-line}

Использовать в качестве разделителя строк для CSV формата CRLF (DOS/Windows стиль) вместо LF (Unix стиль).

## output_format_tsv_crlf_end_of_line {#settings-output-format-tsv-crlf-end-of-line}

Использовать в качестве разделителя строк для TSV формата CRLF (DOC/Windows стиль) вместо LF (Unix стиль).

## insert_quorum {#settings-insert_quorum}

Включает кворумную запись.

-   Если `insert_quorum < 2`, то кворумная запись выключена.
-   Если `insert_quorum >= 2`, то кворумная запись включена.

Значение по умолчанию: 0.

Кворумная запись

`INSERT` завершается успешно только в том случае, когда ClickHouse смог без ошибки записать данные в `insert_quorum` реплик за время `insert_quorum_timeout`. Если по любой причине количество реплик с успешной записью не достигнет `insert_quorum`, то запись считается не состоявшейся и ClickHouse удалит вставленный блок из всех реплик, куда уже успел записать данные.

Когда `insert_quorum_parallel` выключена, все реплики кворума консистентны, то есть содержат данные всех предыдущих запросов `INSERT` (последовательность `INSERT` линеаризуется). При чтении с диска данных, записанных с помощью `insert_quorum` и при выключенной `insert_quorum_parallel`, можно включить последовательную консистентность для запросов `SELECT` с помощью [select_sequential_consistency](#settings-select_sequential_consistency).

ClickHouse генерирует исключение:

-   Если количество доступных реплик на момент запроса меньше `insert_quorum`.
-   При попытке записать данные в момент, когда предыдущий блок ещё не вставлен в `insert_quorum` реплик. Эта ситуация может возникнуть, если пользователь вызвал `INSERT` прежде, чем завершился предыдущий с `insert_quorum`.

-   При выключенной `insert_quorum_parallel` и при попытке записать данные в момент, когда предыдущий блок еще не вставлен в `insert_quorum` реплик (несколько параллельных `INSERT`-запросов). Эта ситуация может возникнуть при попытке пользователя выполнить очередной запрос `INSERT` к той же таблице, прежде чем завершится предыдущий с `insert_quorum`.

См. также:

-   [insert_quorum_timeout](#settings-insert_quorum_timeout)
-   [insert_quorum_parallel](#settings-insert_quorum_parallel)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## insert_quorum_timeout {#settings-insert_quorum_timeout}

Время ожидания кворумной записи в миллисекундах. Если время прошло, а запись так не состоялась, то ClickHouse сгенерирует исключение и клиент должен повторить запрос на запись того же блока на эту же или любую другую реплику.

Значение по умолчанию: 600 000 миллисекунд (10 минут).

См. также:

-   [insert_quorum](#settings-insert_quorum)
-   [insert_quorum_parallel](#settings-insert_quorum_parallel)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## insert_quorum_parallel {#settings-insert_quorum_parallel}

Включает и выключает параллелизм для кворумных вставок (`INSERT`-запросы). Когда опция включена, возможно выполнять несколько кворумных `INSERT`-запросов одновременно, при этом запросы не дожидаются окончания друг друга . Когда опция выключена, одновременные записи с кворумом в одну и ту же таблицу будут отклонены (будет выполнена только одна из них).

Возможные значения:

-   0 — Выключена.
-   1 — Включена.

Значение по умолчанию: 1.

См. также:

-   [insert_quorum](#settings-insert_quorum)
-   [insert_quorum_timeout](#settings-insert_quorum_timeout)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## select_sequential_consistency {#settings-select_sequential_consistency}

Включает или выключает последовательную консистентность для запросов `SELECT`. Необходимо, чтобы `insert_quorum_parallel` была выключена (по умолчанию включена), а опция `insert_quorum` включена.

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

Использование

Когда последовательная консистентность включена, то ClickHouse позволит клиенту выполнить запрос `SELECT` только к тем репликам, которые содержат данные всех предыдущих запросов `INSERT`, выполненных с `insert_quorum`. Если клиент обратится к неполной реплике, то ClickHouse сгенерирует исключение. В запросе SELECT не будут участвовать данные, которые ещё не были записаны на кворум реплик.

Если `insert_quorum_parallel` включена (по умолчанию это так), тогда `select_sequential_consistency` не будет работать. Причина в том, что параллельные запросы `INSERT` можно записать в разные наборы реплик кворума, поэтому нет гарантии того, что в отдельно взятую реплику будут сделаны все записи.

См. также:

-   [insert_quorum](#settings-insert_quorum)
-   [insert_quorum_timeout](#settings-insert_quorum_timeout)
-   [insert_quorum_parallel](#settings-insert_quorum_parallel)

## insert_deduplicate {#settings-insert-deduplicate}

Включает и выключает дедупликацию для запросов `INSERT` (для Replicated\* таблиц).

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 1.

По умолчанию блоки, вставляемые в реплицируемые таблицы оператором `INSERT`, дедуплицируются (см. [Репликация данных](../../engines/table-engines/mergetree-family/replication.md)).

## deduplicate_blocks_in_dependent_materialized_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

Включает и выключает проверку дедупликации для материализованных представлений, которые получают данные из Replicated\* таблиц.

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

По умолчанию проверка дедупликации у материализованных представлений не производится, а наследуется от Replicated\* (основной) таблицы, за которой «следит» материализованное представление.
Т.е. если `INSERT` в основную таблицу д.б. пропущен (сдедуплицирован), то автоматически не будет вставки и в материализованные представления. Это имплементировано для того, чтобы работали материализованные представления, которые сильно группируют данные основных `INSERT`, до такой степени что блоки вставляемые в материализованные представления получаются одинаковыми для разных `INSERT` в основную таблицу.
Одновременно это «ломает» идемпотентность вставки в материализованные представления. Т.е. если `INSERT` был успешен в основную таблицу и неуспешен в таблицу материализованного представления (напр. из-за сетевого сбоя при коммуникации с Zookeeper), клиент получит ошибку и попытается повторить `INSERT`. Но вставки в материализованные представления произведено не будет, потому что дедупликация сработает на основной таблице. Настройка `deduplicate_blocks_in_dependent_materialized_views` позволяет это изменить. Т.е. при повторном `INSERT` будет произведена дедупликация на таблице материализованного представления, и повторный инсерт вставит данные в таблицу материализованного представления, которые не удалось вставить из-за сбоя первого `INSERT`.

## insert_deduplication_token {#insert_deduplication_token}

Этот параметр позволяет пользователю указать собственную семантику дедупликации в MergeTree/ReplicatedMergeTree.
Например, предоставляя уникальное значение параметра в каждом операторе INSERT,
пользователь может избежать дедупликации одних и тех же вставленных данных.

Возможные значения:

-  Любая строка

Значение по умолчанию: пустая строка (выключено).

`insert_deduplication_token` используется для дедупликации _только_ когда значение не пустое

Example:

```sql
CREATE TABLE test_table
( A Int64 )
ENGINE = MergeTree
ORDER BY A
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO test_table Values SETTINGS insert_deduplication_token = 'test' (1);

-- следующая вставка не будет дедуплицирована, потому что insert_deduplication_token отличается
INSERT INTO test_table Values SETTINGS insert_deduplication_token = 'test1' (1);

-- следующая вставка будет дедуплицирована, потому что insert_deduplication_token
-- тот же самый, что и один из предыдущих
INSERT INTO test_table Values SETTINGS insert_deduplication_token = 'test' (2);

SELECT * FROM test_table

┌─A─┐
│ 1 │
└───┘
┌─A─┐
│ 1 │
└───┘
```

## count_distinct_implementation {#settings-count_distinct_implementation}

Задаёт, какая из функций `uniq*` используется при выполнении конструкции [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference/count.md#agg_function-count).

Возможные значения:

-   [uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
-   [uniqExact](../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)

Значение по умолчанию: `uniqExact`.

## max_network_bytes {#settings-max-network-bytes}

Ограничивает объём данных (в байтах), который принимается или передается по сети при выполнении запроса. Параметр применяется к каждому отдельному запросу.

Возможные значения:

-   Положительное целое число.
-   0 — контроль объёма данных отключен.

Значение по умолчанию: 0.

## max_network_bandwidth {#settings-max-network-bandwidth}

Ограничивает скорость обмена данными по сети в байтах в секунду. Параметр применяется к каждому отдельному запросу.

Возможные значения:

-   Положительное целое число.
-   0 — контроль скорости передачи данных отключен.

Значение по умолчанию: 0.

## max_network_bandwidth_for_user {#settings-max-network-bandwidth-for-user}

Ограничивает скорость обмена данными по сети в байтах в секунду. Этот параметр применяется ко всем одновременно выполняемым запросам, запущенным одним пользователем.

Возможные значения:

-   Положительное целое число.
-   0 — управление скоростью передачи данных отключено.

Значение по умолчанию: 0.

## max_network_bandwidth_for_all_users {#settings-max-network-bandwidth-for-all-users}

Ограничивает скорость обмена данными по сети в байтах в секунду. Этот параметр применяется ко всем одновременно выполняемым запросам на сервере.

Возможные значения:

-   Положительное целое число.
-   0 — управление скоростью передачи данных отключено.

Значение по умолчанию: 0.

## skip_unavailable_shards {#settings-skip_unavailable_shards}

Включает или отключает тихий пропуск недоступных шардов.

Шард считается недоступным, если все его реплики недоступны. Реплика недоступна в следующих случаях:

-   ClickHouse не может установить соединение с репликой по любой причине.

        ClickHouse предпринимает несколько попыток подключиться к реплике. Если все попытки оказались неудачными, реплика считается недоступной.

-   Реплика не может быть разрешена с помощью DNS.

        Если имя хоста реплики не может быть разрешено с помощью DNS, это может указывать на следующие ситуации:

        - Нет записи DNS для хоста. Это может происходить в системах с динамическим DNS, например, [Kubernetes](https://kubernetes.io), где отключенные ноды не разрешаться с помощью DNS и это не ошибка.

        - Ошибка конфигурации. Конфигурационный файл ClickHouse может содержать неправильное имя хоста.

Возможные значения:

-   1 — пропуск включен.

        Если шард недоступен, то ClickHouse возвращает результат, основанный на неполных данных и не оповещает о проблемах с доступностью хостов.

-   0 — пропуск выключен.

        Если шард недоступен, то ClickHouse генерирует исключение.

Значение по умолчанию: 0.

## distributed_push_down_limit {#distributed-push-down-limit}

Включает или отключает [LIMIT](#limit), применяемый к каждому шарду по отдельности.

Это позволяет избежать:
- отправки дополнительных строк по сети;
- обработки строк за пределами ограничения для инициатора.

Начиная с версии 21.9 вы больше не сможете получить неточные результаты, так как `distributed_push_down_limit` изменяет выполнение запроса только в том случае, если выполнено хотя бы одно из условий:
- `distributed_group_by_no_merge` > 0.
- запрос **не содержит** `GROUP BY`/`DISTINCT`/`LIMIT BY`, но содержит `ORDER BY`/`LIMIT`.
- запрос **содержит** `GROUP BY`/`DISTINCT`/`LIMIT BY` с `ORDER BY`/`LIMIT` и:
  - включена настройка [optimize_skip_unused_shards](#optimize-skip-unused-shards).
  - включена настройка `optimize_distributed_group_by_sharding_key`.

Возможные значения:

-    0 — выключена.
-    1 — включена.

Значение по умолчанию: `1`.

См. также:

-   [optimize_skip_unused_shards](#optimize-skip-unused-shards)

## optimize_skip_unused_shards {#optimize-skip-unused-shards}

Включает или отключает пропуск неиспользуемых шардов для запросов [SELECT](../../sql-reference/statements/select/index.md) , в которых условие ключа шардирования задано в секции `WHERE/PREWHERE`. Предполагается, что данные распределены с помощью ключа шардирования, в противном случае запрос выдаст неверный результат.

Возможные значения:

-    0 — Выключена.
-    1 — Включена.

Значение по умолчанию: 0

## optimize_skip_unused_shards_nesting {#optimize-skip-unused-shards-nesting}

Контролирует настройку [`optimize_skip_unused_shards`](#optimize-skip-unused-shards) (поэтому все еще требует `optimize_skip_unused_shards`) в зависимости от вложенности распределенного запроса (когда у вас есть `Distributed` таблица которая смотрит на другую `Distributed` таблицу).

Возможные значения:

-    0 — Выключена, `optimize_skip_unused_shards` работает всегда.
-    1 — Включает `optimize_skip_unused_shards` только для 1-ого уровня вложенности.
-    2 — Включает `optimize_skip_unused_shards` для 1-ого и 2-ого уровня вложенности.

Значение по умолчанию: 0

## force_optimize_skip_unused_shards {#settings-force_optimize_skip_unused_shards}

Разрешает или запрещает выполнение запроса, если настройка [optimize_skip_unused_shards](#optimize-skip-unused-shards) включена, а пропуск неиспользуемых шардов невозможен. Если данная настройка включена и пропуск невозможен, ClickHouse генерирует исключение.

Возможные значения:

-    0 — Выключена, `force_optimize_skip_unused_shards` работает всегда.
-    1 — Включает `force_optimize_skip_unused_shards` только для 1-ого уровня вложенности.
-    2 — Включает `force_optimize_skip_unused_shards` для 1-ого и 2-ого уровня вложенности.

Значение по умолчанию: 0

## force_optimize_skip_unused_shards_nesting {#settings-force_optimize_skip_unused_shards_nesting}

Контролирует настройку [`force_optimize_skip_unused_shards`](#settings-force_optimize_skip_unused_shards) (поэтому все еще требует `optimize_skip_unused_shards`) в зависимости от вложенности распределенного запроса (когда у вас есть `Distributed` таблица которая смотрит на другую `Distributed` таблицу).

Возможные значения:

-   0 - Выключена, `force_optimize_skip_unused_shards` работает всегда.
-   1 — Включает `force_optimize_skip_unused_shards` только для 1-ого уровня вложенности.
-   2 — Включает `force_optimize_skip_unused_shards` для 1-ого и 2-ого уровня вложенности.

Значение по умолчанию: 0

## force_optimize_skip_unused_shards_no_nested {#settings-force_optimize_skip_unused_shards_no_nested}

Сбрасывает [`optimize_skip_unused_shards`](#settings-force_optimize_skip_unused_shards) для вложенных `Distributed` таблиц.

Возможные значения:

-   1 — Включена.
-   0 — Выключена.

Значение по умолчанию: 0

## optimize_throw_if_noop {#setting-optimize_throw_if_noop}

Включает или отключает генерирование исключения в случаях, когда запрос [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) не выполняет мёрж.

По умолчанию, `OPTIMIZE` завершается успешно и в тех случаях, когда он ничего не сделал. Настройка позволяет отделить подобные случаи и включает генерирование исключения с поясняющим сообщением.

Возможные значения:

-   1 — генерирование исключения включено.
-   0 — генерирование исключения выключено.

Значение по умолчанию: 0.

## optimize_functions_to_subcolumns {#optimize-functions-to-subcolumns}

Включает или отключает оптимизацию путем преобразования некоторых функций к чтению подстолбцов, таким образом уменьшая объем данных для чтения.

Могут быть преобразованы следующие функции:

-   [length](../../sql-reference/functions/array-functions.md#array_functions-length) к чтению подстолбца [size0](../../sql-reference/data-types/array.md#array-size) subcolumn.
-   [empty](../../sql-reference/functions/array-functions.md#function-empty) к чтению подстолбца [size0](../../sql-reference/data-types/array.md#array-size) subcolumn.
-   [notEmpty](../../sql-reference/functions/array-functions.md#function-notempty) к чтению подстолбца [size0](../../sql-reference/data-types/array.md#array-size).
-   [isNull](../../sql-reference/operators/index.md#operator-is-null) к чтению подстолбца [null](../../sql-reference/data-types/nullable.md#finding-null).
-   [isNotNull](../../sql-reference/operators/index.md#is-not-null) к чтению подстолбца [null](../../sql-reference/data-types/nullable.md#finding-null).
-   [count](../../sql-reference/aggregate-functions/reference/count.md) к чтению подстолбца [null](../../sql-reference/data-types/nullable.md#finding-null).
-   [mapKeys](../../sql-reference/functions/tuple-map-functions.md#mapkeys) к чтению подстолбца [keys](../../sql-reference/data-types/map.md#map-subcolumns).
-   [mapValues](../../sql-reference/functions/tuple-map-functions.md#mapvalues) к чтению подстолбца [values](../../sql-reference/data-types/map.md#map-subcolumns).

Возможные значения:

-   0 — оптимизация отключена.
-   1 — оптимизация включена.

Значение по умолчанию: `0`.

## optimize_trivial_count_query {#optimize-trivial-count-query}

Включает или отключает оптимизацию простого запроса `SELECT count() FROM table` с использованием метаданных MergeTree. Если вы хотите управлять безопасностью на уровне строк, отключите оптимизацию.

Возможные значения:

   - 0 — оптимизация отключена.
   - 1 — оптимизация включена.

Значение по умолчанию: `1`.

См. также:

-   [optimize_functions_to_subcolumns](#optimize-functions-to-subcolumns)

## distributed_replica_error_half_life {#settings-distributed_replica_error_half_life}

-   Тип: секунды
-   Значение по умолчанию: 60 секунд

Управляет скоростью обнуления счетчика ошибок в распределенных таблицах. Предположим, реплика остается недоступна в течение какого-то времени, и за этот период накопилось 5 ошибок. Если настройка `distributed_replica_error_half_life` установлена в значение 1 секунда, то реплика снова будет считаться доступной через 3 секунды после последней ошибки.

См. также:

-   [load_balancing](#load_balancing-round_robin)
-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap](#settings-distributed_replica_error_cap)
-   [distributed_replica_max_ignored_errors](#settings-distributed_replica_max_ignored_errors)

## distributed_replica_error_cap {#settings-distributed_replica_error_cap}

-   Тип: unsigned int
-   Значение по умолчанию: 1000

Счетчик ошибок каждой реплики ограничен этим значением, чтобы одна реплика не накапливала слишком много ошибок.

См. также:

-   [load_balancing](#load_balancing-round_robin)
-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_half_life](#settings-distributed_replica_error_half_life)
-   [distributed_replica_max_ignored_errors](#settings-distributed_replica_max_ignored_errors)

## distributed_replica_max_ignored_errors {#settings-distributed_replica_max_ignored_errors}

-   Тип: unsigned int
-   Значение по умолчанию: 0

Количество ошибок, которые будут проигнорированы при выборе реплик (согласно алгоритму `load_balancing`).

См. также:

-   [load_balancing](#load_balancing-round_robin)
-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap](#settings-distributed_replica_error_cap)
-   [distributed_replica_error_half_life](#settings-distributed_replica_error_half_life)

## distributed_directory_monitor_sleep_time_ms {#distributed_directory_monitor_sleep_time_ms}

Основной интервал отправки данных движком таблиц [Distributed](../../engines/table-engines/special/distributed.md). Фактический интервал растёт экспоненциально при возникновении ошибок.

Возможные значения:

-   Положительное целое количество миллисекунд.

Значение по умолчанию: 100 миллисекунд.

## distributed_directory_monitor_max_sleep_time_ms {#distributed_directory_monitor_max_sleep_time_ms}

Максимальный интервал отправки данных движком таблиц [Distributed](../../engines/table-engines/special/distributed.md). Ограничивает экпоненциальный рост интервала, установленого настройкой [distributed_directory_monitor_sleep_time_ms](#distributed_directory_monitor_sleep_time_ms).

Возможные значения:

-   Положительное целое количество миллисекунд.

Значение по умолчанию: 30000 миллисекунд (30 секунд).

## distributed_directory_monitor_batch_inserts {#distributed_directory_monitor_batch_inserts}

Включает/выключает пакетную отправку вставленных данных.

Если пакетная отправка включена, то движок таблиц [Distributed](../../engines/table-engines/special/distributed.md) вместо того, чтобы отправлять каждый файл со вставленными данными по отдельности, старается отправить их все за одну операцию. Пакетная отправка улучшает производительность кластера за счет более оптимального использования ресурсов сервера и сети.

Возможные значения:

-   1 — включено.
-   0 — выключено.

Значение по умолчанию: 0.

## os_thread_priority {#setting-os-thread-priority}

Устанавливает приоритет ([nice](https://en.wikipedia.org/wiki/Nice_(Unix))) для потоков, исполняющих запросы. Планировщик ОС учитывает эти приоритеты при выборе следующего потока для исполнения на доступном ядре CPU.

:::warning "Предупреждение"
    Для использования этой настройки необходимо установить свойство `CAP_SYS_NICE`. Пакет `clickhouse-server` устанавливает его во время инсталляции. Некоторые виртуальные окружения не позволяют установить `CAP_SYS_NICE`. В этом случае, `clickhouse-server` выводит сообщение при запуске.
:::

Допустимые значения:

-   Любое значение из диапазона `[-20, 19]`.

Более низкие значения означают более высокий приоритет. Потоки с низкими значениями приоритета `nice` выполняются чаще, чем потоки с более высокими значениями. Высокие значения предпочтительно использовать для долгих неинтерактивных запросов, поскольку это позволяет бысто выделить ресурс в пользу коротких интерактивных запросов.

Значение по умолчанию: 0.

## query_profiler_real_time_period_ns {#query_profiler_real_time_period_ns}

Устанавливает период для таймера реального времени [профилировщика запросов](../../operations/optimizing-performance/sampling-query-profiler.md). Таймер реального времени считает wall-clock time.

Возможные значения:

-   Положительное целое число в наносекундах.

        Рекомендуемые значения:

            - 10000000 (100 раз в секунду) наносекунд и меньшее значение для одиночных запросов.
            - 1000000000 (раз в секунду) для профилирования в масштабе кластера.

-   0 для выключения таймера.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

Значение по умолчанию: 1000000000 наносекунд (раз в секунду).

См. также:

-   Системная таблица [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## query_profiler_cpu_time_period_ns {#query_profiler_cpu_time_period_ns}

Устанавливает период для таймера CPU [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Этот таймер считает только время CPU.

Возможные значения:

-   Положительное целое число в наносекундах.

        Рекомендуемые значения:

            - 10000000 (100 раз в секунду) наносекунд и большее значение для одиночных запросов.
            - 1000000000 (раз в секунду) для профилирования в масштабе кластера.

-   0 для выключения таймера.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

Значение по умолчанию: 1000000000 наносекунд.

См. также:

-   Системная таблица [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## allow_introspection_functions {#settings-allow_introspection_functions}

Включает или отключает [функции самоанализа](../../sql-reference/functions/introspection.md) для профилирования запросов.

Возможные значения:

-   1 — включены функции самоанализа.
-   0 — функции самоанализа отключены.

Значение по умолчанию: 0.

**См. также**

-   [Sampling Query Profiler](../optimizing-performance/sampling-query-profiler.md)
-   Системная таблица [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## input_format_parallel_parsing {#input-format-parallel-parsing}

Включает или отключает режим, при котором входящие данные разбиваются на части, парсинг каждой из которых осуществляется параллельно с сохранением исходного порядка. Поддерживается только для форматов [TSV](../../interfaces/formats.md#tabseparated), [TKSV](../../interfaces/formats.md#tskv), [CSV](../../interfaces/formats.md#csv) и [JSONEachRow](../../interfaces/formats.md#jsoneachrow).

Возможные значения:

-   1 — включен режим параллельного разбора.
-   0 — отключен режим параллельного разбора.

Значение по умолчанию: `1`.

## output_format_parallel_formatting {#output-format-parallel-formatting}

Включает или отключает режим, при котором исходящие данные форматируются параллельно с сохранением исходного порядка. Поддерживается только для форматов [TSV](../../interfaces/formats.md#tabseparated), [TKSV](../../interfaces/formats.md#tskv), [CSV](../../interfaces/formats.md#csv) и [JSONEachRow](../../interfaces/formats.md#jsoneachrow).

Возможные значения:

-   1 — включен режим параллельного форматирования.
-   0 — отключен режим параллельного форматирования.

Значение по умолчанию: `1`.

## min_chunk_bytes_for_parallel_parsing {#min-chunk-bytes-for-parallel-parsing}

-   Тип: unsigned int
-   Значение по умолчанию: 1 MiB

Минимальный размер блока в байтах, который каждый поток будет анализировать параллельно.

## output_format_avro_codec {#settings-output_format_avro_codec}

Устанавливает кодек сжатия, используемый для вывода файла Avro.

Тип: строка

Возможные значения:

-   `null` — без сжатия
-   `deflate` — сжать с помощью Deflate (zlib)
-   `snappy` — сжать с помощью [Snappy](https://google.github.io/snappy/)

Значение по умолчанию: `snappy` (если доступно) или `deflate`.

## output_format_avro_sync_interval {#settings-output_format_avro_sync_interval}

Устанавливает минимальный размер данных (в байтах) между маркерами синхронизации для выходного файла Avro.

Тип: unsigned int

Возможные значения: 32 (32 байта) - 1073741824 (1 GiB)

Значение по умолчанию: 32768 (32 KiB)

## merge_selecting_sleep_ms {#merge_selecting_sleep_ms}

Время ожидания для слияния выборки, если ни один кусок не выбран. Снижение времени ожидания приводит к частому выбору задач в пуле `background_schedule_pool` и увеличению количества запросов к Zookeeper в крупных кластерах.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `5000`.

## parallel_distributed_insert_select {#parallel_distributed_insert_select}

Включает параллельную обработку распределённых запросов `INSERT ... SELECT`.

Если при выполнении запроса `INSERT INTO distributed_table_a SELECT ... FROM distributed_table_b` оказывается, что обе таблицы находятся в одном кластере, то независимо от того [реплицируемые](../../engines/table-engines/mergetree-family/replication.md) они или нет, запрос  выполняется локально на каждом шарде.

Допустимые значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

## insert_distributed_sync {#insert_distributed_sync}

Включает или отключает режим синхронного добавления данных в распределенные таблицы (таблицы с движком [Distributed](../../engines/table-engines/special/distributed.md#distributed)).

По умолчанию ClickHouse вставляет данные в распределённую таблицу в асинхронном режиме. Если `insert_distributed_sync=1`, то данные вставляются сихронно, а запрос `INSERT` считается выполненным успешно, когда данные записаны на все шарды (по крайней мере на одну реплику для каждого шарда, если `internal_replication = true`).

Возможные значения:

-   0 — Данные добавляются в асинхронном режиме.
-   1 — Данные добавляются в синхронном режиме.

Значение по умолчанию: `0`.

**См. также**

-   [Движок Distributed](../../engines/table-engines/special/distributed.md#distributed)
-   [Управление распределёнными таблицами](../../sql-reference/statements/system.md#query-language-system-distributed)

## insert_distributed_one_random_shard {#insert_distributed_one_random_shard}

Включает или отключает режим вставки данных в [Distributed](../../engines/table-engines/special/distributed.md#distributed)) таблицу в случайный шард при отсутствии ключ шардирования.

По умолчанию при вставке данных в `Distributed` таблицу с несколькими шардами и при отсутствии ключа шардирования сервер ClickHouse будет отклонять любой запрос на вставку данных. Когда `insert_distributed_one_random_shard = 1`, вставки принимаются, а данные записываются в случайный шард.

Возможные значения:

-   0 — если у таблицы несколько шардов, но ключ шардирования отсутствует, вставка данных отклоняется.
-   1 — если ключ шардирования отсутствует, то вставка данных осуществляется в случайный шард среди всех доступных шардов.

Значение по умолчанию: `0`.

## insert_shard_id {#insert_shard_id}

Если не `0`, указывает, в какой шард [Distributed](../../engines/table-engines/special/distributed.md#distributed) таблицы данные будут вставлены синхронно.

Если значение настройки `insert_shard_id` указано неверно, сервер выдаст ошибку.

Узнать количество шардов `shard_num` на кластере `requested_cluster` можно из конфигурации сервера, либо используя запрос:

``` sql
SELECT uniq(shard_num) FROM system.clusters WHERE cluster = 'requested_cluster';
```

Возможные значения:

-   0 — выключено.
-   Любое число от `1` до `shards_num` соответствующей [Distributed](../../engines/table-engines/special/distributed.md#distributed) таблицы.

Значение по умолчанию: `0`.

**Пример**

Запрос:

```sql
CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number;
CREATE TABLE x_dist AS x ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), x);
INSERT INTO x_dist SELECT * FROM numbers(5) SETTINGS insert_shard_id = 1;
SELECT * FROM x_dist ORDER BY number ASC;
```

Результат:

``` text
┌─number─┐
│      0 │
│      0 │
│      1 │
│      1 │
│      2 │
│      2 │
│      3 │
│      3 │
│      4 │
│      4 │
└────────┘
```

## validate_polygons {#validate_polygons}

Включает или отключает генерирование исключения в функции [pointInPolygon](../../sql-reference/functions/geo/index.md#pointinpolygon), если многоугольник самопересекающийся или самокасающийся.

Допустимые значения:

- 0 — генерирование исключения отключено. `pointInPolygon` принимает недопустимые многоугольники и возвращает для них, возможно, неверные результаты.
- 1 — генерирование исключения включено.

Значение по умолчанию: 1.

## always_fetch_merged_part {#always_fetch_merged_part}

Запрещает слияние данных для таблиц семейства [Replicated*MergeTree](../../engines/table-engines/mergetree-family/replication.md).

Если слияние запрещено, реплика никогда не выполняет слияние отдельных кусков данных, а всегда загружает объединённые данные из других реплик. Если объединённых данных пока нет, реплика ждет их появления. Нагрузка на процессор и диски на реплике уменьшается, но нагрузка на сеть в кластере возрастает. Настройка может быть полезна на репликах с относительно слабыми процессорами или медленными дисками, например, на репликах для хранения архивных данных.

Возможные значения:

-   0 — таблицы семейства `Replicated*MergeTree` выполняют слияние данных на реплике.
-   1 — таблицы семейства `Replicated*MergeTree` не выполняют слияние данных на реплике, а загружают объединённые данные из других реплик.

Значение по умолчанию: 0.

**См. также:**

-   [Репликация данных](../../engines/table-engines/mergetree-family/replication.md)

## transform_null_in {#transform_null_in}

Разрешает сравнивать значения [NULL](../../sql-reference/syntax.md#null-literal) в операторе [IN](../../sql-reference/operators/in.md).

По умолчанию, значения `NULL` нельзя сравнивать, поскольку `NULL` обозначает неопределённое значение. Следовательно, сравнение `expr = NULL` должно всегда возвращать `false`. С этой настройкой `NULL = NULL` возвращает `true` в операторе `IN`.

Possible values:

-   0 — Сравнение значений `NULL` в операторе `IN` возвращает `false`.
-   1 — Сравнение значений `NULL` в операторе `IN` возвращает `true`.

Значение по умолчанию: 0.

**Пример**

Рассмотрим таблицу `null_in`:

```text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
│    3 │     3 │
└──────┴───────┘
```

Consider the `null_in` table:

```text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
│    3 │     3 │
└──────┴───────┘
```

Запрос:

```sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 0;
```

Ответ:

```text
┌──idx─┬────i─┐
│    1 │    1 │
└──────┴──────┘
```

Запрос:

```sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 1;
```

Ответ:

```text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
└──────┴───────┘
```

**См. также**

-   [Обработка значения NULL в операторе IN](../../sql-reference/operators/in.md#in-null-processing)

## low_cardinality_max_dictionary_size {#low_cardinality_max_dictionary_size}

Задает максимальный размер общего глобального словаря (в строках) для типа данных `LowCardinality`, который может быть записан в файловую систему хранилища. Настройка предотвращает проблемы с оперативной памятью в случае неограниченного увеличения словаря. Все данные, которые не могут быть закодированы из-за ограничения максимального размера словаря, ClickHouse записывает обычным способом.

Допустимые значения:

-   Положительное целое число.

Значение по умолчанию: 8192.

## low_cardinality_use_single_dictionary_for_part {#low_cardinality_use_single_dictionary_for_part}

Включает или выключает использование единого словаря для куска (парта).

По умолчанию сервер ClickHouse следит за размером словарей, и если словарь переполняется, сервер создает следующий. Чтобы запретить создание нескольких словарей, задайте настройку `low_cardinality_use_single_dictionary_for_part = 1`.

Допустимые значения:

-   1 — Создание нескольких словарей для частей данных запрещено.
-   0 — Создание нескольких словарей для частей данных не запрещено.

Значение по умолчанию: 0.

## low_cardinality_allow_in_native_format {#low_cardinality_allow_in_native_format}

Разрешает или запрещает использование типа данных `LowCardinality` с форматом данных [Native](../../interfaces/formats.md#native).

Если использование типа `LowCardinality` ограничено, сервер ClickHouse преобразует столбцы `LowCardinality` в обычные столбцы для запросов `SELECT`, а обычные столбцы - в столбцы `LowCardinality` для запросов `INSERT`.

В основном настройка используется для сторонних клиентов, не поддерживающих тип данных `LowCardinality`.

Допустимые значения:

-   1 — Использование `LowCardinality` не ограничено.
-   0 — Использование `LowCardinality` ограничено.

Значение по умолчанию: 1.

## allow_suspicious_low_cardinality_types {#allow_suspicious_low_cardinality_types}

Разрешает или запрещает использование типа данных `LowCardinality` с типами данных с фиксированным размером 8 байт или меньше: числовые типы данных и `FixedString (8_bytes_or_less)`.

Для небольших фиксированных значений использование `LowCardinality` обычно неэффективно, поскольку ClickHouse хранит числовой индекс для каждой строки. В результате:

-   Используется больше дискового пространства.
-   Потребление ОЗУ увеличивается, в зависимости от размера словаря.
-   Некоторые функции работают медленнее из-за дополнительных операций кодирования.

Время слияния в таблицах на движке [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) также может увеличиться по описанным выше причинам.

Допустимые значения:

-   1 — Использование `LowCardinality` не ограничено.
-   0 — Использование `LowCardinality` ограничено.

Значение по умолчанию: 0.

## background_buffer_flush_schedule_pool_size {#background_buffer_flush_schedule_pool_size}

Задает количество потоков для выполнения фонового сброса данных в таблицах с движком [Buffer](../../engines/table-engines/special/buffer.md). Настройка применяется при запуске сервера ClickHouse и не может быть изменена в пользовательском сеансе.

Допустимые значения:

-   Положительное целое число.

Значение по умолчанию: 16.

## background_move_pool_size {#background_move_pool_size}

Задает количество потоков для фоновых перемещений кусков между дисками. Работает для таблиц с движком [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes). Настройка применяется при запуске сервера ClickHouse и не может быть изменена в пользовательском сеансе.

Допустимые значения:

-   Положительное целое число.

Значение по умолчанию: 8.

## background_schedule_pool_size {#background_schedule_pool_size}

Задает количество потоков для выполнения фоновых задач. Работает для [реплицируемых](../../engines/table-engines/mergetree-family/replication.md) таблиц, стримов в [Kafka](../../engines/table-engines/integrations/kafka.md) и обновления IP адресов у записей во внутреннем [DNS кеше](../server-configuration-parameters/settings.md#server-settings-dns-cache-update-period). Настройка применяется при запуске сервера ClickHouse и не может быть изменена в пользовательском сеансе.

Допустимые значения:

-   Положительное целое число.

Значение по умолчанию: 128.

## background_fetches_pool_size {#background_fetches_pool_size}

Задает количество потоков для скачивания кусков данных для [реплицируемых](../../engines/table-engines/mergetree-family/replication.md) таблиц. Настройка применяется при запуске сервера ClickHouse и не может быть изменена в пользовательском сеансе. Для использования в продакшене с частыми небольшими вставками или медленным кластером ZooKeeper рекомендуется использовать значение по умолчанию.

Допустимые значения:

-   Положительное целое число.

Значение по умолчанию: 8.

## background_distributed_schedule_pool_size {#background_distributed_schedule_pool_size}

Задает количество потоков для выполнения фоновых задач. Работает для таблиц с движком [Distributed](../../engines/table-engines/special/distributed.md). Настройка применяется при запуске сервера ClickHouse и не может быть изменена в пользовательском сеансе.

Допустимые значения:

-   Положительное целое число.

Значение по умолчанию: 16.

## background_message_broker_schedule_pool_size {#background_message_broker_schedule_pool_size}

Задает количество потоков для фонового потокового вывода сообщений. Настройка применяется при запуске сервера ClickHouse и не может быть изменена в пользовательском сеансе.

Допустимые значения:

-   Положительное целое число.

Значение по умолчанию: 16.

**Смотрите также**

-   Движок [Kafka](../../engines/table-engines/integrations/kafka.md#kafka).
-   Движок [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md#rabbitmq-engine).

## format_avro_schema_registry_url {#format_avro_schema_registry_url}

Задает URL реестра схем [Confluent](https://docs.confluent.io/current/schema-registry/index.html) для использования с форматом [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent).

Значение по умолчанию: `Пустая строка`.

## input_format_avro_allow_missing_fields {#input_format_avro_allow_missing_fields}
Позволяет использовать данные, которых не нашлось в схеме формата [Avro](../../interfaces/formats.md#data-format-avro) или [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent). Если поле не найдено в схеме, ClickHouse подставит значение по умолчанию вместо исключения.

Возможные значения:

-   0 — Выключена.
-   1 — Включена.

Значение по умолчанию: `0`.

## min_insert_block_size_rows_for_materialized_views {#min-insert-block-size-rows-for-materialized-views}

Устанавливает минимальное количество строк в блоке, который может быть вставлен в таблицу запросом `INSERT`. Блоки меньшего размера склеиваются в блоки большего размера. Настройка применяется только для блоков, вставляемых в [материализованное представление](../../sql-reference/statements/create/view.md#create-view). Настройка позволяет избежать избыточного потребления памяти.

Допустимые значения:

-   Положительное целое число.
-   0 — Склейка блоков выключена.

Значение по умолчанию: 1048576.

**См. также:**

-   [min_insert_block_size_rows](#min-insert-block-size-rows)

## min_insert_block_size_bytes_for_materialized_views {#min-insert-block-size-bytes-for-materialized-views}

Устанавливает минимальное количество байтов в блоке, который может быть вставлен в таблицу запросом `INSERT`. Блоки меньшего размера склеиваются в блоки большего размера. Настройка применяется только для блоков, вставляемых в [материализованное представление](../../sql-reference/statements/create/view.md#create-view). Настройка позволяет избежать избыточного потребления памяти.

Допустимые значения:

-   Положительное целое число.
-   0 — Склейка блоков выключена.

Значение по умолчанию: 268435456.

**См. также:**

-   [min_insert_block_size_bytes](#min-insert-block-size-bytes)

## output_format_pretty_grid_charset {#output-format-pretty-grid-charset}

Позволяет изменить кодировку, которая используется для отрисовки таблицы при выводе результатов запросов. Доступны следующие кодировки: UTF-8, ASCII.

**Пример**

``` text
SET output_format_pretty_grid_charset = 'UTF-8';
SELECT * FROM a;
┌─a─┐
│ 1 │
└───┘

SET output_format_pretty_grid_charset = 'ASCII';
SELECT * FROM a;
+-a-+
| 1 |
+---+
```

## optimize_read_in_order {#optimize_read_in_order}

Включает или отключает оптимизацию в запросах [SELECT](../../sql-reference/statements/select/index.md) с секцией [ORDER BY](../../sql-reference/statements/select/order-by.md#optimize_read_in_order) при работе с таблицами семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Возможные значения:

-   0 — оптимизация отключена.
-   1 — оптимизация включена.

Значение по умолчанию: `1`.

**См. также**

-   [Оптимизация чтения данных](../../sql-reference/statements/select/order-by.md#optimize_read_in_order) в секции `ORDER BY`

## optimize_aggregation_in_order {#optimize_aggregation_in_order}

Включает или отключает оптимизацию в запросах [SELECT](../../sql-reference/statements/select/index.md) с секцией [GROUP BY](../../sql-reference/statements/select/group-by.md) при наличии подходящих ключей сортировки. Используется при работе с таблицами [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Возможные значения:

-   0 — оптимизация по ключу сортировки отключена.
-   1 — оптимизация по ключу сортировки включена.

Значение по умолчанию: `0`.

**См. также**

-   [Оптимизация GROUP BY для отсортированных таблиц](../../sql-reference/statements/select/group-by.md#aggregation-in-order)

## mutations_sync {#mutations_sync}

Позволяет выполнять запросы `ALTER TABLE ... UPDATE|DELETE` ([мутации](../../sql-reference/statements/alter/index.md#mutations)) синхронно.

Возможные значения:

-   0 - мутации выполняются асинхронно.
-   1 - запрос ждет завершения всех мутаций на текущем сервере.
-   2 - запрос ждет завершения всех мутаций на всех репликах (если они есть).

Значение по умолчанию: `0`.

**См. также**

-   [Синхронность запросов ALTER](../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
-   [Мутации](../../sql-reference/statements/alter/index.md#mutations)

## ttl_only_drop_parts {#ttl_only_drop_parts}

Для таблиц [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) включает или отключает  возможность полного удаления кусков данных, в которых все записи устарели.

Когда настройка `ttl_only_drop_parts` отключена (т.е. по умолчанию), сервер лишь удаляет устаревшие записи в соответствии с их временем жизни (TTL).

Когда настройка `ttl_only_drop_parts` включена, сервер целиком удаляет куски данных, в которых все записи устарели.

Удаление целых кусков данных вместо удаления отдельных записей позволяет устанавливать меньший таймаут `merge_with_ttl_timeout` и уменьшает нагрузку на сервер, что способствует росту производительности.

Возможные значения:

-   0 — Возможность удаления целых кусков данных отключена.
-   1 — Возможность удаления целых кусков данных включена.

Значение по умолчанию: `0`.

**См. также**

-   [Секции и настройки запроса CREATE TABLE](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses) (настройка `merge_with_ttl_timeout`)
-   [Table TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)

## output_format_pretty_max_value_width {#output_format_pretty_max_value_width}

Ограничивает длину значения, выводимого в формате [Pretty](../../interfaces/formats.md#pretty). Если значение длиннее указанного количества символов, оно обрезается.

Возможные значения:

-   Положительное целое число.
-   0 — значение обрезается полностью.

Значение по умолчанию: `10000` символов.

**Примеры**

Запрос:

```sql
SET output_format_pretty_max_value_width = 10;
SELECT range(number) FROM system.numbers LIMIT 10 FORMAT PrettyCompactNoEscapes;
```
Результат:

```text
┌─range(number)─┐
│ []            │
│ [0]           │
│ [0,1]         │
│ [0,1,2]       │
│ [0,1,2,3]     │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
└───────────────┘
```

Запрос, где длина выводимого значения ограничена 0 символов:

```sql
SET output_format_pretty_max_value_width = 0;
SELECT range(number) FROM system.numbers LIMIT 5 FORMAT PrettyCompactNoEscapes;
```
Результат:

```text
┌─range(number)─┐
│ ⋯             │
│ ⋯             │
│ ⋯             │
│ ⋯             │
│ ⋯             │
└───────────────┘
```

## output_format_pretty_row_numbers {#output_format_pretty_row_numbers}

Включает режим отображения номеров строк для запросов, выводимых в формате [Pretty](../../interfaces/formats.md#pretty).

Возможные значения:

-   0 — номера строк не выводятся.
-   1 — номера строк выводятся.

Значение по умолчанию: `0`.

**Пример**

Запрос:

```sql
SET output_format_pretty_row_numbers = 1;
SELECT TOP 3 name, value FROM system.settings;
```

Результат:

```text
   ┌─name────────────────────┬─value───┐
1. │ min_compress_block_size │ 65536   │
2. │ max_compress_block_size │ 1048576 │
3. │ max_block_size          │ 65505   │
   └─────────────────────────┴─────────┘
```

## system_events_show_zero_values {#system_events_show_zero_values}

Позволяет выбрать события с нулевыми значениями из таблицы [`system.events`](../../operations/system-tables/events.md).

В некоторые системы мониторинга вам нужно передать значения всех измерений (для каждой контрольной точки), даже если в результате — "0".

Возможные значения:

-   0 — настройка отключена — вы получите все события.
-   1 — настройка включена — вы сможете отсортировать события по нулевым и остальным значениям.

Значение по умолчанию: `0`.

**Примеры**

Запрос

```sql
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Результат

```text
Ok.
```

Запрос

```sql
SET system_events_show_zero_values = 1;
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Результат

```text
┌─event────────────────────┬─value─┬─description───────────────────────────────────────────┐
│ QueryMemoryLimitExceeded │     0 │ Number of times when memory limit exceeded for query. │
└──────────────────────────┴───────┴───────────────────────────────────────────────────────┘
```

## lock_acquire_timeout {#lock_acquire_timeout}

Устанавливает, сколько секунд сервер ожидает возможности выполнить блокировку таблицы.

Таймаут устанавливается для защиты от взаимоблокировки при выполнении операций чтения или записи. Если время ожидания истекло, а блокировку выполнить не удалось, сервер возвращает исключение с кодом `DEADLOCK_AVOIDED` и сообщением "Locking attempt timed out! Possible deadlock avoided. Client should retry." ("Время ожидания блокировки истекло! Возможная взаимоблокировка предотвращена. Повторите запрос.").

Возможные значения:

-   Положительное целое число (в секундах).
-   0 — таймаут не устанавливается.

Значение по умолчанию: `120` секунд.

## cast_keep_nullable {#cast_keep_nullable}

Включает или отключает сохранение типа `Nullable` для аргумента функции [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast).

Если настройка включена, то когда в функцию `CAST` передается аргумент с типом `Nullable`, функция возвращает результат, также преобразованный к типу `Nullable`.
Если настройка отключена, то функция `CAST` всегда возвращает результат строго указанного типа.

Возможные значения:

-  0 — функция `CAST` преобразует аргумент строго к указанному типу.
-  1 — если аргумент имеет тип `Nullable`, то функция `CAST` преобразует его к типу `Nullable` для указанного типа.

Значение по умолчанию: `0`.

**Примеры**

Запрос возвращает аргумент, преобразованный строго к указанному типу:

```sql
SET cast_keep_nullable = 0;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Результат:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Int32                                             │
└───┴───────────────────────────────────────────────────┘
```

Запрос возвращает аргумент, преобразованный к типу `Nullable` для указанного типа:

```sql
SET cast_keep_nullable = 1;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Результат:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Nullable(Int32)                                   │
└───┴───────────────────────────────────────────────────┘
```

**См. также**

-   Функция [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast)

## persistent {#persistent}

Отключает перманентность для табличных движков [Set](../../engines/table-engines/special/set.md#set) и [Join](../../engines/table-engines/special/join.md#join).

Уменьшает расходы на ввод/вывод. Может быть полезно, когда требуется высокая производительность, а перманентность не обязательна.

Возможные значения:

- 1 — включено.
- 0 — отключено.

Значение по умолчанию: `1`.

## format_csv_null_representation {#format_csv_null_representation}

Определяет представление `NULL` для формата выходных данных [CSV](../../interfaces/formats.md#csv). Пользователь может установить в качестве значения любую строку, например, `My NULL`.

Значение по умолчанию: `\N`.

**Примеры**

Запрос:

```sql
SELECT * FROM csv_custom_null FORMAT CSV;
```

Результат:

```text
788
\N
\N
```

Запрос:

```sql
SET format_csv_null_representation = 'My NULL';
SELECT * FROM csv_custom_null FORMAT CSV;
```

Результат:

```text
788
My NULL
My NULL
```

## format_tsv_null_representation {#format_tsv_null_representation}

Определяет представление `NULL` для формата выходных данных [TSV](../../interfaces/formats.md#tabseparated). Пользователь может установить в качестве значения любую строку.

Значение по умолчанию: `\N`.

**Примеры**

Запрос

```sql
SELECT * FROM tsv_custom_null FORMAT TSV;
```

Результат

```text
788
\N
\N
```

Запрос

```sql
SET format_tsv_null_representation = 'My NULL';
SELECT * FROM tsv_custom_null FORMAT TSV;
```

Результат

```text
788
My NULL
My NULL
```

## output_format_json_array_of_rows {#output-format-json-array-of-rows}

Позволяет выводить все строки в виде массива JSON в формате [JSONEachRow](../../interfaces/formats.md#jsoneachrow).

Возможные значения:

-   1 — ClickHouse выводит все строки в виде массива и при этом каждую строку в формате `JSONEachRow`.
-   0 — ClickHouse выводит каждую строку отдельно в формате `JSONEachRow`.

Значение по умолчанию: `0`.

**Пример запроса с включенной настройкой**

Запрос:

```sql
SET output_format_json_array_of_rows = 1;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Результат:

```text
[
{"number":"0"},
{"number":"1"},
{"number":"2"}
]
```

**Пример запроса с отключенной настройкой**

Запрос:

```sql
SET output_format_json_array_of_rows = 0;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Результат:

```text
{"number":"0"}
{"number":"1"}
{"number":"2"}
```

## allow_nullable_key {#allow-nullable-key}

Включает или отключает поддержку типа [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable) для ключей таблиц [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree).

Возможные значения:

- 1 — включает поддержку типа `Nullable` для ключей таблиц.
- 0 — отключает поддержку типа `Nullable` для ключей таблиц.

Значение по умолчанию: `0`.


## aggregate_functions_null_for_empty {#aggregate_functions_null_for_empty}

Включает или отключает перезапись всех агрегатных функций в запросе, с добавлением к ним суффикса [-OrNull](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-ornull). Включите для совместимости со стандартом SQL.
Реализуется с помощью перезаписи запросов (аналогично настройке [count_distinct_implementation](#settings-count_distinct_implementation)), чтобы получить согласованные результаты для распределенных запросов.

Возможные значения:

-   0 — выключена.
-   1 — включена.

Значение по умолчанию: 0.

**Пример**

Рассмотрим запрос с агрегирующими функциями:
```sql
SELECT SUM(-1), MAX(0) FROM system.one WHERE 0;
```

Результат запроса с настройкой `aggregate_functions_null_for_empty = 0`:
```text
┌─SUM(-1)─┬─MAX(0)─┐
│       0 │      0 │
└─────────┴────────┘
```

Результат запроса с настройкой `aggregate_functions_null_for_empty = 1`:
```text
┌─SUMOrNull(-1)─┬─MAXOrNull(0)─┐
│          NULL │         NULL │
└───────────────┴──────────────┘
```


## union_default_mode {#union-default-mode}

Устанавливает режим объединения результатов `SELECT` запросов. Настройка используется только при совместном использовании с [UNION](../../sql-reference/statements/select/union.md) без явного указания `UNION ALL` или `UNION DISTINCT`.

Возможные значения:

-   `'DISTINCT'` — ClickHouse выводит строки в результате объединения результатов запросов, удаляя повторяющиеся строки.
-   `'ALL'` — ClickHouse выводит все строки в результате объединения результатов запросов, включая повторяющиеся строки.
-   `''` — Clickhouse генерирует исключение при использовании с `UNION`.

Значение по умолчанию: `''`.

Смотрите примеры в разделе [UNION](../../sql-reference/statements/select/union.md).

## data_type_default_nullable {#data_type_default_nullable}

Позволяет использовать по умолчанию тип данных [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable) в определении столбца без явных модификаторов [NULL или NOT NULL](../../sql-reference/statements/create/table.md#null-modifiers).

Возможные значения:

- 1 — типы данных в определении столбца заданы по умолчанию как `Nullable`.
- 0 — типы данных в определении столбца не заданы по умолчанию как `Nullable`.

Значение по умолчанию: `0`.

## execute_merges_on_single_replica_time_threshold {#execute-merges-on-single-replica-time-threshold}

Включает особую логику выполнения слияний на репликах.

Возможные значения:

-   Положительное целое число (в секундах).
-   0 — не используется особая логика выполнения слияний. Слияния происходят обычным образом на всех репликах.

Значение по умолчанию: `0`.

**Использование**

Выбирается одна реплика для выполнения слияния. Устанавливается порог времени с момента начала слияния. Другие реплики ждут завершения слияния, а затем скачивают результат. Если время выполнения слияния превышает установленный порог и выбранная реплика не выполняет слияние, тогда слияние выполняется на других репликах как обычно.

Большие значения этой настройки могут привести к задержкам репликации.

Эта настройка полезна, когда скорость слияния ограничивается мощностью процессора, а не скоростью операций ввода-вывода (при выполнении "тяжелого" сжатия данных, при расчете агрегатных функций или выражений по умолчанию, требующих большого объема вычислений, или просто при большом количестве мелких слияний).

## max_final_threads {#max-final-threads}

Устанавливает максимальное количество параллельных потоков для фазы чтения данных запроса `SELECT` с модификатором [FINAL](../../sql-reference/statements/select/from.md#select-from-final).

Возможные значения:

-   Положительное целое число.
-   0 или 1 — настройка отключена. `SELECT` запросы выполняются в один поток.

Значение по умолчанию: `16`.

## opentelemetry_start_trace_probability {#opentelemetry-start-trace-probability}

Задает вероятность того, что ClickHouse начнет трассировку для выполненных запросов (если не указан [входящий контекст](https://www.w3.org/TR/trace-context/) трассировки).

Возможные значения:

-   0 — трассировка для выполненных запросов отключена (если не указан входящий контекст трассировки).
-   Положительное число с плавающей точкой в диапазоне [0..1]. Например, при значении настройки, равной `0,5`, ClickHouse начнет трассировку в среднем для половины запросов.
-   1 — трассировка для всех выполненных запросов включена.

Значение по умолчанию: `0`.

## optimize_on_insert {#optimize-on-insert}

Включает или выключает преобразование данных перед добавлением в таблицу, как будто над добавляемым блоком предварительно было произведено слияние (в соответствии с движком таблицы).

Возможные значения:

-   0 — выключена
-   1 — включена.

Значение по умолчанию: 1.

**Пример**

Сравните добавление данных при включенной и выключенной настройке:

Запрос:

```sql
SET optimize_on_insert = 1;

CREATE TABLE test1 (`FirstTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY FirstTable;

INSERT INTO test1 SELECT number % 2 FROM numbers(5);

SELECT * FROM test1;

SET optimize_on_insert = 0;

CREATE TABLE test2 (`SecondTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY SecondTable;

INSERT INTO test2 SELECT number % 2 FROM numbers(5);

SELECT * FROM test2;
```

Результат:

``` text
┌─FirstTable─┐
│          0 │
│          1 │
└────────────┘

┌─SecondTable─┐
│           0 │
│           0 │
│           0 │
│           1 │
│           1 │
└─────────────┘
```

Обратите внимание на то, что эта настройка влияет на поведение [материализованных представлений](../../sql-reference/statements/create/view.md#materialized) и БД [MaterializedMySQL](../../engines/database-engines/materialized-mysql.md).

## engine_file_empty_if_not_exists {#engine-file-empty_if-not-exists}

Включает или отключает возможность выполнять запрос `SELECT` к таблице на движке [File](../../engines/table-engines/special/file.md), не содержащей файл.

Возможные значения:
- 0 — запрос `SELECT` генерирует исключение.
- 1 — запрос `SELECT` возвращает пустой результат.

Значение по умолчанию: `0`.

## engine_file_truncate_on_insert {#engine-file-truncate-on-insert}

Включает или выключает удаление данных из таблицы до вставки в таблицу на движке [File](../../engines/table-engines/special/file.md).

Возможные значения:
- 0 — запрос `INSERT` добавляет данные в конец файла после существующих.
- 1 — `INSERT` удаляет имеющиеся в файле данные и замещает их новыми.

Значение по умолчанию: `0`.

## allow_experimental_geo_types {#allow-experimental-geo-types}

Разрешает использование экспериментальных типов данных для работы с [географическими структурами](../../sql-reference/data-types/geo.md).

Возможные значения:
-   0 — использование типов данных для работы с географическими структурами не поддерживается.
-   1 — использование типов данных для работы с географическими структурами поддерживается.

Значение по умолчанию: `0`.

## database_atomic_wait_for_drop_and_detach_synchronously {#database_atomic_wait_for_drop_and_detach_synchronously}

Добавляет модификатор `SYNC` ко всем запросам `DROP` и `DETACH`.

Возможные значения:

-   0 — Запросы будут выполняться с задержкой.
-   1 — Запросы будут выполняться без задержки.

Значение по умолчанию: `0`.

## show_table_uuid_in_table_create_query_if_not_nil {#show_table_uuid_in_table_create_query_if_not_nil}

Устанавливает отображение запроса `SHOW TABLE`.

Возможные значения:

-   0 — Запрос будет отображаться без UUID таблицы.
-   1 — Запрос будет отображаться с UUID таблицы.

Значение по умолчанию: `0`.

## allow_experimental_live_view {#allow-experimental-live-view}

Включает экспериментальную возможность использования [LIVE-представлений](../../sql-reference/statements/create/view.md#live-view).

Возможные значения:
- 0 — живые представления не поддерживаются.
- 1 — живые представления поддерживаются.

Значение по умолчанию: `0`.

## live_view_heartbeat_interval {#live-view-heartbeat-interval}

Задает интервал в секундах для периодической проверки существования [LIVE VIEW](../../sql-reference/statements/create/view.md#live-view).

Значение по умолчанию: `15`.

## max_live_view_insert_blocks_before_refresh {#max-live-view-insert-blocks-before-refresh}

Задает наибольшее число вставок, после которых запрос на формирование [LIVE VIEW](../../sql-reference/statements/create/view.md#live-view) исполняется снова.

Значение по умолчанию: `64`.

## temporary_live_view_timeout {#temporary-live-view-timeout}

Задает время в секундах, после которого [LIVE VIEW](../../sql-reference/statements/create/view.md#live-view) удаляется.

Значение по умолчанию: `5`.

## periodic_live_view_refresh {#periodic-live-view-refresh}

Задает время в секундах, по истечении которого [LIVE VIEW](../../sql-reference/statements/create/view.md#live-view) с установленным автообновлением обновляется.

Значение по умолчанию: `60`.

## check_query_single_value_result {#check_query_single_value_result}

Определяет уровень детализации результата для запросов [CHECK TABLE](../../sql-reference/statements/check-table.md#checking-mergetree-tables) для таблиц семейства `MergeTree`.

Возможные значения:

-   0 — запрос возвращает статус каждого куска данных таблицы.
-   1 — запрос возвращает статус таблицы в целом.

Значение по умолчанию: `0`.

## prefer_column_name_to_alias {#prefer-column-name-to-alias}

Включает или отключает замену названий столбцов на псевдонимы (alias) в выражениях и секциях запросов, см. [Примечания по использованию синонимов](../../sql-reference/syntax.md#syntax-expression_aliases). Включите эту настройку, чтобы синтаксис псевдонимов в ClickHouse был более совместим с большинством других СУБД.

Возможные значения:

- 0 — псевдоним подставляется вместо имени столбца.
- 1 — псевдоним не подставляется вместо имени столбца.

Значение по умолчанию: `0`.

**Пример**

Какие изменения привносит включение и выключение настройки:

Запрос:

```sql
SET prefer_column_name_to_alias = 0;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Результат:

```text
Received exception from server (version 21.5.1):
Code: 184. DB::Exception: Received from localhost:9000. DB::Exception: Aggregate function avg(number) is found inside another aggregate function in query: While processing avg(number) AS number.
```

Запрос:

```sql
SET prefer_column_name_to_alias = 1;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Результат:

```text
┌─number─┬─max(number)─┐
│    4.5 │           9 │
└────────┴─────────────┘
```

## limit {#limit}

Устанавливает максимальное количество строк, возвращаемых запросом. Ограничивает сверху значение, установленное в запросе в секции [LIMIT](../../sql-reference/statements/select/limit.md#limit-clause).

Возможные значения:

-   0 — число строк не ограничено.
-   Положительное целое число.

Значение по умолчанию: `0`.

## offset {#offset}

Устанавливает количество строк, которые необходимо пропустить перед началом возврата строк из запроса. Суммируется со значением, установленным в запросе в секции [OFFSET](../../sql-reference/statements/select/offset.md#offset-fetch).

Возможные значения:

-   0 — строки не пропускаются.
-   Положительное целое число.

Значение по умолчанию: `0`.

**Пример**

Исходная таблица:

``` sql
CREATE TABLE test (i UInt64) ENGINE = MergeTree() ORDER BY i;
INSERT INTO test SELECT number FROM numbers(500);
```

Запрос:

``` sql
SET limit = 5;
SET offset = 7;
SELECT * FROM test LIMIT 10 OFFSET 100;
```

Результат:

``` text
┌───i─┐
│ 107 │
│ 108 │
│ 109 │
└─────┘
```
## http_connection_timeout {#http_connection_timeout}

Тайм-аут для HTTP-соединения (в секундах).

Возможные значения:

-   0 - бесконечный тайм-аут.
-   Любое положительное целое число.

Значение по умолчанию: `1`.

## http_send_timeout {#http_send_timeout}

Тайм-аут для отправки данных через HTTP-интерфейс (в секундах).

Возможные значения:

-   0 - бесконечный тайм-аут.
-   Любое положительное целое число.

Значение по умолчанию: `1800`.

## http_receive_timeout {#http_receive_timeout}

Тайм-аут для получения данных через HTTP-интерфейс (в секундах).

Возможные значения:

-   0 - бесконечный тайм-аут.
-   Любое положительное целое число.

Значение по умолчанию: `1800`.

## optimize_syntax_fuse_functions {#optimize_syntax_fuse_functions}

Позволяет объединить агрегатные функции с одинаковым аргументом. Запрос, содержащий по крайней мере две агрегатные функции: [sum](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum), [count](../../sql-reference/aggregate-functions/reference/count.md#agg_function-count) или [avg](../../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg) с одинаковым аргументом, перезаписывается как [sumCount](../../sql-reference/aggregate-functions/reference/sumcount.md#agg_function-sumCount).

Возможные значения:

-   0 — функции с одинаковым аргументом не объединяются.
-   1 — функции с одинаковым аргументом объединяются.

Значение по умолчанию: `0`.

**Пример**

Запрос:

``` sql
CREATE TABLE fuse_tbl(a Int8, b Int8) Engine = Log;
SET optimize_syntax_fuse_functions = 1;
EXPLAIN SYNTAX SELECT sum(a), sum(b), count(b), avg(b) from fuse_tbl FORMAT TSV;
```

Результат:

``` text
SELECT
    sum(a),
    sumCount(b).1,
    sumCount(b).2,
    (sumCount(b).1) / (sumCount(b).2)
FROM fuse_tbl
```

## allow_experimental_database_replicated {#allow_experimental_database_replicated}

Позволяет создавать базы данных с движком [Replicated](../../engines/database-engines/replicated.md).

Возможные значения:

-   0 — Disabled.
-   1 — Enabled.

Значение по умолчанию: `0`.

## database_replicated_initial_query_timeout_sec {#database_replicated_initial_query_timeout_sec}

Устанавливает, как долго начальный DDL-запрос должен ждать, пока реплицированная база данных прецессирует предыдущие записи очереди DDL в секундах.

Возможные значения:

-   Положительное целое число.
-   0 — Не ограничено.

Значение по умолчанию: `300`.

## distributed_ddl_task_timeout {#distributed_ddl_task_timeout}

Устанавливает тайм-аут для ответов на DDL-запросы от всех хостов в кластере. Если DDL-запрос не был выполнен на всех хостах, ответ будет содержать ошибку тайм-аута, и запрос будет выполнен в асинхронном режиме.

Возможные значения:

-   Положительное целое число.
-   0 — Асинхронный режим.
-   Отрицательное число — бесконечный тайм-аут.

Значение по умолчанию: `180`.

## distributed_ddl_output_mode {#distributed_ddl_output_mode}

Задает формат результата распределенного DDL-запроса.

Возможные значения:

-   `throw` — возвращает набор результатов со статусом выполнения запросов для всех хостов, где завершен запрос. Если запрос не выполнился на некоторых хостах, то будет выброшено исключение. Если запрос еще не закончен на некоторых хостах и таймаут [distributed_ddl_task_timeout](#distributed_ddl_task_timeout) превышен, то выбрасывается исключение `TIMEOUT_EXCEEDED`.
-   `none` — идентично `throw`, но распределенный DDL-запрос не возвращает набор результатов.
-   `null_status_on_timeout` — возвращает `NULL` в качестве статуса выполнения в некоторых строках набора результатов вместо выбрасывания `TIMEOUT_EXCEEDED`, если запрос не закончен на соответствующих хостах.
-   `never_throw` — не выбрасывает исключение и `TIMEOUT_EXCEEDED`, если запрос не удался на некоторых хостах.

Значение по умолчанию: `throw`.

## flatten_nested {#flatten-nested}

Устанавливает формат данных у [вложенных](../../sql-reference/data-types/nested-data-structures/nested.md) столбцов.

Возможные значения:

-   1 — вложенный столбец преобразуется к отдельным массивам.
-   0 — вложенный столбец преобразуется к массиву кортежей.

Значение по умолчанию: `1`.

**Использование**

Если установлено значение `0`, можно использовать любой уровень вложенности.

**Примеры**

Запрос:

``` sql
SET flatten_nested = 1;

CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Результат:

``` text
┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n.a` Array(UInt32),
    `n.b` Array(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Запрос:

``` sql
SET flatten_nested = 0;

CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Результат:

``` text
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n` Nested(a UInt32, b UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## external_table_functions_use_nulls {#external-table-functions-use-nulls}

Определяет, как табличные функции [mysql](../../sql-reference/table-functions/mysql.md), [postgresql](../../sql-reference/table-functions/postgresql.md) и [odbc](../../sql-reference/table-functions/odbc.md)] используют Nullable столбцы.

Возможные значения:

-   0 — табличная функция явно использует Nullable столбцы.
-   1 — табличная функция неявно использует Nullable столбцы.

Значение по умолчанию: `1`.

**Использование**

Если установлено значение `0`, то табличная функция не делает Nullable столбцы, а вместо NULL выставляет значения по умолчанию для скалярного типа. Это также применимо для значений NULL внутри массивов.

## output_format_arrow_low_cardinality_as_dictionary {#output-format-arrow-low-cardinality-as-dictionary}

Позволяет конвертировать тип [LowCardinality](../../sql-reference/data-types/lowcardinality.md) в тип `DICTIONARY` формата [Arrow](../../interfaces/formats.md#data-format-arrow) для запросов `SELECT`.

Возможные значения:

-   0 — тип `LowCardinality` не конвертируется в тип `DICTIONARY`.
-   1 — тип `LowCardinality` конвертируется в тип `DICTIONARY`.

Значение по умолчанию: `0`.

## materialized_postgresql_max_block_size {#materialized-postgresql-max-block-size}

Задает максимальное количество строк, собранных в памяти перед вставкой данных в таблицу базы данных PostgreSQL.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `65536`.

## materialized_postgresql_tables_list {#materialized-postgresql-tables-list}

Задает список таблиц базы данных PostgreSQL, разделенных запятыми, которые будут реплицироваться с помощью движка базы данных [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md).

Значение по умолчанию: пустой список — база данных PostgreSQL будет полностью реплицирована.

## materialized_postgresql_allow_automatic_update {#materialized-postgresql-allow-automatic-update}

Позволяет автоматически обновить таблицу в фоновом режиме при обнаружении изменений схемы. DDL-запросы на стороне сервера PostgreSQL не реплицируются с помощью движка ClickHouse [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md), поскольку это запрещено протоколом логической репликации PostgreSQL, но факт DDL-измененений обнаруживается транзакционно.  После обнаружения DDL по умолчанию прекращается репликация этих таблиц. Однако, если эта настройка включена, то вместо остановки репликации, таблицы будут перезагружены в фоновом режиме с помощью снимка базы данных без потери информации, и репликация для них будет продолжена.

Возможные значения:

-   0 — таблица не обновляется автоматически в фоновом режиме при обнаружении изменений схемы.
-   1 — таблица обновляется автоматически в фоновом режиме при обнаружении изменений схемы.

Значение по умолчанию: `0`.

## materialized_postgresql_replication_slot {#materialized-postgresql-replication-slot}

Строка с идентификатором слота репликации, созданного пользователем вручную. Эта настройка должна использоваться совместно с [materialized_postgresql_snapshot](#materialized-postgresql-snapshot).

## materialized_postgresql_snapshot {#materialized-postgresql-snapshot}

Строка с идентификатором снэпшота, из которого будет выполняться [исходный дамп таблиц PostgreSQL](../../engines/database-engines/materialized-postgresql.md). Эта настройка должна использоваться совместно с [materialized_postgresql_replication_slot](#materialized-postgresql-replication-slot).

## allow_experimental_projection_optimization {#allow-experimental-projection-optimization}

Включает или отключает поддержку [проекций](../../engines/table-engines/mergetree-family/mergetree.md#projections) при обработке запросов `SELECT`.

Возможные значения:

-   0 — Проекции не поддерживаются.
-   1 — Проекции поддерживаются.

Значение по умолчанию: `0`.

## force_optimize_projection {#force-optimize-projection}

Включает или отключает обязательное использование [проекций](../../engines/table-engines/mergetree-family/mergetree.md#projections) в запросах `SELECT`, если поддержка проекций включена (см. настройку [allow_experimental_projection_optimization](#allow-experimental-projection-optimization)).

Возможные значения:

-   0 — Проекции используются опционально.
-   1 — Проекции обязательно используются.

Значение по умолчанию: `0`.

## replication_alter_partitions_sync {#replication-alter-partitions-sync}

Позволяет настроить ожидание выполнения действий на репликах запросами [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) или [TRUNCATE](../../sql-reference/statements/truncate.md).

Возможные значения:

-   0 — не ждать.
-   1 — ждать выполнения действий на своей реплике.
-   2 — ждать выполнения действий на всех репликах.

Значение по умолчанию: `1`.

## replication_wait_for_inactive_replica_timeout {#replication-wait-for-inactive-replica-timeout}

Указывает время ожидания (в секундах) выполнения запросов [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) или [TRUNCATE](../../sql-reference/statements/truncate.md) для неактивных реплик.

Возможные значения:

-   0 — не ждать.
-   Отрицательное целое число — ждать неограниченное время.
-   Положительное целое число — установить соответствующее количество секунд ожидания.

Значение по умолчанию: `120` секунд.

## regexp_max_matches_per_row {#regexp-max-matches-per-row}

Задает максимальное количество совпадений для регулярного выражения. Настройка применяется для защиты памяти от перегрузки при использовании "жадных" квантификаторов в регулярном выражении для функции [extractAllGroupsHorizontal](../../sql-reference/functions/string-search-functions.md#extractallgroups-horizontal).

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `1000`.

## http_max_single_read_retries {#http-max-single-read-retries}

Задает максимальное количество попыток чтения данных во время одного HTTP-запроса.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `1024`.

## log_queries_probability {#log-queries-probability}

Позволяет пользователю записывать в системные таблицы [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md) и [query_views_log](../../operations/system-tables/query_views_log.md) только часть запросов, выбранных случайным образом, с указанной вероятностью. Это помогает снизить нагрузку при большом объеме запросов в секунду.

Возможные значения:

-   0 — запросы не регистрируются в системных таблицах.
-   Положительное число с плавающей точкой в диапазоне [0..1]. Например, при значении настройки, равном `0.5`, примерно половина запросов регистрируется в системных таблицах.
-   1 — все запросы регистрируются в системных таблицах.

Значение по умолчанию: `1`.

## short_circuit_function_evaluation {#short-circuit-function-evaluation}

Позволяет вычислять функции [if](../../sql-reference/functions/conditional-functions.md#if), [multiIf](../../sql-reference/functions/conditional-functions.md#multiif), [and](../../sql-reference/functions/logical-functions.md#logical-and-function) и [or](../../sql-reference/functions/logical-functions.md#logical-or-function) по [короткой схеме](https://ru-wikipedia-org.turbopages.org/ru.wikipedia.org/s/wiki/Вычисления_по_короткой_схеме). Это помогает оптимизировать выполнение сложных выражений в этих функциях и предотвратить возможные исключения (например, деление на ноль, когда оно не ожидается).

Возможные значения:

-   `enable` — по короткой схеме вычисляются функции, которые подходят для этого (могут сгенерировать исключение или требуют сложных вычислений).
-   `force_enable` — все функции вычисляются по короткой схеме.
-   `disable` — вычисление функций по короткой схеме отключено.

Значение по умолчанию: `enable`.

## max_hyperscan_regexp_length {#max-hyperscan-regexp-length}

Задает максимальную длину каждого регулярного выражения в [hyperscan-функциях](../../sql-reference/functions/string-search-functions.md#multimatchanyhaystack-pattern1-pattern2-patternn)  поиска множественных совпадений в строке.

Возможные значения:

-   Положительное целое число.
-   0 - длина не ограничена.

Значение по умолчанию: `0`.

**Пример**

Запрос:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 3;
```

Результат:

```text
┌─multiMatchAny('abcd', ['ab', 'bcd', 'c', 'd'])─┐
│                                              1 │
└────────────────────────────────────────────────┘
```

Запрос:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 2;
```

Результат:

```text
Exception: Regexp length too large.
```

**См. также**

-   [max_hyperscan_regexp_total_length](#max-hyperscan-regexp-total-length)

## max_hyperscan_regexp_total_length {#max-hyperscan-regexp-total-length}

Задает максимальную общую длину всех регулярных выражений в каждой [hyperscan-функции](../../sql-reference/functions/string-search-functions.md#multimatchanyhaystack-pattern1-pattern2-patternn)  поиска множественных совпадений в строке.

Возможные значения:

-   Положительное целое число.
-   0 - длина не ограничена.

Значение по умолчанию: `0`.

**Пример**

Запрос:

```sql
SELECT multiMatchAny('abcd', ['a','b','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Результат:

```text
┌─multiMatchAny('abcd', ['a', 'b', 'c', 'd'])─┐
│                                           1 │
└─────────────────────────────────────────────┘
```

Запрос:

```sql
SELECT multiMatchAny('abcd', ['ab','bc','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Результат:

```text
Exception: Total regexp lengths too large.
```

**См. также**

-   [max_hyperscan_regexp_length](#max-hyperscan-regexp-length)

## enable_positional_arguments {#enable-positional-arguments}

Включает и отключает поддержку позиционных аргументов для [GROUP BY](../../sql-reference/statements/select/group-by.md), [LIMIT BY](../../sql-reference/statements/select/limit-by.md), [ORDER BY](../../sql-reference/statements/select/order-by.md).

Возможные значения:

-   0 — Позиционные аргументы не поддерживаются.
-   1 — Позиционные аргументы поддерживаются: можно использовать номера столбцов вместо названий столбцов.

Значение по умолчанию: `1`.

**Пример**

Запрос:

```sql
CREATE TABLE positional_arguments(one Int, two Int, three Int) ENGINE=Memory();

INSERT INTO positional_arguments VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20);

SELECT * FROM positional_arguments ORDER BY 2,3;
```

Результат:

```text
┌─one─┬─two─┬─three─┐
│  30 │  10 │   20  │
│  20 │  20 │   10  │
│  10 │  20 │   30  │
└─────┴─────┴───────┘
```

## optimize_move_to_prewhere {#optimize_move_to_prewhere}

Включает или отключает автоматическую оптимизацию [PREWHERE](../../sql-reference/statements/select/prewhere.md) в запросах [SELECT](../../sql-reference/statements/select/index.md).

Работает только с таблицами семейства [*MergeTree](../../engines/table-engines/mergetree-family/index.md).

Возможные значения:

-   0 — автоматическая оптимизация `PREWHERE` отключена.
-   1 — автоматическая оптимизация `PREWHERE` включена.

Значение по умолчанию: `1`.

## optimize_move_to_prewhere_if_final {#optimize_move_to_prewhere_if_final}

Включает или отключает автоматическую оптимизацию [PREWHERE](../../sql-reference/statements/select/prewhere.md) в запросах [SELECT](../../sql-reference/statements/select/index.md) с модификатором [FINAL](../../sql-reference/statements/select/from.md#select-from-final).

Работает только с таблицами семейства [*MergeTree](../../engines/table-engines/mergetree-family/index.md).

Возможные значения:

-   0 — автоматическая оптимизация `PREWHERE` в запросах `SELECT` с модификатором `FINAL` отключена.
-   1 — автоматическая оптимизация `PREWHERE` в запросах `SELECT` с модификатором `FINAL` включена.

Значение по умолчанию: `0`.

**См. также**

-   настройка [optimize_move_to_prewhere](#optimize_move_to_prewhere)

## describe_include_subcolumns {#describe_include_subcolumns}

Включает или отключает описание подстолбцов при выполнении запроса [DESCRIBE](../../sql-reference/statements/describe-table.md). Настройка действует, например, на элементы [Tuple](../../sql-reference/data-types/tuple.md) или подстолбцы типов [Map](../../sql-reference/data-types/map.md#map-subcolumns), [Nullable](../../sql-reference/data-types/nullable.md#finding-null) или [Array](../../sql-reference/data-types/array.md#array-size).

Возможные значения:

-   0 — подстолбцы не включаются в результат запросов `DESCRIBE`.
-   1 — подстолбцы включаются в результат запросов `DESCRIBE`.

Значение по умолчанию: `0`.

**Пример**

Смотрите пример запроса [DESCRIBE](../../sql-reference/statements/describe-table.md).

## async_insert {#async-insert}

Включает или отключает асинхронные вставки. Работает только для вставок по протоколу HTTP. Обратите внимание, что при таких вставках дедупликация не производится.

Если включено, данные собираются в пачки перед вставкой в таблицу. Это позволяет производить мелкие и частые вставки в ClickHouse (до 15000 запросов в секунду) без промежуточных таблиц.

Вставка данных происходит либо как только объем вставляемых данных превышает [async_insert_max_data_size](#async-insert-max-data-size), либо через [async_insert_busy_timeout_ms](#async-insert-busy-timeout-ms) миллисекунд после первого запроса `INSERT`. Если в [async_insert_stale_timeout_ms](#async-insert-stale-timeout-ms) задано ненулевое значение, то данные вставляются через `async_insert_stale_timeout_ms` миллисекунд после последнего запроса.

Если включен параметр [wait_for_async_insert](#wait-for-async-insert), каждый клиент ждет, пока данные будут сброшены в таблицу. Иначе запрос будет обработан почти моментально, даже если данные еще не вставлены.

Возможные значения:

-   0 — вставки производятся синхронно, один запрос за другим.
-   1 — включены множественные асинхронные вставки.

Значение по умолчанию: `0`.

## async_insert_threads {#async-insert-threads}

Максимальное число потоков для фоновой обработки и вставки данных.

Возможные значения:

-   Положительное целое число.
-   0 — асинхронные вставки отключены.

Значение по умолчанию: `16`.

## wait_for_async_insert {#wait-for-async-insert}

Включает или отключает ожидание обработки асинхронных вставок. Если включено, клиент выведет `OK` только после того, как данные вставлены. Иначе будет выведен `OK`, даже если вставка не произошла.

Возможные значения:

-   0 — сервер возвращает `OK` даже если вставка данных еще не завершена.
-   1 — сервер возвращает `OK` только после завершения вставки данных.

Значение по умолчанию: `1`.

## wait_for_async_insert_timeout {#wait-for-async-insert-timeout}

Время ожидания в секундах, выделяемое для обработки асинхронной вставки.

Возможные значения:

-   Положительное целое число.
-   0 — ожидание отключено.

Значение по умолчанию: [lock_acquire_timeout](#lock_acquire_timeout).

## async_insert_max_data_size {#async-insert-max-data-size}

Максимальный размер необработанных данных (в байтах), собранных за запрос, перед их вставкой.

Возможные значения:

-   Положительное целое число.
-   0 — асинхронные вставки отключены.

Значение по умолчанию: `1000000`.

## async_insert_busy_timeout_ms {#async-insert-busy-timeout-ms}

Максимальное время ожидания в миллисекундах после первого запроса `INSERT` и перед вставкой данных.

Возможные значения:

-   Положительное целое число.
-   0 — ожидание отключено.

Значение по умолчанию: `200`.

## async_insert_stale_timeout_ms {#async-insert-stale-timeout-ms}

Максимальное время ожидания в миллисекундах после последнего запроса `INSERT` и перед вставкой данных. Если установлено ненулевое значение, [async_insert_busy_timeout_ms](#async-insert-busy-timeout-ms) будет продлеваться с каждым запросом `INSERT`, пока не будет превышен [async_insert_max_data_size](#async-insert-max-data-size).

Возможные значения:

-   Положительное целое число.
-   0 — ожидание отключено.

Значение по умолчанию: `0`.

## alter_partition_verbose_result {#alter-partition-verbose-result}

Включает или отключает вывод информации о кусках, к которым были успешно применены операции манипуляции с партициями и кусками. Применимо к [ATTACH PARTITION|PART](../../sql-reference/statements/alter/partition.md#alter_attach-partition) и к [FREEZE PARTITION](../../sql-reference/statements/alter/partition.md#alter_freeze-partition)

Возможные значения:

-   0 — отображение отключено.
-   1 — отображение включено.

Значение по умолчанию: `0`.

**Пример**

```sql
CREATE TABLE test(a Int64, d Date, s String) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY a;
INSERT INTO test VALUES(1, '2021-01-01', '');
INSERT INTO test VALUES(1, '2021-01-01', '');
ALTER TABLE test DETACH PARTITION ID '202101';

ALTER TABLE test ATTACH PARTITION ID '202101' SETTINGS alter_partition_verbose_result = 1;

┌─command_type─────┬─partition_id─┬─part_name────┬─old_part_name─┐
│ ATTACH PARTITION │ 202101       │ 202101_7_7_0 │ 202101_5_5_0  │
│ ATTACH PARTITION │ 202101       │ 202101_8_8_0 │ 202101_6_6_0  │
└──────────────────┴──────────────┴──────────────┴───────────────┘

ALTER TABLE test FREEZE SETTINGS alter_partition_verbose_result = 1;

┌─command_type─┬─partition_id─┬─part_name────┬─backup_name─┬─backup_path───────────────────┬─part_backup_path────────────────────────────────────────────┐
│ FREEZE ALL   │ 202101       │ 202101_7_7_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_7_7_0 │
│ FREEZE ALL   │ 202101       │ 202101_8_8_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_8_8_0 │
└──────────────┴──────────────┴──────────────┴─────────────┴───────────────────────────────┴─────────────────────────────────────────────────────────────┘
```

## format_capn_proto_enum_comparising_mode {#format-capn-proto-enum-comparising-mode}

Определяет, как сопоставить тип данных ClickHouse `Enum` и тип данных `Enum` формата [CapnProto](../../interfaces/formats.md#capnproto) из схемы.

Возможные значения:

-   `'by_values'` — значения в перечислениях должны быть одинаковыми, а имена могут быть разными.
-   `'by_names'` — имена в перечислениях должны быть одинаковыми, а значения могут быть разными.
-   `'by_name_case_insensitive'` — имена в перечислениях должны быть одинаковыми без учета регистра, а значения могут быть разными.

Значение по умолчанию: `'by_values'`.

## min_bytes_to_use_mmap_io {#min-bytes-to-use-mmap-io}

Это экспериментальная настройка. Устанавливает минимальный объем памяти для чтения больших файлов без копирования данных из ядра в пространство пользователей. Рекомендуемый лимит составляет около 64 MB, поскольку [mmap/munmap](https://en.wikipedia.org/wiki/Mmap) работает медленно. Это имеет смысл только для больших файлов и помогает только в том случае, если данные находятся в кеше страниц.

Возможные значения:

-   Положительное целое число.
-   0 — большие файлы считываются только с копированием данных из ядра в пространство пользователей.

Значение по умолчанию: `0`.

## format_custom_escaping_rule {#format-custom-escaping-rule}

Устанавливает правило экранирования данных формата [CustomSeparated](../../interfaces/formats.md#format-customseparated).

Возможные значения:

-   `'Escaped'` — как в формате [TSV](../../interfaces/formats.md#tabseparated).
-   `'Quoted'` — как в формате [Values](../../interfaces/formats.md#data-format-values).
-   `'CSV'` — как в формате [CSV](../../interfaces/formats.md#csv).
-   `'JSON'` — как в формате [JSONEachRow](../../interfaces/formats.md#jsoneachrow).
-   `'XML'` — как в формате [XML](../../interfaces/formats.md#xml).
-   `'Raw'` — данные импортируются как есть, без экранирования, как в формате [TSVRaw](../../interfaces/formats.md#tabseparatedraw).

Значение по умолчанию: `'Escaped'`.

## format_custom_field_delimiter {#format-custom-field-delimiter}

Задает символ, который интерпретируется как разделитель между полями данных формата [CustomSeparated](../../interfaces/formats.md#format-customseparated).

Значение по умолчанию: `'\t'`.

## format_custom_row_before_delimiter {#format-custom-row-before-delimiter}

Задает символ, который интерпретируется как разделитель перед полем первого столбца данных формата [CustomSeparated](../../interfaces/formats.md#format-customseparated).

Значение по умолчанию: `''`.

## format_custom_row_after_delimiter {#format-custom-row-after-delimiter}

Задает символ, который интерпретируется как разделитель после поля последнего столбца данных формата [CustomSeparated](../../interfaces/formats.md#format-customseparated).

Значение по умолчанию: `'\n'`.

## format_custom_row_between_delimiter {#format-custom-row-between-delimiter}

Задает символ, который интерпретируется как разделитель между строками данных формата [CustomSeparated](../../interfaces/formats.md#format-customseparated).

Значение по умолчанию: `''`.

## format_custom_result_before_delimiter {#format-custom-result-before-delimiter}

Задает символ, который интерпретируется как префикс перед результирующим набором данных формата [CustomSeparated](../../interfaces/formats.md#format-customseparated).

Значение по умолчанию: `''`.

## format_custom_result_after_delimiter {#format-custom-result-after-delimiter}

Задает символ, который интерпретируется как суффикс после результирующего набора данных формата [CustomSeparated](../../interfaces/formats.md#format-customseparated).

Значение по умолчанию: `''`.
