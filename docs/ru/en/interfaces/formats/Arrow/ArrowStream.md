---
alias: []
description: 'Документация по формату ArrowStream'
input_format: true
keywords: ['ArrowStream']
output_format: true
slug: /interfaces/formats/ArrowStream
title: 'ArrowStream'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

`ArrowStream` — это формат Apache Arrow в «потоковом режиме». Он предназначен для потоковой обработки данных в памяти.

<div id="example-usage">
  ## Пример использования
</div>

В примере ниже используется набор данных `forex`, доступный в
[Песочнице ClickHouse](https://sql.clickhouse.com). К ней можно подключиться
удалённо с помощью `clickhouse-client`, используя хост `sql-clickhouse.clickhouse.com`
и пользователя `demo` (без пароля). Таблица `forex` находится в
базе данных `forex`, поэтому мы выбираем её в качестве базы данных по умолчанию:

```bash
clickhouse-client --secure --host sql-clickhouse.clickhouse.com --user demo --database forex
```

Таблица `forex` хранит курсы валют. Мы можем проверить её размер и
степень сжатия на диске, выполнив запрос к [`system.columns`](/ru/operations/system-tables/columns):

```sql title="Query"
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) AS compression_ratio
FROM system.columns
WHERE (database = 'forex') AND (table = 'forex')
GROUP BY table
ORDER BY table ASC
```

```response title="Response"
   ┌─table─┬─compressed_size─┬─uncompressed_size─┬───compression_ratio─┐
1. │ forex │ 63.69 GiB       │ 280.48 GiB        │ 0.22708227109363446 │
   └───────┴─────────────────┴───────────────────┴─────────────────────┘
```

В отличие от формата [`Arrow`](/ru/interfaces/formats/Arrow) в режиме &quot;file mode&quot;, который
требует получить весь результат целиком, прежде чем его можно будет прочитать, `ArrowStream` передаётся как
последовательность батчей записей, которые получатель может читать инкрементально по мере их
поступления. Благодаря этому он хорошо подходит для стриминга результата запроса напрямую в
средство визуализации или аналитический инструмент без предварительной материализации всего набора данных.

Чтобы передавать результат потоком, отправьте запрос через HTTP-интерфейс ClickHouse с помощью
запроса `POST` и считывайте ответ как Arrow stream. Мы отключаем сжатие
вывода Arrow с помощью настройки
[`output_format_arrow_compression_method`](/ru/operations/settings/formats#output_format_arrow_compression_method),
чтобы получатели могли декодировать батчи сразу по мере их получения.

Вывод `ArrowStream` представляет собой необработанные двоичные данные, поэтому вместо вывода его в
терминал мы передаём его в получатель. Поток является самоописывающимся (он содержит
собственную схему), поэтому здесь мы передаём его напрямую в
[`clickhouse-local`](/ru/operations/utilities/clickhouse-local), который считывает
входящие батчи с помощью `--input-format ArrowStream` и выполняет к ним запросы как к таблице.
Таблица `forex` большая, поэтому мы ограничиваем удалённый запрос с помощью предиката `WHERE`
и `LIMIT`, чтобы этот пример оставался небольшим:

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'USD' AND quote = 'CHF'
        ORDER BY datetime ASC
        LIMIT 5
        FORMAT ArrowStream
        SETTINGS output_format_arrow_compression_method='none'" \
  | clickhouse-local --input-format ArrowStream \
      --query "SELECT * FROM table ORDER BY last_update ASC FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬────bid─┬────ask─┬────────────────spread─┐
1. │ USD.CHF    │ 2000-05-30 17:23:44.000 │  1.688 │ 1.6885 │ 0.0005000829696655273 │
2. │ USD.CHF    │ 2000-05-30 17:23:46.000 │ 1.6885 │  1.689 │ 0.0004999637603759766 │
3. │ USD.CHF    │ 2000-05-30 17:23:48.000 │ 1.6886 │ 1.6891 │ 0.0005000829696655273 │
4. │ USD.CHF    │ 2000-05-30 17:23:49.000 │ 1.6888 │ 1.6893 │ 0.0004999637603759766 │
5. │ USD.CHF    │ 2000-05-30 17:24:45.000 │  1.689 │ 1.6895 │ 0.0004999637603759766 │
   └────────────┴─────────────────────────┴────────┴────────┴───────────────────────┘
```

Один и тот же поток может поэтапно считываться любым клиентом с поддержкой Arrow, который
читает его батч за батчем, не буферизуя результат целиком. Например,
с помощью [библиотеки Apache Arrow для JavaScript](https://arrow.apache.org/docs/js/) `RecordBatchReader`
выдаёт каждый батч записей сразу после того, как он поступает с
сервера:

```js
const reader = await RecordBatchReader.from(response);
await reader.open();
for await (const recordBatch of reader) {
    const batchTable = new Table(recordBatch);
    const ipcStream = tableToIPC(batchTable, 'stream');
    const bytes = new Uint8Array(ipcStream);
    table.update(bytes);
}
```

Подробное пошаговое руководство по стримингу данных `ArrowStream` из ClickHouse в
визуализацию в реальном времени с помощью [Perspective](https://perspective.finos.org/) см. в
блоге:
[Потоковые визуализации в реальном времени с ClickHouse, Apache Arrow и Perspective](https://clickhouse.com/blog/streaming-real-time-visualizations-clickhouse-apache-arrow-perpsective).

<div id="format-settings">
  ## Настройки формата
</div>

`ArrowStream` использует те же настройки формата, что и [`Arrow`](/ru/interfaces/formats/Arrow).

| Setting                                                                      | Description                                                                                                                                                                                                                                                                | Default     |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | Разрешить отсутствие столбцов при чтении входных форматов Arrow                                                                                                                                                                                                            | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Игнорировать регистр при сопоставлении столбцов Arrow со столбцами ClickHouse                                                                                                                                                                                              | `0`         |
| `input_format_arrow_import_nested`                                           | Устаревшая настройка, не влияет ни на что.                                                                                                                                                                                                                                 | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Пропускать столбцы с неподдерживаемыми типами при определении схемы для формата Arrow                                                                                                                                                                                      | `0`         |
| `input_format_arrow_use_native_reader`                                       | Использовать встроенный модуль чтения ClickHouse для форматов `Arrow` и `ArrowStream` вместо библиотеки Apache Arrow. Установите `0`, чтобы использовать модуль чтения библиотеки Apache Arrow.                                                                            | `1`         |
| `output_format_arrow_compression_method`                                     | Метод сжатия для выходного формата Arrow. Поддерживаемые кодеки: lz4&#95;frame, zstd, none (без сжатия)                                                                                                                                                                    | `lz4_frame` |
| `output_format_arrow_date_as_uint16`                                         | Записывать значения Date как обычные 16-битные числа (при обратном чтении — как UInt16) вместо преобразования в 32-битный тип Arrow DATE32 (при обратном чтении — как Date32).                                                                                             | `0`         |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Использовать тип Arrow FIXED&#95;SIZE&#95;BINARY вместо Binary для столбцов FixedString                                                                                                                                                                                    | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Включить вывод типа LowCardinality как типа Arrow Dictionary                                                                                                                                                                                                               | `0`         |
| `output_format_arrow_string_as_string`                                       | Использовать тип Arrow String вместо Binary для столбцов String                                                                                                                                                                                                            | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | Выводить тип, для которого нет эквивалента в Arrow (например, `BFloat16`, `AggregateFunction`), как необработанные бинарные данные. Если false, такой тип вызывает исключение. Применяется как к встроенному модулю записи, так и к модулю записи библиотеки Apache Arrow. | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Всегда использовать 64-битные целые числа для индексов словаря в формате Arrow                                                                                                                                                                                             | `0`         |
| `output_format_arrow_use_native_writer`                                      | Использовать встроенный модуль записи ClickHouse для форматов `Arrow` и `ArrowStream` вместо библиотеки Apache Arrow. Установите `0`, чтобы использовать модуль записи библиотеки Apache Arrow.                                                                            | `1`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Использовать знаковые целые числа для индексов словаря в формате Arrow                                                                                                                                                                                                     | `1`         |