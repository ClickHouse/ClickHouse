---
alias: []
description: 'Документация по формату Arrow'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

[Apache Arrow](https://arrow.apache.org/) включает два встроенных формата столбцового хранения.
ClickHouse поддерживает чтение и запись в этих форматах.
`Arrow` — это формат Apache Arrow в «файловом режиме», предназначенный для произвольного доступа к данным в памяти.

<div id="data-types-matching">
  ## Соответствие типов данных
</div>

В таблице ниже показаны поддерживаемые типы данных и то, как они сопоставляются с [типами данных](/ru/sql-reference/data-types/index.md) ClickHouse в запросах `INSERT` и `SELECT`.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                             | Arrow data type (`SELECT`) |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `BOOL`                                  | [Bool](/ru/sql-reference/data-types/boolean.md)                                                                     | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/ru/sql-reference/data-types/int-uint.md)                                                                   | `UINT8`                    |
| `INT8`                                  | [Int8](/ru/sql-reference/data-types/int-uint.md)/[Enum8](/ru/sql-reference/data-types/enum.md)                         | `INT8`                     |
| `UINT16`                                | [UInt16](/ru/sql-reference/data-types/int-uint.md)                                                                  | `UINT16`                   |
| `INT16`                                 | [Int16](/ru/sql-reference/data-types/int-uint.md)/[Enum16](/ru/sql-reference/data-types/enum.md)                       | `INT16`                    |
| `UINT32`                                | [UInt32](/ru/sql-reference/data-types/int-uint.md)                                                                  | `UINT32`                   |
| `INT32`                                 | [Int32](/ru/sql-reference/data-types/int-uint.md)                                                                   | `INT32`                    |
| `UINT64`                                | [UInt64](/ru/sql-reference/data-types/int-uint.md)                                                                  | `UINT64`                   |
| `INT64`                                 | [Int64](/ru/sql-reference/data-types/int-uint.md)                                                                   | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/ru/sql-reference/data-types/float.md)                                                                    | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/ru/sql-reference/data-types/float.md)                                                                    | `FLOAT64`                  |
| `DATE32`                                | [Date32](/ru/sql-reference/data-types/date32.md)                                                                    | `UINT16`                   |
| `DATE64`                                | [DateTime](/ru/sql-reference/data-types/datetime.md)                                                                | `UINT32`                   |
| `TIMESTAMP`                             | [DateTime64](/ru/sql-reference/data-types/datetime64.md)                                                            | `TIMESTAMP`                |
| `TIME32`, `TIME64`                      | [Time64](/ru/sql-reference/data-types/time64.md)                                                                    | `TIME32`, `TIME64`         |
| `STRING`, `BINARY`                      | [String](/ru/sql-reference/data-types/string.md)                                                                    | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/ru/sql-reference/data-types/fixedstring.md)                                                          | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/ru/sql-reference/data-types/decimal.md)                                                                  | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/ru/sql-reference/data-types/decimal.md)                                                               | `DECIMAL256`               |
| `LIST`                                  | [Array](/ru/sql-reference/data-types/array.md)                                                                      | `LIST`                     |
| `STRUCT`                                | [Tuple](/ru/sql-reference/data-types/tuple.md)                                                                      | `STRUCT`                   |
| `MAP`                                   | [Map](/ru/sql-reference/data-types/map.md)                                                                          | `MAP`                      |
| `UINT32`                                | [IPv4](/ru/sql-reference/data-types/ipv4.md)                                                                        | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/ru/sql-reference/data-types/ipv6.md)                                                                        | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/ru/sql-reference/data-types/int-uint.md)                                           | `FIXED_SIZE_BINARY`        |
| `DURATION`                              | [Interval](/ru/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`                 |
| `INT64`                                 | [Interval](/ru/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year)   | `INT64`                    |

Массивы могут быть вложенными и в качестве аргумента могут принимать значение типа `Nullable`. Типы `Tuple` и `Map` также могут быть вложенными.

Тип `DICTIONARY` поддерживается для запросов `INSERT`, а для запросов `SELECT` есть настройка [`output_format_arrow_low_cardinality_as_dictionary`](/ru/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary), которая позволяет выводить тип [LowCardinality](/ru/sql-reference/data-types/lowcardinality.md) как тип `DICTIONARY`. Обратите внимание, что в словаре `LowCardinality` могут быть неиспользуемые значения, из-за чего при выводе в Arrow `DICTIONARY` также могут появляться неиспользуемые значения.

Неподдерживаемые типы данных Arrow:

* `FIXED_SIZE_BINARY`
* `JSON`
* `UUID`
* `ENUM`.

Типы данных столбцов таблицы ClickHouse не обязательно должны совпадать с соответствующими полями данных Arrow. При вставке данных ClickHouse интерпретирует типы данных в соответствии с таблицей выше, а затем [преобразует](/ru/sql-reference/functions/type-conversion-functions#CAST) данные к типу данных, заданному для столбца таблицы ClickHouse.

<div id="example-usage">
  ## Пример использования
</div>

В примере ниже используется набор данных `forex`, доступный в
[Песочнице ClickHouse](https://sql.clickhouse.com).

<div id="selecting-data">
  ### Выборка данных
</div>

Мы выбираем данные за один день по курсу `EUR/USD` в Песочнице ClickHouse и сохраняем их
в локальный файл `forex_eurusd.arrow`. Мы отправляем запрос к песочнице через HTTP-
интерфейс, где хост — `sql-clickhouse.clickhouse.com`, а пользователь —
`demo` (без пароля):

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
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

<div id="reading-data">
  ### Чтение файла
</div>

Теперь мы можем прочитать локальный файл Arrow с помощью
[`clickhouse-local`](/ru/operations/utilities/clickhouse-local), используя
табличную функцию [`file`](/ru/sql-reference/table-functions/file). Файл
самоописывающийся, поэтому формат `Arrow` автоматически определяет схему:

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

<div id="inserting-data">
  ### Вставка данных
</div>

Чтобы загрузить файл Arrow в таблицу ClickHouse, направьте его в `clickhouse-client`
с `FORMAT Arrow`:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                                    | Описание                                                                                                | По умолчанию |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------ |
| `input_format_arrow_allow_missing_columns`                                   | Разрешить отсутствие столбцов при чтении входных форматов Arrow                                         | `1`          |
| `input_format_arrow_case_insensitive_column_matching`                        | Игнорировать регистр при сопоставлении столбцов Arrow со столбцами ClickHouse.                          | `0`          |
| `input_format_arrow_import_nested`                                           | Устаревшая настройка, не имеет эффекта.                                                                 | `0`          |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Пропускать столбцы с неподдерживаемыми типами при определении схемы для формата Arrow                   | `0`          |
| `output_format_arrow_compression_method`                                     | Метод сжатия для выходного формата Arrow. Поддерживаемые кодеки: lz4&#95;frame, zstd, none (без сжатия) | `lz4_frame`  |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Использовать тип Arrow FIXED&#95;SIZE&#95;BINARY вместо Binary для столбцов FixedString.                | `1`          |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Выводить тип LowCardinality как тип Arrow Dictionary                                                    | `0`          |
| `output_format_arrow_string_as_string`                                       | Использовать тип Arrow String вместо Binary для строковых столбцов                                      | `1`          |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Всегда использовать 64-битные целые числа для индексов словаря в формате Arrow                          | `0`          |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Использовать целые числа со знаком для индексов словаря в формате Arrow                                 | `1`          |