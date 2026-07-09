---
alias: []
description: 'Документация по формату BSONEachRow'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: 'Справочник'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `BSONEachRow` разбирает данные как последовательность документов Binary JSON (BSON) без каких-либо разделителей между ними.
Каждая строка представляется отдельным документом, а каждый столбец — отдельным полем BSON-документа, где ключом служит имя столбца.

<div id="data-types-matching">
  ## Соответствие типов данных
</div>

Для вывода используется следующее соответствие между типами ClickHouse и типами BSON:

| Тип ClickHouse                                                                                        | Тип BSON                                                                                                                                |
| ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| [Bool](/ru/sql-reference/data-types/boolean.md)                                                          | `\x08` boolean                                                                                                                          |
| [Int8/UInt8](/ru/sql-reference/data-types/int-uint.md)/[Enum8](/ru/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                                            |
| [Int16/UInt16](/ru/sql-reference/data-types/int-uint.md)/[Enum16](/ru/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                                                            |
| [Int32](/ru/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                                                            |
| [UInt32](/ru/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                                                            |
| [Int64/UInt64](/ru/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                                                            |
| [Float32/Float64](/ru/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                                                           |
| [Date](/ru/sql-reference/data-types/date.md)/[Date32](/ru/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                                            |
| [DateTime](/ru/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                                                            |
| [DateTime64](/ru/sql-reference/data-types/datetime64.md)                                                 | `\x09` datetime                                                                                                                         |
| [Decimal32](/ru/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                                                            |
| [Decimal64](/ru/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                                                            |
| [Decimal128](/ru/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` binary subtype, size = 16                                                                                         |
| [Decimal256](/ru/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` binary subtype, size = 32                                                                                         |
| [Int128/UInt128](/ru/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` binary subtype, size = 16                                                                                         |
| [Int256/UInt256](/ru/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` binary subtype, size = 32                                                                                         |
| [String](/ru/sql-reference/data-types/string.md)/[FixedString](/ru/sql-reference/data-types/fixedstring.md) | `\x05` binary, `\x00` binary subtype или `\x02` string, если включена настройка output&#95;format&#95;bson&#95;string&#95;as&#95;string |
| [UUID](/ru/sql-reference/data-types/uuid.md)                                                             | `\x05` binary, `\x04` uuid subtype, size = 16                                                                                           |
| [Array](/ru/sql-reference/data-types/array.md)                                                           | `\x04` массив                                                                                                                            |
| [Tuple](/ru/sql-reference/data-types/tuple.md)                                                           | `\x04` массив                                                                                                                            |
| [Named Tuple](/ru/sql-reference/data-types/tuple.md)                                                     | `\x03` документ                                                                                                                         |
| [Map](/ru/sql-reference/data-types/map.md)                                                               | `\x03` документ                                                                                                                         |
| [IPv4](/ru/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                                                            |
| [IPv6](/ru/sql-reference/data-types/ipv6.md)                                                             | `\x05` binary, `\x00` binary subtype                                                                                                    |

Для ввода используется следующее соответствие между типами BSON и типами ClickHouse:

| Тип BSON                                 | Тип ClickHouse                                                                                                                                                                                      |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` double                            | [Float32/Float64](/ru/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` string                            | [String](/ru/sql-reference/data-types/string.md)/[FixedString](/ru/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` документ                          | [Map](/ru/sql-reference/data-types/map.md)/[Named Tuple](/ru/sql-reference/data-types/tuple.md)                                                                                                           |
| `\x04` массив                            | [Array](/ru/sql-reference/data-types/array.md)/[Tuple](/ru/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` binary, `\x00` binary subtype     | [String](/ru/sql-reference/data-types/string.md)/[FixedString](/ru/sql-reference/data-types/fixedstring.md)/[IPv6](/ru/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` binary, `\x02` old binary subtype | [String](/ru/sql-reference/data-types/string.md)/[FixedString](/ru/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` binary, `\x03` old uuid subtype   | [UUID](/ru/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` binary, `\x04` uuid subtype       | [UUID](/ru/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId                          | [String](/ru/sql-reference/data-types/string.md)/[FixedString](/ru/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` boolean                           | [Bool](/ru/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` datetime                          | [DateTime64](/ru/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` значение NULL                     | [NULL](/ru/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` код JavaScript                    | [String](/ru/sql-reference/data-types/string.md)/[FixedString](/ru/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` символ                            | [String](/ru/sql-reference/data-types/string.md)/[FixedString](/ru/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                             | [Int32/UInt32](/ru/sql-reference/data-types/int-uint.md)/[Decimal32](/ru/sql-reference/data-types/decimal.md)/[IPv4](/ru/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/ru/sql-reference/data-types/enum.md) |
| `\x12` int64                             | [Int64/UInt64](/ru/sql-reference/data-types/int-uint.md)/[Decimal64](/ru/sql-reference/data-types/decimal.md)/[DateTime64](/ru/sql-reference/data-types/datetime64.md)                                       |

Остальные типы BSON не поддерживаются. Кроме того, формат выполняет преобразование между различными целочисленными типами.
Например, значение BSON `int32` можно вставить в ClickHouse как [`UInt8`](../../sql-reference/data-types/int-uint.md).

Большие целые числа и десятичные значения, такие как `Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256`, можно считывать из BSON-значения Binary с binary subtype `\x00`.
В этом случае формат проверяет, что размер бинарных данных соответствует размеру ожидаемого значения.

:::note
Этот формат некорректно работает на платформах с порядком байтов Big-Endian.
:::

<div id="example-usage">
  ## Пример использования
</div>

<div id="inserting-data">
  ### Вставка данных
</div>

Используя BSON-файл `football.bson` со следующими данными:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Вставьте данные:

```sql
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### Чтение данных
</div>

Читайте данные в формате `BSONEachRow`:

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON — это двоичный формат, который не отображается в терминале в удобочитаемом виде. Используйте предложение `INTO OUTFILE` для вывода BSON-файлов.
:::

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                                                                                                                                                             | Описание                                                                                                            | По умолчанию |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------ |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | Использовать тип BSON String вместо Binary для столбцов типа String.                                                | `false`      |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | Разрешить пропуск столбцов с неподдерживаемыми типами при автоматическом определении схемы для формата BSONEachRow. | `false`      |