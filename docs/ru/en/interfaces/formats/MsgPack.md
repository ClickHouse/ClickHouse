---
alias: []
description: 'Документация по формату MsgPack'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'Справочник'
---

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

ClickHouse поддерживает чтение и запись файлов данных в формате [MessagePack](https://msgpack.org/).

<div id="data-types-matching">
  ## Сопоставление типов данных
</div>

| Тип данных MessagePack (`INSERT`)                                  | Тип данных ClickHouse                                                                       | Тип данных MessagePack (`SELECT`)  |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | ---------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/ru/sql-reference/data-types/int-uint.md)                                            | `uint N`                           |
| `int N`, `negative fixint`                                         | [`IntN`](/ru/sql-reference/data-types/int-uint.md)                                             | `int N`                            |
| `bool`                                                             | [`UInt8`](/ru/sql-reference/data-types/int-uint.md)                                            | `uint 8`                           |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/ru/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`        |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/ru/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`        |
| `float 32`                                                         | [`Float32`](/ru/sql-reference/data-types/float.md)                                             | `float 32`                         |
| `float 64`                                                         | [`Float64`](/ru/sql-reference/data-types/float.md)                                             | `float 64`                         |
| `uint 16`                                                          | [`Date`](/ru/sql-reference/data-types/date.md)                                                 | `uint 16`                          |
| `int 32`                                                           | [`Date32`](/ru/sql-reference/data-types/date32.md)                                             | `int 32`                           |
| `uint 32`                                                          | [`DateTime`](/ru/sql-reference/data-types/datetime.md)                                         | `uint 32`                          |
| `uint 64`                                                          | [`DateTime64`](/ru/sql-reference/data-types/datetime.md)                                       | `uint 64`                          |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/ru/sql-reference/data-types/array.md)/[`Tuple`](/ru/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/ru/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`       |
| `uint 32`                                                          | [`IPv4`](/ru/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                          |
| `bin 8`                                                            | [`String`](/ru/sql-reference/data-types/string.md)                                             | `bin 8`                            |
| `int 8`                                                            | [`Enum8`](/ru/sql-reference/data-types/enum.md)                                                | `int 8`                            |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/ru/sql-reference/data-types/int-uint.md)                            | `bin 8`                            |
| `int 32`                                                           | [`Decimal32`](/ru/sql-reference/data-types/decimal.md)                                         | `int 32`                           |
| `int 64`                                                           | [`Decimal64`](/ru/sql-reference/data-types/decimal.md)                                         | `int 64`                           |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/ru/sql-reference/data-types/decimal.md)                           | `bin 8 `                           |

<div id="example-usage">
  ## Пример использования
</div>

Запись в файл &quot;.msgpk&quot;:

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                                                                                          | Описание                                                                                                                | По умолчанию |
| ---------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------ |
| [`input_format_msgpack_number_of_columns`](/ru/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | количество столбцов во вставляемых данных MsgPack. Используется для автоматического определения схемы на основе данных. | `0`          |
| [`output_format_msgpack_uuid_representation`](/ru/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | способ вывода UUID в формате MsgPack.                                                                                   | `EXT`        |