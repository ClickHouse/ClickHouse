---
alias: []
description: 'Documentación sobre el formato MsgPack'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

ClickHouse admite leer y escribir archivos de datos [MessagePack](https://msgpack.org/).

<div id="data-types-matching">
  ## Correspondencia entre tipos de datos
</div>

| Tipo de dato de MessagePack (`INSERT`)                             | Tipo de dato de ClickHouse                                                                  | Tipo de dato de MessagePack (`SELECT`) |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | -------------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/es/sql-reference/data-types/int-uint.md)                                            | `uint N`                               |
| `int N`, `negative fixint`                                         | [`IntN`](/es/sql-reference/data-types/int-uint.md)                                             | `int N`                                |
| `bool`                                                             | [`UInt8`](/es/sql-reference/data-types/int-uint.md)                                            | `uint 8`                               |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/es/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`            |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/es/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`            |
| `float 32`                                                         | [`Float32`](/es/sql-reference/data-types/float.md)                                             | `float 32`                             |
| `float 64`                                                         | [`Float64`](/es/sql-reference/data-types/float.md)                                             | `float 64`                             |
| `uint 16`                                                          | [`Date`](/es/sql-reference/data-types/date.md)                                                 | `uint 16`                              |
| `int 32`                                                           | [`Date32`](/es/sql-reference/data-types/date32.md)                                             | `int 32`                               |
| `uint 32`                                                          | [`DateTime`](/es/sql-reference/data-types/datetime.md)                                         | `uint 32`                              |
| `uint 64`                                                          | [`DateTime64`](/es/sql-reference/data-types/datetime.md)                                       | `uint 64`                              |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/es/sql-reference/data-types/array.md)/[`Tuple`](/es/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32`     |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/es/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`           |
| `uint 32`                                                          | [`IPv4`](/es/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                              |
| `bin 8`                                                            | [`String`](/es/sql-reference/data-types/string.md)                                             | `bin 8`                                |
| `int 8`                                                            | [`Enum8`](/es/sql-reference/data-types/enum.md)                                                | `int 8`                                |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/es/sql-reference/data-types/int-uint.md)                            | `bin 8`                                |
| `int 32`                                                           | [`Decimal32`](/es/sql-reference/data-types/decimal.md)                                         | `int 32`                               |
| `int 64`                                                           | [`Decimal64`](/es/sql-reference/data-types/decimal.md)                                         | `int 64`                               |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/es/sql-reference/data-types/decimal.md)                           | `bin 8 `                               |

<div id="example-usage">
  ## Ejemplo de uso
</div>

Escribir en un archivo &quot;.msgpk&quot;:

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## Configuración de formato
</div>

| Configuración                                                                                                                      | Descripción                                                                                                                    | Predeterminado |
| ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | -------------- |
| [`input_format_msgpack_number_of_columns`](/es/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | el número de columnas de los datos MsgPack insertados. Se usa para la inferencia automática del esquema a partir de los datos. | `0`            |
| [`output_format_msgpack_uuid_representation`](/es/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | la forma de generar el UUID en formato MsgPack.                                                                                | `EXT`          |