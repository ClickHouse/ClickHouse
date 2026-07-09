---
alias: []
description: 'Documentação do formato MsgPack'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'referência'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O ClickHouse oferece suporte à leitura e à gravação de arquivos de dados [MessagePack](https://msgpack.org/).

<div id="data-types-matching">
  ## Correspondência entre tipos de dados
</div>

| Tipo de dado do MessagePack (`INSERT`)                             | Tipo de dado do ClickHouse                                                                  | Tipo de dado do MessagePack (`SELECT`) |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | -------------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/pt-BR/sql-reference/data-types/int-uint.md)                                            | `uint N`                               |
| `int N`, `negative fixint`                                         | [`IntN`](/pt-BR/sql-reference/data-types/int-uint.md)                                             | `int N`                                |
| `bool`                                                             | [`UInt8`](/pt-BR/sql-reference/data-types/int-uint.md)                                            | `uint 8`                               |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/pt-BR/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`            |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/pt-BR/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`            |
| `float 32`                                                         | [`Float32`](/pt-BR/sql-reference/data-types/float.md)                                             | `float 32`                             |
| `float 64`                                                         | [`Float64`](/pt-BR/sql-reference/data-types/float.md)                                             | `float 64`                             |
| `uint 16`                                                          | [`Date`](/pt-BR/sql-reference/data-types/date.md)                                                 | `uint 16`                              |
| `int 32`                                                           | [`Date32`](/pt-BR/sql-reference/data-types/date32.md)                                             | `int 32`                               |
| `uint 32`                                                          | [`DateTime`](/pt-BR/sql-reference/data-types/datetime.md)                                         | `uint 32`                              |
| `uint 64`                                                          | [`DateTime64`](/pt-BR/sql-reference/data-types/datetime.md)                                       | `uint 64`                              |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/pt-BR/sql-reference/data-types/array.md)/[`Tuple`](/pt-BR/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32`     |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/pt-BR/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`           |
| `uint 32`                                                          | [`IPv4`](/pt-BR/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                              |
| `bin 8`                                                            | [`String`](/pt-BR/sql-reference/data-types/string.md)                                             | `bin 8`                                |
| `int 8`                                                            | [`Enum8`](/pt-BR/sql-reference/data-types/enum.md)                                                | `int 8`                                |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/pt-BR/sql-reference/data-types/int-uint.md)                            | `bin 8`                                |
| `int 32`                                                           | [`Decimal32`](/pt-BR/sql-reference/data-types/decimal.md)                                         | `int 32`                               |
| `int 64`                                                           | [`Decimal64`](/pt-BR/sql-reference/data-types/decimal.md)                                         | `int 64`                               |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/pt-BR/sql-reference/data-types/decimal.md)                           | `bin 8 `                               |

<div id="example-usage">
  ## Exemplo de uso
</div>

Gravando em um arquivo &quot;.msgpk&quot;:

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## Configurações de formato
</div>

| Configuração                                                                                                                       | Descrição                                                                                                          | Padrão |
| ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------ |
| [`input_format_msgpack_number_of_columns`](/pt-BR/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | o número de colunas nos dados MsgPack inseridos. Usado para a inferência automática de esquema a partir dos dados. | `0`    |
| [`output_format_msgpack_uuid_representation`](/pt-BR/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | a forma de gerar o UUID no formato MsgPack.                                                                        | `EXT`  |