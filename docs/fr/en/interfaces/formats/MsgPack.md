---
alias: []
description: 'Documentation sur le format MsgPack'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

ClickHouse prend en charge la lecture et l’écriture de fichiers de données au format [MessagePack](https://msgpack.org/).

<div id="data-types-matching">
  ## Correspondance entre les types de données
</div>

| Type de données MessagePack (`INSERT`)                             | Type de données ClickHouse                                                                  | Type de données MessagePack (`SELECT`) |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | -------------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/fr/sql-reference/data-types/int-uint.md)                                            | `uint N`                               |
| `int N`, `negative fixint`                                         | [`IntN`](/fr/sql-reference/data-types/int-uint.md)                                             | `int N`                                |
| `bool`                                                             | [`UInt8`](/fr/sql-reference/data-types/int-uint.md)                                            | `uint 8`                               |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/fr/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`            |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/fr/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`            |
| `float 32`                                                         | [`Float32`](/fr/sql-reference/data-types/float.md)                                             | `float 32`                             |
| `float 64`                                                         | [`Float64`](/fr/sql-reference/data-types/float.md)                                             | `float 64`                             |
| `uint 16`                                                          | [`Date`](/fr/sql-reference/data-types/date.md)                                                 | `uint 16`                              |
| `int 32`                                                           | [`Date32`](/fr/sql-reference/data-types/date32.md)                                             | `int 32`                               |
| `uint 32`                                                          | [`DateTime`](/fr/sql-reference/data-types/datetime.md)                                         | `uint 32`                              |
| `uint 64`                                                          | [`DateTime64`](/fr/sql-reference/data-types/datetime.md)                                       | `uint 64`                              |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/fr/sql-reference/data-types/array.md)/[`Tuple`](/fr/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32`     |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/fr/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`           |
| `uint 32`                                                          | [`IPv4`](/fr/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                              |
| `bin 8`                                                            | [`String`](/fr/sql-reference/data-types/string.md)                                             | `bin 8`                                |
| `int 8`                                                            | [`Enum8`](/fr/sql-reference/data-types/enum.md)                                                | `int 8`                                |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/fr/sql-reference/data-types/int-uint.md)                            | `bin 8`                                |
| `int 32`                                                           | [`Decimal32`](/fr/sql-reference/data-types/decimal.md)                                         | `int 32`                               |
| `int 64`                                                           | [`Decimal64`](/fr/sql-reference/data-types/decimal.md)                                         | `int 64`                               |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/fr/sql-reference/data-types/decimal.md)                           | `bin 8 `                               |

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Écriture dans un fichier &quot;.msgpk&quot; :

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## Paramètres du format
</div>

| Paramètre                                                                                                                          | Description                                                                                                                   | Par défaut |
| ---------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ---------- |
| [`input_format_msgpack_number_of_columns`](/fr/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | le nombre de colonnes dans les données MsgPack insérées. Utilisé pour l’inférence automatique du schéma à partir des données. | `0`        |
| [`output_format_msgpack_uuid_representation`](/fr/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | la manière de représenter un UUID au format MsgPack en sortie.                                                                | `EXT`      |