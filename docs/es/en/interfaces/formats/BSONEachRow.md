---
alias: []
description: 'Documentación sobre el formato BSONEachRow'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: 'referencia'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `BSONEachRow` analiza los datos como una secuencia de documentos Binary JSON (BSON) sin ningún separador entre ellos.
Cada fila se representa como un único documento y cada columna como un único campo de documento BSON, con el nombre de la columna como clave.

<div id="data-types-matching">
  ## Correspondencia de tipos de datos
</div>

Para la salida, se utiliza la siguiente correspondencia entre los tipos de ClickHouse y los tipos BSON:

| Tipo de ClickHouse                                                                                    | Tipo BSON                                                                                                                                 |
| ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| [Bool](/es/sql-reference/data-types/boolean.md)                                                          | `\x08` booleano                                                                                                                           |
| [Int8/UInt8](/es/sql-reference/data-types/int-uint.md)/[Enum8](/es/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                                              |
| [Int16/UInt16](/es/sql-reference/data-types/int-uint.md)/[Enum16](/es/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                                                              |
| [Int32](/es/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                                                              |
| [UInt32](/es/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                                                              |
| [Int64/UInt64](/es/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                                                              |
| [Float32/Float64](/es/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                                                             |
| [Date](/es/sql-reference/data-types/date.md)/[Date32](/es/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                                              |
| [DateTime](/es/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                                                              |
| [DateTime64](/es/sql-reference/data-types/datetime64.md)                                                 | `\x09` DateTime                                                                                                                           |
| [Decimal32](/es/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                                                              |
| [Decimal64](/es/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                                                              |
| [Decimal128](/es/sql-reference/data-types/decimal.md)                                                    | `\x05` binario, `\x00` subtipo binario, tamaño = 16                                                                                       |
| [Decimal256](/es/sql-reference/data-types/decimal.md)                                                    | `\x05` binario, `\x00` subtipo binario, tamaño = 32                                                                                       |
| [Int128/UInt128](/es/sql-reference/data-types/int-uint.md)                                               | `\x05` binario, `\x00` subtipo binario, tamaño = 16                                                                                       |
| [Int256/UInt256](/es/sql-reference/data-types/int-uint.md)                                               | `\x05` binario, `\x00` subtipo binario, tamaño = 32                                                                                       |
| [String](/es/sql-reference/data-types/string.md)/[FixedString](/es/sql-reference/data-types/fixedstring.md) | `\x05` binario, `\x00` subtipo binario o \x02 string si la opción output&#95;format&#95;bson&#95;string&#95;as&#95;string está habilitada |
| [UUID](/es/sql-reference/data-types/uuid.md)                                                             | `\x05` binario, `\x04` subtipo uuid, tamaño = 16                                                                                          |
| [Array](/es/sql-reference/data-types/array.md)                                                           | `\x04` array                                                                                                                              |
| [Tuple](/es/sql-reference/data-types/tuple.md)                                                           | `\x04` array                                                                                                                              |
| [Named Tuple](/es/sql-reference/data-types/tuple.md)                                                     | `\x03` document                                                                                                                           |
| [Map](/es/sql-reference/data-types/map.md)                                                               | `\x03` document                                                                                                                           |
| [IPv4](/es/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                                                              |
| [IPv6](/es/sql-reference/data-types/ipv6.md)                                                             | `\x05` binario, `\x00` subtipo binario                                                                                                    |

Para la entrada, se utiliza la siguiente correspondencia entre los tipos BSON y los tipos de ClickHouse:

| Tipo BSON                                      | Tipo de ClickHouse                                                                                                                                                                                  |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` double                                  | [Float32/Float64](/es/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` string                                  | [String](/es/sql-reference/data-types/string.md)/[FixedString](/es/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` documento                               | [Map](/es/sql-reference/data-types/map.md)/[Named Tuple](/es/sql-reference/data-types/tuple.md)                                                                                                           |
| `\x04` array                                   | [Array](/es/sql-reference/data-types/array.md)/[Tuple](/es/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` binario, `\x00` subtipo binario         | [String](/es/sql-reference/data-types/string.md)/[FixedString](/es/sql-reference/data-types/fixedstring.md)/[IPv6](/es/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` binario, `\x02` subtipo binario antiguo | [String](/es/sql-reference/data-types/string.md)/[FixedString](/es/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` binario, `\x03` subtipo UUID antiguo    | [UUID](/es/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` binario, `\x04` subtipo UUID            | [UUID](/es/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId                                | [String](/es/sql-reference/data-types/string.md)/[FixedString](/es/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` booleano                                 | [Bool](/es/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` DateTime                            | [DateTime64](/es/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` valor NULL                              | [NULL](/es/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` código JavaScript                       | [String](/es/sql-reference/data-types/string.md)/[FixedString](/es/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` símbolo                                 | [String](/es/sql-reference/data-types/string.md)/[FixedString](/es/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                                   | [Int32/UInt32](/es/sql-reference/data-types/int-uint.md)/[Decimal32](/es/sql-reference/data-types/decimal.md)/[IPv4](/es/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/es/sql-reference/data-types/enum.md) |
| `\x12` int64                                   | [Int64/UInt64](/es/sql-reference/data-types/int-uint.md)/[Decimal64](/es/sql-reference/data-types/decimal.md)/[DateTime64](/es/sql-reference/data-types/datetime64.md)                                       |

Los demás tipos BSON no son compatibles. Además, realiza conversiones entre distintos tipos enteros.
Por ejemplo, es posible insertar un valor BSON `int32` en ClickHouse como [`UInt8`](../../sql-reference/data-types/int-uint.md).

Los enteros grandes y los decimales, como `Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256`, pueden analizarse a partir de un valor Binary de BSON con el subtipo binario `\x00`.
En este caso, el formato validará que el tamaño de los datos binarios sea igual al tamaño del valor esperado.

:::note
Este formato no funciona correctamente en plataformas big-endian.
:::

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="inserting-data">
  ### Inserción de datos
</div>

Con un archivo BSON que contiene los siguientes datos, llamado `football.bson`:

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

Inserte los datos:

```sql
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### Lectura de datos
</div>

Lea los datos con el formato `BSONEachRow`:

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON es un formato binario que no se muestra de forma legible para humanos en la terminal. Use `INTO OUTFILE` para generar archivos BSON.
:::

<div id="format-settings">
  ## Configuración del formato
</div>

| Setting                                                                                                                                                                                               | Description                                                                                                     | Default |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | ------- |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | Usa el tipo String de BSON en lugar de Binary para las columnas String.                                         | `false` |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | Permite omitir columnas con tipos no compatibles durante la inferencia del esquema para el formato BSONEachRow. | `false` |