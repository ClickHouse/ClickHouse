---
alias: []
description: 'Documentación sobre el formato Arrow'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

[Apache Arrow](https://arrow.apache.org/) incluye dos formatos de almacenamiento columnar integrados.
ClickHouse admite operaciones de lectura y escritura para estos formatos.
`Arrow` es el formato de «modo de archivo» de Apache Arrow, diseñado para acceso aleatorio en memoria.

<div id="data-types-matching">
  ## Correspondencia de tipos de datos
</div>

La tabla siguiente muestra los tipos de datos compatibles y cómo se corresponden con los [tipos de datos](/es/sql-reference/data-types/index.md) de ClickHouse en las consultas `INSERT` y `SELECT`.

| Tipo de dato de Arrow (`INSERT`)        | Tipo de dato de ClickHouse                                                                                          | Tipo de dato de Arrow (`SELECT`) |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `BOOL`                                  | [Bool](/es/sql-reference/data-types/boolean.md)                                                                        | `BOOL`                           |
| `UINT8`, `BOOL`                         | [UInt8](/es/sql-reference/data-types/int-uint.md)                                                                      | `UINT8`                          |
| `INT8`                                  | [Int8](/es/sql-reference/data-types/int-uint.md)/[Enum8](/es/sql-reference/data-types/enum.md)                            | `INT8`                           |
| `UINT16`                                | [UInt16](/es/sql-reference/data-types/int-uint.md)                                                                     | `UINT16`                         |
| `INT16`                                 | [Int16](/es/sql-reference/data-types/int-uint.md)/[Enum16](/es/sql-reference/data-types/enum.md)                          | `INT16`                          |
| `UINT32`                                | [UInt32](/es/sql-reference/data-types/int-uint.md)                                                                     | `UINT32`                         |
| `INT32`                                 | [Int32](/es/sql-reference/data-types/int-uint.md)                                                                      | `INT32`                          |
| `UINT64`                                | [UInt64](/es/sql-reference/data-types/int-uint.md)                                                                     | `UINT64`                         |
| `INT64`                                 | [Int64](/es/sql-reference/data-types/int-uint.md)                                                                      | `INT64`                          |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/es/sql-reference/data-types/float.md)                                                                       | `FLOAT32`                        |
| `DOUBLE`                                | [Float64](/es/sql-reference/data-types/float.md)                                                                       | `FLOAT64`                        |
| `DATE32`                                | [Date32](/es/sql-reference/data-types/date32.md)                                                                       | `UINT16`                         |
| `DATE64`                                | [DateTime](/es/sql-reference/data-types/datetime.md)                                                                   | `UINT32`                         |
| `TIMESTAMP`                             | [DateTime64](/es/sql-reference/data-types/datetime64.md)                                                               | `TIMESTAMP`                      |
| `TIME32`, `TIME64`                      | [Time64](/es/sql-reference/data-types/time64.md)                                                                       | `TIME32`, `TIME64`               |
| `STRING`, `BINARY`                      | [String](/es/sql-reference/data-types/string.md)                                                                       | `BINARY`                         |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/es/sql-reference/data-types/fixedstring.md)                                                             | `FIXED_SIZE_BINARY`              |
| `DECIMAL`                               | [Decimal](/es/sql-reference/data-types/decimal.md)                                                                     | `DECIMAL`                        |
| `DECIMAL256`                            | [Decimal256](/es/sql-reference/data-types/decimal.md)                                                                  | `DECIMAL256`                     |
| `LIST`                                  | [Array](/es/sql-reference/data-types/array.md)                                                                         | `LIST`                           |
| `STRUCT`                                | [Tuple](/es/sql-reference/data-types/tuple.md)                                                                         | `STRUCT`                         |
| `MAP`                                   | [Map](/es/sql-reference/data-types/map.md)                                                                             | `MAP`                            |
| `UINT32`                                | [IPv4](/es/sql-reference/data-types/ipv4.md)                                                                           | `UINT32`                         |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/es/sql-reference/data-types/ipv6.md)                                                                           | `FIXED_SIZE_BINARY`              |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/es/sql-reference/data-types/int-uint.md)                                              | `FIXED_SIZE_BINARY`              |
| `DURATION`                              | [Interval](/es/sql-reference/data-types/special-data-types/interval.md) (Nanosegundo/Microsegundo/Milisegundo/Segundo) | `DURATION`                       |
| `INT64`                                 | [Interval](/es/sql-reference/data-types/special-data-types/interval.md) (Minuto/Hora/Día/Semana/Mes/Trimestre/Año)     | `INT64`                          |

Los `Array` pueden anidarse y aceptar un valor de tipo `Nullable` como argumento. Los tipos `Tuple` y `Map` también pueden anidarse.

El tipo `DICTIONARY` es compatible con las consultas `INSERT` y, para las consultas `SELECT`, existe una configuración [`output_format_arrow_low_cardinality_as_dictionary`](/es/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) que permite generar el tipo [LowCardinality](/es/sql-reference/data-types/lowcardinality.md) como un tipo `DICTIONARY`. Tenga en cuenta que puede haber valores no utilizados en el diccionario `LowCardinality`, lo que puede dar lugar a valores no utilizados en el `DICTIONARY` de Arrow en la salida.

Tipos de datos de Arrow no compatibles:

* `FIXED_SIZE_BINARY`
* `JSON`
* `UUID`
* `ENUM`.

Los tipos de datos de las columnas de la tabla de ClickHouse no tienen por qué coincidir con los campos de datos de Arrow correspondientes. Al insertar datos, ClickHouse interpreta los tipos de datos de acuerdo con la tabla anterior y luego [convierte](/es/sql-reference/functions/type-conversion-functions#CAST) los datos al tipo de datos establecido para la columna de la tabla de ClickHouse.

<div id="example-usage">
  ## Ejemplo de uso
</div>

En el ejemplo de abajo usamos el conjunto de datos `forex`, disponible en el
[Playground de SQL de ClickHouse](https://sql.clickhouse.com).

<div id="selecting-data">
  ### Selección de datos
</div>

Seleccionamos un día de tipos de cambio de `EUR/USD` del Playground y lo guardamos
en un archivo local llamado `forex_eurusd.arrow`. Hacemos la consulta al Playground a través de la interfaz
HTTP, donde el host es `sql-clickhouse.clickhouse.com` y el usuario es
`demo` (sin contraseña):

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
  ### Volver a leer el archivo
</div>

Ahora podemos volver a leer el archivo local de Arrow con
[`clickhouse-local`](/es/operations/utilities/clickhouse-local) usando la
función de tabla [`file`](/es/sql-reference/table-functions/file). El archivo es
autodescriptivo, así que el formato `Arrow` deduce el esquema automáticamente:

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
  ### Inserción de datos
</div>

Para cargar un archivo Arrow en una tabla de ClickHouse, envíalo a `clickhouse-client`
mediante una tubería con `FORMAT Arrow`:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## Configuración del formato
</div>

| Configuración                                                                | Descripción                                                                                                         | Predeterminado |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------------- |
| `input_format_arrow_allow_missing_columns`                                   | Permite columnas ausentes al leer formatos de entrada Arrow                                                         | `1`            |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignora las mayúsculas y minúsculas al hacer coincidir columnas Arrow con columnas CH.                               | `0`            |
| `input_format_arrow_import_nested`                                           | Configuración obsoleta, no hace nada.                                                                               | `0`            |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Omite columnas con tipos no compatibles durante la inferencia de esquema para el formato Arrow                      | `0`            |
| `output_format_arrow_compression_method`                                     | Método de compresión para el formato de salida Arrow. Códecs compatibles: lz4&#95;frame, zstd, none (sin comprimir) | `lz4_frame`    |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Usa el tipo Arrow FIXED&#95;SIZE&#95;BINARY en lugar de Binary para columnas FixedString.                           | `1`            |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Habilita la salida del tipo LowCardinality como tipo Arrow Dictionary                                               | `0`            |
| `output_format_arrow_string_as_string`                                       | Usa el tipo Arrow String en lugar de Binary para columnas String                                                    | `1`            |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Usa siempre enteros de 64 bits para índices de diccionario en formato Arrow                                         | `0`            |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Usa enteros con signo para índices de diccionario en formato Arrow                                                  | `1`            |