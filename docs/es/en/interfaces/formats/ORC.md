---
alias: []
description: 'Documentación del formato ORC'
input_format: true
keywords: ['ORC']
output_format: true
slug: /interfaces/formats/ORC
title: 'ORC'
doc_type: 'referencia'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

[Apache ORC](https://orc.apache.org/) es un formato de almacenamiento columnar muy utilizado en el ecosistema de [Hadoop](https://hadoop.apache.org/).

<div id="data-types-matching-orc">
  ## Correspondencia entre tipos de datos
</div>

La siguiente tabla compara los tipos de datos ORC admitidos y sus correspondientes [tipos de datos](/es/sql-reference/data-types/index.md) de ClickHouse en consultas `INSERT` y `SELECT`.

| Tipo de dato ORC (`INSERT`)           | Tipo de dato de ClickHouse                                                                        | Tipo de dato ORC (`SELECT`) |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- | --------------------------- |
| `Boolean`                             | [UInt8](/es/sql-reference/data-types/int-uint.md)                                                    | `Boolean`                   |
| `Tinyint`                             | [Int8/UInt8](/es/sql-reference/data-types/int-uint.md)/[Enum8](/es/sql-reference/data-types/enum.md)    | `Tinyint`                   |
| `Smallint`                            | [Int16/UInt16](/es/sql-reference/data-types/int-uint.md)/[Enum16](/es/sql-reference/data-types/enum.md) | `Smallint`                  |
| `Int`                                 | [Int32/UInt32](/es/sql-reference/data-types/int-uint.md)                                             | `Int`                       |
| `Bigint`                              | [Int64/UInt32](/es/sql-reference/data-types/int-uint.md)                                             | `Bigint`                    |
| `Float`                               | [Float32](/es/sql-reference/data-types/float.md)                                                     | `Float`                     |
| `Double`                              | [Float64](/es/sql-reference/data-types/float.md)                                                     | `Double`                    |
| `Decimal`                             | [Decimal](/es/sql-reference/data-types/decimal.md)                                                   | `Decimal`                   |
| `Date`                                | [Date32](/es/sql-reference/data-types/date32.md)                                                     | `Date`                      |
| `Timestamp`                           | [DateTime64](/es/sql-reference/data-types/datetime64.md)                                             | `Timestamp`                 |
| `String`, `Char`, `Varchar`, `Binary` | [String](/es/sql-reference/data-types/string.md)                                                     | `Binary`                    |
| `List`                                | [Array](/es/sql-reference/data-types/array.md)                                                       | `List`                      |
| `Struct`                              | [Tuple](/es/sql-reference/data-types/tuple.md)                                                       | `Struct`                    |
| `Map`                                 | [Map](/es/sql-reference/data-types/map.md)                                                           | `Map`                       |
| `Int`                                 | [IPv4](/es/sql-reference/data-types/int-uint.md)                                                     | `Int`                       |
| `Binary`                              | [IPv6](/es/sql-reference/data-types/ipv6.md)                                                         | `Binary`                    |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/es/sql-reference/data-types/int-uint.md)                            | `Binary`                    |
| `Binary`                              | [Decimal256](/es/sql-reference/data-types/decimal.md)                                                | `Binary`                    |

* No se admiten otros tipos.
* Los tipos `Array` pueden estar anidados y pueden tener como argumento un valor del tipo `Nullable`. Los tipos `Tuple` y `Map` también pueden estar anidados.
* Los tipos de datos de las columnas de la tabla de ClickHouse no tienen por qué coincidir con los campos de datos ORC correspondientes. Al insertar datos, ClickHouse interpreta los tipos de datos según la tabla anterior y luego [convierte](/es/sql-reference/functions/type-conversion-functions#CAST) los datos al tipo de dato definido para la columna de la tabla de ClickHouse.

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="inserting-data">
  ### Inserción de datos
</div>

Con un archivo ORC llamado `football.orc`, que contiene los siguientes datos:

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
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

<div id="reading-data">
  ### Leer datos
</div>

Lea los datos con el formato `ORC`:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC es un formato binario que no se muestra de forma legible para humanos en la terminal. Usa `INTO OUTFILE` para generar archivos ORC.
:::

<div id="format-settings">
  ## Configuración de formatos
</div>

| Configuración                                                                                                                                                                                        | Descripción                                                                                                         | Predeterminado |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------------- |
| [`output_format_arrow_string_as_string`](/es/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | Usa el tipo String de Arrow en lugar de Binary para las columnas String.                                            | `false`        |
| [`output_format_orc_compression_method`](/es/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | Método de compresión utilizado en el formato ORC de salida. Valor predeterminado                                    | `none`         |
| [`input_format_arrow_case_insensitive_column_matching`](/es/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | No distingue entre mayúsculas y minúsculas al hacer coincidir las columnas de Arrow con las columnas de ClickHouse. | `false`        |
| [`input_format_arrow_allow_missing_columns`](/es/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | Permite columnas ausentes al leer datos Arrow.                                                                      | `false`        |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/es/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | Permite omitir columnas con tipos no compatibles durante la inferencia de esquema del formato Arrow.                | `false`        |

Para intercambiar datos con Hadoop, puede usar el [motor de tabla HDFS](/es/engines/table-engines/integrations/hdfs.md).