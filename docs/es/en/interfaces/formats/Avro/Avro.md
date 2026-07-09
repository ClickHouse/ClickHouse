---
alias: []
description: 'Documentación sobre el formato Avro'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'reference'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

[Apache Avro](https://avro.apache.org/) es un formato de serialización orientado a filas que utiliza codificación binaria para procesar datos de forma eficiente. El formato `Avro` admite la lectura y escritura de [archivos de datos Avro](https://avro.apache.org/docs/current/specification/#object-container-files). Este formato requiere mensajes autodescriptivos con un esquema incrustado. Si usas Avro con un schema registry, consulta el formato [`AvroConfluent`](./AvroConfluent.md).

<div id="data-type-mapping">
  ## Correspondencia de tipos de datos
</div>

<DataTypeMapping />

<div id="format-settings">
  ## Configuración del formato
</div>

| Configuración                              | Descripción                                                                                                                                                                                               | Predeterminado |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| `input_format_avro_allow_missing_fields`   | Indica si se debe usar un valor predeterminado en lugar de generar un error cuando no se encuentra un campo en el esquema.                                                                                | `0`            |
| `input_format_avro_null_as_default`        | Indica si se debe usar un valor predeterminado en lugar de generar un error al insertar un valor `null` en una columna que no admite valores nulos.                                                       | `0`            |
| `output_format_avro_codec`                 | Algoritmo de compresión para los archivos de salida Avro. Posibles valores: `null`, `deflate`, `snappy`, `zstd`.                                                                                          |                |
| `output_format_avro_sync_interval`         | Frecuencia del marcador de sincronización en los archivos Avro (en bytes).                                                                                                                                | `16384`        |
| `output_format_avro_string_column_pattern` | Expresión regular para identificar columnas `String` para la correspondencia de tipos de cadena de Avro. De forma predeterminada, las columnas `String` de ClickHouse se escriben como tipo Avro `bytes`. |                |
| `output_format_avro_rows_in_file`          | Número máximo de filas por archivo de salida Avro. Cuando se alcanza este límite, se crea un archivo nuevo (si el sistema de almacenamiento admite dividir archivos).                                     | `1`            |

<div id="examples">
  ## Ejemplos
</div>

<div id="reading-avro-data">
  ### Leer datos Avro
</div>

Para leer datos de un archivo Avro e insertarlos en una tabla de ClickHouse:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

El esquema raíz del archivo Avro ingerido debe ser de tipo `record`.

Para encontrar la correspondencia entre las columnas de la tabla y los campos del esquema de Avro, ClickHouse compara sus nombres.
Esta comparación es sensible a mayúsculas y minúsculas, y los campos no utilizados se omiten.

Los tipos de datos de las columnas de la tabla de ClickHouse pueden diferir de los de los campos correspondientes de los datos Avro insertados. Al insertar datos, ClickHouse interpreta los tipos de datos según la tabla anterior y luego [convierte](/es/sql-reference/functions/type-conversion-functions#CAST) los datos al tipo de columna correspondiente.

Al importar datos, cuando no se encuentra un campo en el esquema y la configuración [`input_format_avro_allow_missing_fields`](/es/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) está habilitada, se usará el valor predeterminado en lugar de generar un error.

<div id="writing-avro-data">
  ### Escritura de datos Avro
</div>

Para escribir datos de una tabla de ClickHouse en un archivo Avro:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Los nombres de las columnas deben:

* Empezar por `[A-Za-z_]`
* Ir seguidos únicamente de `[A-Za-z0-9_]`

La compresión de salida y el intervalo de sincronización de los archivos Avro se pueden configurar mediante los ajustes [`output_format_avro_codec`](/es/operations/settings/settings-formats.md/#output_format_avro_codec) y [`output_format_avro_sync_interval`](/es/operations/settings/settings-formats.md/#output_format_avro_sync_interval), respectivamente.

<div id="inferring-the-avro-schema">
  ### Inferencia del esquema Avro
</div>

Con la función [`DESCRIBE`](/es/sql-reference/statements/describe-table) de ClickHouse, puede ver rápidamente el formato inferido de un archivo Avro, como en el siguiente ejemplo.
Este ejemplo incluye la URL de un archivo Avro accesible públicamente en el bucket público de S3 de ClickHouse:

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```