---
description: 'Proporciona una interfaz de solo lectura similar a una tabla para tablas
  Hudi en Amazon S3.'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

Proporciona una interfaz de solo lectura similar a una tabla para las tablas [Hudi](https://hudi.apache.org/) en Amazon S3.

<div id="syntax">
  ## Sintaxis
</div>

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento                                    | Descripción                                                                                                                                                                                                                                                                                                                                                                                                      |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                                        | URL del bucket con la ruta a una tabla Hudi existente en S3.                                                                                                                                                                                                                                                                                                                                                     |
| `aws_access_key_id`, `aws_secret_access_key` | Credenciales a largo plazo para el usuario de la cuenta de [AWS](https://aws.amazon.com/). Puede usarlas para autenticar sus solicitudes. Estos parámetros son opcionales. Si no se especifican credenciales, se tomarán de la configuración de ClickHouse. Para obtener más información, consulte [Using S3 for Data Storage](/es/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | El [formato](/es/interfaces/formats) del archivo.                                                                                                                                                                                                                                                                                                                                                                   |
| `structure`                                  | Estructura de la tabla. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                  |
| `compression`                                | El parámetro es opcional. Valores admitidos: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. De forma predeterminada, la compresión se detecta automáticamente a partir de la extensión del archivo.                                                                                                                                                                                                      |
| `extra_credentials`                          | El parámetro es opcional. Se usa para pasar un `role_arn` para el acceso basado en roles en ClickHouse Cloud. Consulte [Secure S3](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.                                                                                                                                                                                                           |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con la estructura especificada para leer datos de la tabla Hudi especificada en S3.

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño del archivo, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.
* `_etag` — El etag del archivo. Tipo: `LowCardinality(String)`. Si se desconoce el etag, el valor es `NULL`.

<div id="related">
  ## Relacionados
</div>

* [motor Hudi](/es/engines/table-engines/integrations/hudi.md)
* [función de tabla de clúster de Hudi](/es/sql-reference/table-functions/hudiCluster.md)