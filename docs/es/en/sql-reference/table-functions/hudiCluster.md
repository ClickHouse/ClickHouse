---
description: 'Una extensión de la función de tabla hudi. Permite procesar archivos de
  tablas Apache Hudi en Amazon S3 en paralelo con muchos nodos en un clúster especificado.'
sidebar_label: 'hudiCluster'
sidebar_position: 86
slug: /sql-reference/table-functions/hudiCluster
title: 'Función de tabla hudiCluster'
doc_type: 'reference'
---

Esta es una extensión de la función de tabla [hudi](/es/sql-reference/table-functions/hudi.md).

Permite procesar archivos de tablas Apache [Hudi](https://hudi.apache.org/) en Amazon S3 en paralelo con muchos nodos en un clúster especificado. En el nodo iniciador, crea una conexión con todos los nodos del clúster y distribuye cada archivo dinámicamente. En el nodo worker, solicita al iniciador la siguiente tarea que debe procesar y la procesa. Esto se repite hasta que todas las tareas hayan finalizado.

<div id="syntax">
  ## Sintaxis
</div>

```sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento                                    | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                       |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                               | Nombre de un clúster que se utiliza para crear un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.                                                                                                                                                                                                                                                                                             |
| `url`                                        | URL del bucket con la ruta a una tabla Hudi existente en S3.                                                                                                                                                                                                                                                                                                                                                                      |
| `aws_access_key_id`, `aws_secret_access_key` | Credenciales de larga duración para el usuario de la cuenta de [AWS](https://aws.amazon.com/). Puede usarlas para autenticar las solicitudes. Estos parámetros son opcionales. Si no se especifican credenciales, se usarán las definidas en la configuración de ClickHouse. Para obtener más información, consulte [Using S3 for Data Storage](/es/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | El [formato](/es/interfaces/formats) del archivo.                                                                                                                                                                                                                                                                                                                                                                                    |
| `structure`                                  | Estructura de la tabla. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                   |
| `compression`                                | El parámetro es opcional. Valores admitidos: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. De forma predeterminada, la compresión se detecta automáticamente a partir de la extensión del archivo.                                                                                                                                                                                                                       |
| `extra_credentials`                          | El parámetro es opcional. Se utiliza para pasar un `role_arn` para el acceso basado en roles en ClickHouse Cloud. Consulte [Secure S3](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.                                                                                                                                                                                                                        |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con la estructura especificada para leer datos de la tabla Hudi especificada en S3 desde el clúster.

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño del archivo, el valor es `NULL`.
* `_time` — Fecha y hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.
* `_etag` — El etag del archivo. Tipo: `LowCardinality(String)`. Si se desconoce el etag, el valor es `NULL`.

<div id="related">
  ## Relacionado
</div>

* [motor Hudi](/es/engines/table-engines/integrations/hudi.md)
* [función de tabla de Hudi](/es/sql-reference/table-functions/hudi.md)