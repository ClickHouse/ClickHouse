---
description: 'Una extensión de la función de tabla s3, que permite procesar archivos
  de Amazon S3 y Google Cloud Storage en paralelo con muchos nodos en un clúster
  especificado.'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
doc_type: 'reference'
---

Esta es una extensión de la función de tabla [s3](/es/sql-reference/table-functions/s3.md).

Permite procesar archivos de [Amazon S3](https://aws.amazon.com/s3/) y Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) en paralelo usando muchos nodos de un clúster especificado. En el nodo iniciador, crea una conexión con todos los nodos del clúster, expande los asteriscos en la ruta de archivo de S3 y distribuye dinámicamente cada archivo. En el nodo worker, solicita al iniciador la siguiente tarea que debe procesar y la procesa. Esto se repite hasta que se hayan completado todas las tareas.

<div id="syntax">
  ## Sintaxis
</div>

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento                               | Descripción                                                                                                                                                                                                                                                                                                                            |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                          | Nombre de un clúster que se utiliza para construir un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.                                                                                                                                                                                              |
| `url`                                   | Ruta a un archivo o a un conjunto de archivos. Admite los siguientes comodines en modo `readonly`: `*`, `**`, `?`, `{'abc','def'}` y `{N..M}`, donde `N` y `M` son números, y `abc` y `def` son cadenas. Para obtener más información, consulta [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `NOSIGN`                                | Si esta palabra clave se proporciona en lugar de credenciales, ninguna de las solicitudes se firmará.                                                                                                                                                                                                                                  |
| `access_key_id` and `secret_access_key` | Claves que especifican las credenciales que se usarán con el endpoint indicado. Opcional.                                                                                                                                                                                                                                              |
| `session_token`                         | Token de sesión que se usará con las claves indicadas. Opcional cuando se pasan claves.                                                                                                                                                                                                                                                |
| `format`                                | El [formato](/es/sql-reference/formats) del archivo.                                                                                                                                                                                                                                                                                      |
| `structure`                             | Estructura de la tabla. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                        |
| `compression_method`                    | El parámetro es opcional. Valores admitidos: `none`, `gzip` o `gz`, `brotli` o `br`, `xz` o `LZMA`, `zstd` o `zst`. De forma predeterminada, el método de compresión se detecta automáticamente a partir de la extensión del archivo.                                                                                                  |
| `headers`                               | El parámetro es opcional. Permite pasar headers en la solicitud a S3. Pásalos en el formato `headers(key=value)`; por ejemplo, `headers('x-amz-request-payer' = 'requester')`. Consulta [aquí](/es/sql-reference/table-functions/s3#accessing-requester-pays-buckets) un ejemplo de uso.                                                  |
| `extra_credentials`                     | Opcional. `roleARN` se puede pasar mediante este parámetro. Consulta [aquí](/es/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role) un ejemplo.                                                                                                                                                            |

Los argumentos también se pueden pasar mediante [colecciones con nombre](/es/operations/named-collections.md). En este caso, `url`, `access_key_id`, `secret_access_key`, `format`, `structure` y `compression_method` funcionan de la misma manera, y se admiten algunos parámetros adicionales:

| Argumento                     | Descripción                                                                                                                                                                                                                                                  |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `filename`                    | Se añade a la URL si se especifica.                                                                                                                                                                                                                          |
| `use_environment_credentials` | habilitado de forma predeterminada, permite pasar parámetros adicionales mediante las variables de entorno `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | deshabilitado de forma predeterminada.                                                                                                                                                                                                                       |
| `expiration_window_seconds`   | el valor predeterminado es 120.                                                                                                                                                                                                                              |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con la estructura especificada para leer o escribir datos en el archivo indicado.

<div id="examples">
  ## Ejemplos
</div>

Selecciona los datos de todos los archivos de las carpetas `/root/data/clickhouse` y `/root/data/database/` usando todos los nodos del clúster `cluster_simple`:

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

Cuenta el número total de filas de todos los archivos del clúster `cluster_simple`:

:::tip
Si el listado de archivos contiene rangos numéricos con ceros a la izquierda, usa la construcción con llaves para cada dígito por separado o `?`.
:::

Para casos de uso en producción, se recomienda usar [colecciones con nombre](/es/operations/named-collections.md). Aquí tienes el ejemplo:

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

<div id="accessing-private-and-public-buckets">
  ## Acceso a buckets privados y públicos
</div>

Los usuarios pueden utilizar los mismos métodos documentados para la función s3 [aquí](/es/sql-reference/table-functions/s3#accessing-public-buckets).

<div id="optimizing-performance">
  ## Optimización del rendimiento
</div>

Para más detalles sobre cómo optimizar el rendimiento de la función s3, consulta [nuestra guía detallada](/es/integrations/s3/performance).

<div id="related">
  ## Temas relacionados
</div>

* [motor S3](../../engines/table-engines/integrations/s3.md)
* [función de tabla S3](../../sql-reference/table-functions/s3.md)