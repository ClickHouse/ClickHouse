---
description: 'Una extensión de la función de tabla iceberg que permite procesar archivos
  de Apache Iceberg en paralelo desde muchos nodos de un clúster especificado.'
sidebar_label: 'icebergCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/icebergCluster
title: 'icebergCluster'
doc_type: 'reference'
---

Esta es una extensión de la función de tabla [iceberg](/es/sql-reference/table-functions/iceberg.md).

Permite procesar archivos de Apache [Iceberg](https://iceberg.apache.org/) en paralelo desde muchos nodos de un clúster especificado. En el iniciador, crea una conexión con todos los nodos del clúster y asigna cada archivo dinámicamente. En el nodo worker, consulta al iniciador cuál es la siguiente tarea que debe procesar y la procesa. Este proceso se repite hasta que se completan todas las tareas.

<div id="syntax">
  ## Sintaxis
</div>

```sql
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])

icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])

icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Argumentos
</div>

* `cluster_name` — Nombre de un clúster que se utiliza para crear un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.
* La descripción del resto de los argumentos coincide con la de la función de tabla [iceberg](/es/sql-reference/table-functions/iceberg.md) equivalente.
* Se puede usar un parámetro opcional `extra_credentials` para proporcionar un `role_arn` para el acceso basado en roles en ClickHouse Cloud. Consulta [Secure S3](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.

**Valor devuelto**

Una tabla con la estructura especificada para leer datos del clúster desde la tabla Iceberg especificada.

**Ejemplos**

```sql
SELECT * FROM icebergS3Cluster('cluster_simple', 'http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño del archivo, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.
* `_etag` — El etag del archivo. Tipo: `LowCardinality(String)`. Si se desconoce el etag, el valor es `NULL`.

**Véase también**

* [motor Iceberg](/es/engines/table-engines/integrations/iceberg.md)
* [función de tabla Iceberg](/es/sql-reference/table-functions/iceberg.md)