---
description: 'Una extensión de la función de tabla paimon que permite procesar en paralelo archivos
  de Apache Paimon desde varios nodos de un clúster especificado.'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'referencia'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimoncluster-table-function">
  # Función de tabla paimonCluster
</div>

<ExperimentalBadge />

Esta es una extensión de la función de tabla [paimon](/es/sql-reference/table-functions/paimon.md).

Permite procesar archivos de Apache [Paimon](https://paimon.apache.org/) en paralelo desde varios nodos de un clúster especificado. En el nodo iniciador, crea una conexión con todos los nodos del clúster y distribuye dinámicamente cada archivo. En el nodo worker, consulta al iniciador cuál es la siguiente tarea que debe procesar y la procesa. Esto se repite hasta que todas las tareas hayan finalizado.

<div id="syntax">
  ## Sintaxis
</div>

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## Argumentos
</div>

* `cluster_name` — Nombre de un clúster que se utiliza para crear un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.
* La descripción del resto de los argumentos coincide con la de la función de tabla [paimon](/es/sql-reference/table-functions/paimon.md) equivalente.
* Se puede usar un parámetro opcional, `extra_credentials`, para pasar un `role_arn` para el acceso basado en roles en ClickHouse Cloud. Consulta [Secure S3](/es/cloud/data-sources/secure-s3) para ver los pasos de configuración.

**Valor devuelto**

Una tabla con la estructura especificada para leer datos del clúster desde la tabla Paimon especificada.

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño del archivo, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.
* `_etag` — El etag del archivo. Tipo: `LowCardinality(String)`. Si se desconoce el etag, el valor es `NULL`.

**Véase también**

* [función de tabla Paimon](/es/sql-reference/table-functions/paimon.md)