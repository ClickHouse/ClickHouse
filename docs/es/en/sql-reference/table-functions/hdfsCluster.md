---
description: 'Permite procesar archivos de HDFS en paralelo desde muchos nodos de un
  clúster especificado.'
sidebar_label: 'hdfsCluster'
sidebar_position: 81
slug: /sql-reference/table-functions/hdfsCluster
title: 'hdfsCluster'
doc_type: 'reference'
---

Permite procesar archivos de HDFS en paralelo desde muchos nodos de un clúster especificado. En el iniciador, crea una conexión con todos los nodos del clúster, expande los asteriscos en la ruta de archivo de HDFS y distribuye dinámicamente cada archivo. En el nodo de trabajo, consulta al iniciador cuál es la siguiente tarea que debe procesar y la procesa. Esto se repite hasta que todas las tareas se completan.

<div id="syntax">
  ## Sintaxis
</div>

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento      | Descripción                                                                                                                                                                                                                                                                                                                             |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | Nombre de un clúster que se utiliza para construir un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.                                                                                                                                                                                               |
| `URI`          | URI de un archivo o de un conjunto de archivos. Admite los siguientes comodines en modo `readonly`: `*`, `**`, `?`, `{'abc','def'}` y `{N..M}`, donde `N` y `M` son números, y `abc` y `def` son cadenas. Para obtener más información, consulte [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `format`       | El [formato](/es/sql-reference/formats) del archivo.                                                                                                                                                                                                                                                                                       |
| `structure`    | Estructura de la tabla. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                         |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con la estructura especificada para leer datos del archivo especificado.

<div id="examples">
  ## Ejemplos
</div>

1. Supongamos que tenemos un clúster de ClickHouse llamado `cluster_simple` y varios archivos con las siguientes URI en HDFS:

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Consulta el número de filas de estos archivos:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. Consulta el número de filas en todos los archivos de estos dos directorios:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
Si tu lista de archivos incluye intervalos numéricos con ceros a la izquierda, usa la construcción con llaves para cada dígito por separado o `?`.
:::

<div id="related">
  ## Véase también
</div>

* [Motor HDFS](../../engines/table-engines/integrations/hdfs.md)
* [Función de tabla HDFS](../../sql-reference/table-functions/hdfs.md)