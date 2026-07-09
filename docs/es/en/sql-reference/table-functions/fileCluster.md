---
description: 'Permite el procesamiento simultáneo de archivos que coinciden con una ruta especificada en
  varios nodos dentro de un clúster. El iniciador establece conexiones con los
  nodos worker, expande los globs en la ruta de archivo y delega tareas de lectura de archivos a los nodos worker.
  Cada nodo worker consulta al iniciador cuál es el siguiente archivo que debe procesar,
  repitiendo esto hasta que se completen todas las tareas (se lean todos los archivos).'
sidebar_label: 'fileCluster'
sidebar_position: 61
slug: /sql-reference/table-functions/fileCluster
title: 'fileCluster'
doc_type: 'reference'
---

Permite el procesamiento simultáneo de archivos que coinciden con una ruta especificada en varios nodos dentro de un clúster. El iniciador establece conexiones con los nodos worker, expande los globs en la ruta de archivo y delega tareas de lectura de archivos a los nodos worker. Cada nodo worker consulta al iniciador cuál es el siguiente archivo que debe procesar, repitiendo esto hasta que se completen todas las tareas (se lean todos los archivos).

:::note
Esta función funcionará *correctamente* solo si el conjunto de archivos que coincide con la ruta especificada inicialmente es idéntico en todos los nodos y su contenido es consistente entre los distintos nodos.
Si estos archivos difieren entre nodos, el valor devuelto no puede determinarse de antemano y depende del orden en que los nodos worker soliciten tareas al iniciador.
:::

<div id="syntax">
  ## Sintaxis
</div>

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento            | Descripción                                                                                                                                                                                    |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`       | Nombre de un clúster que se utiliza para crear un conjunto de direcciones y parámetros de conexión para servidores remotos y locales.                                                          |
| `path`               | Ruta relativa al archivo desde [user&#95;files&#95;path](/es/operations/server-configuration-parameters/settings.md#user_files_path). La ruta del archivo también admite [globs](#globs-in-path). |
| `format`             | [Formato](/es/sql-reference/formats) de los archivos. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                   |
| `structure`          | Estructura de la tabla en el formato `'UserID UInt64, Name String'`. Determina los nombres y tipos de las columnas. Tipo: [String](../../sql-reference/data-types/string.md).                  |
| `compression_method` | Método de compresión. Los tipos de compresión compatibles son `gz`, `br`, `xz`, `zst`, `lz4` y `bz2`.                                                                                          |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con el formato y la estructura especificados, y con datos de los archivos que coinciden con la ruta especificada.

**Ejemplo**

Dado un clúster llamado `my_cluster` y el siguiente valor de la configuración `user_files_path`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

Además, dado que en `user_files_path` de cada nodo del clúster están los archivos `test1.csv` y `test2.csv`, y su contenido es idéntico en todos los nodos:

```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

Por ejemplo, estos archivos pueden crearse ejecutando estas dos consultas en cada nodo del clúster:

```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

Ahora, lea el contenido de `test1.csv` y `test2.csv` mediante la función de tabla `fileCluster`:

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

<div id="globs-in-path">
  ## Globs en la ruta
</div>

FileCluster admite todos los patrones compatibles con la función de tabla [File](../../sql-reference/table-functions/file.md#globs-in-path).

<div id="related">
  ## Relacionado
</div>

* [Función de tabla File](../../sql-reference/table-functions/file.md)