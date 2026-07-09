---
description: 'Crea una tabla a partir de archivos en HDFS. Esta función de tabla es similar a
  las funciones de tabla url y file.'
sidebar_label: 'hdfs'
sidebar_position: 80
slug: /sql-reference/table-functions/hdfs
title: 'hdfs'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-function">
  # Función de tabla hdfs
</div>

Crea una tabla a partir de archivos de HDFS. Esta función de tabla es similar a las funciones de tabla [url](../../sql-reference/table-functions/url.md) y [file](../../sql-reference/table-functions/file.md).

<div id="syntax">
  ## Sintaxis
</div>

```sql
hdfs(URI, format, structure)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento   | Descripción                                                                                                                                                                                        |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `URI`       | El URI relativo del archivo en HDFS. La ruta al archivo admite los siguientes patrones glob en modo de solo lectura: `*`, `?`, `{abc,def}` y `{N..M}`, donde `N`, `M` — números, `'abc', 'def'` — cadenas. |
| `format`    | El [formato](/es/sql-reference/formats) del archivo.                                                                                                                                                  |
| `structure` | Estructura de la tabla. Formato `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                     |

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con la estructura especificada para leer o escribir datos en el archivo indicado.

**Ejemplo**

Tabla de `hdfs://hdfs1:9000/test` y selección de las dos primeras filas:

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="globs_in_path">
  ## Globs en la ruta
</div>

Las rutas pueden usar patrones glob. Los archivos deben coincidir con el patrón completo de la ruta, no solo con el sufijo o el prefijo.

* `*` — Representa una cantidad arbitraria de caracteres, excepto `/`, aunque incluye la cadena vacía.
* `**` — Representa todos los archivos dentro de una carpeta de forma recursiva.
* `?` — Representa un único carácter arbitrario.
* `{some_string,another_string,yet_another_one}` — Sustituye cualquiera de las cadenas `'some_string', 'another_string', 'yet_another_one'`. Las cadenas pueden contener el símbolo `/`.
* `{N..M}` — Representa cualquier número `>= N` y `<= M`.

Las construcciones con `{}` son similares a las funciones de tabla [remote](remote.md) y [file](file.md).

**Ejemplo**

1. Supongamos que tenemos varios archivos con los siguientes URI en HDFS:

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Consulta la cantidad de filas de estos archivos:

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. Consulta el número de filas de todos los archivos de estos dos directorios:

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
Si tu lista de archivos contiene rangos numéricos con ceros a la izquierda, usa la construcción con llaves para cada dígito por separado o `?`.
:::

**Ejemplo**

Consulta los datos de los archivos llamados `file000`, `file001`, ... , `file999`:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.

<div id="hive-style-partitioning">
  ## ajuste use_hive_partitioning
</div>

Cuando el ajuste `use_hive_partitioning` se establece en 1, ClickHouse detecta el particionado de estilo Hive en la ruta (`/name=value/`) y permite usar las columnas de partición como columnas virtuales en la consulta. Estas columnas virtuales tendrán los mismos nombres que en la ruta particionada.

**Ejemplo**

Usar una columna virtual creada con particionado de estilo Hive

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="storage-settings">
  ## Ajustes de almacenamiento
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/es/operations/settings/settings.md#hdfs_truncate_on_insert) - permite truncar el archivo antes de insertar datos en él. Deshabilitado de forma predeterminada.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/es/operations/settings/settings.md#hdfs_create_new_file_on_insert) - permite crear un archivo nuevo con cada inserción si el formato tiene un sufijo. Deshabilitado de forma predeterminada.
* [hdfs&#95;skip&#95;empty&#95;files](/es/operations/settings/settings.md#hdfs_skip_empty_files) - permite omitir archivos vacíos durante la lectura. Deshabilitado de forma predeterminada.

<div id="related">
  ## Véase también
</div>

* [Columnas virtuales](../../engines/table-engines/index.md#table_engines-virtual_columns)