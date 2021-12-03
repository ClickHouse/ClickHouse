---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

Este motor proporciona integración con [Acerca de nosotros](https://en.wikipedia.org/wiki/Apache_Hadoop) permitiendo gestionar datos sobre [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)a través de ClickHouse. Este motor es similar
a la [File](../special/file.md#table_engines-file) y [URL](../special/url.md#table_engines-url) motores, pero proporciona características específicas de Hadoop.

## Uso {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

El `URI` El parámetro es el URI del archivo completo en HDFS.
El `format` parámetro especifica uno de los formatos de archivo disponibles. Realizar
`SELECT` consultas, el formato debe ser compatible para la entrada, y para realizar
`INSERT` queries – for output. The available formats are listed in the
[Formato](../../../interfaces/formats.md#formats) apartado.
La parte de la ruta de `URI` puede contener globs. En este caso, la tabla sería de solo lectura.

**Ejemplo:**

**1.** Configurar el `hdfs_engine_table` tabla:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Llenar archivo:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Consultar los datos:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Detalles de implementación {#implementation-details}

-   Las lecturas y escrituras pueden ser paralelas
-   No soportado:
    -   `ALTER` y `SELECT...SAMPLE` operación.
    -   Índices.
    -   Replicación.

**Globs en el camino**

Múltiples componentes de ruta de acceso pueden tener globs. Para ser procesado, el archivo debe existir y coincidir con todo el patrón de ruta. Listado de archivos determina durante `SELECT` (no en `CREATE` momento).

-   `*` — Substitutes any number of any characters except `/` incluyendo cadena vacía.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Construcciones con `{}` son similares a la [remoto](../../../sql-reference/table-functions/remote.md) función de la tabla.

**Ejemplo**

1.  Supongamos que tenemos varios archivos en formato TSV con los siguientes URI en HDFS:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

1.  Hay varias maneras de hacer una tabla que consta de los seis archivos:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Otra forma:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

La tabla consta de todos los archivos en ambos directorios (todos los archivos deben satisfacer el formato y el esquema descritos en la consulta):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "Advertencia"
    Si la lista de archivos contiene rangos de números con ceros a la izquierda, use la construcción con llaves para cada dígito por separado o use `?`.

**Ejemplo**

Crear tabla con archivos llamados `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## Virtual Columnas {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**Ver también**

-   [Virtual columnas](../index.md#table_engines-virtual_columns)

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/hdfs/) <!--hide-->
