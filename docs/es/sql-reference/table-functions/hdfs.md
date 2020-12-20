---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: Hdfs
---

# Hdfs {#hdfs}

Crea una tabla a partir de archivos en HDFS. Esta función de tabla es similar a [URL](url.md) y [file](file.md) aquel.

``` sql
hdfs(URI, format, structure)
```

**Parámetros de entrada**

-   `URI` — The relative URI to the file in HDFS. Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` y `{N..M}` donde `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [formato](../../interfaces/formats.md#formats) del archivo.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Valor devuelto**

Una tabla con la estructura especificada para leer o escribir datos en el archivo especificado.

**Ejemplo**

Tabla de `hdfs://hdfs1:9000/test` y selección de las dos primeras filas de ella:

``` sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

**Globs en el camino**

Múltiples componentes de ruta de acceso pueden tener globs. Para ser procesado, el archivo debe existir y coincidir con todo el patrón de ruta (no solo el sufijo o el prefijo).

-   `*` — Substitutes any number of any characters except `/` incluyendo cadena vacía.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Construcciones con `{}` son similares a la [función de tabla remota](../../sql-reference/table-functions/remote.md)).

**Ejemplo**

1.  Supongamos que tenemos varios archivos con los siguientes URI en HDFS:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

1.  Consulta la cantidad de filas en estos archivos:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  Consulta la cantidad de filas en todos los archivos de estos dos directorios:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "Advertencia"
    Si su lista de archivos contiene rangos de números con ceros a la izquierda, use la construcción con llaves para cada dígito por separado o use `?`.

**Ejemplo**

Consultar los datos desde archivos nombrados `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Virtual Columnas {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**Ver también**

-   [Virtual columnas](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[Artículo Original](https://clickhouse.tech/docs/en/query_language/table_functions/hdfs/) <!--hide-->
