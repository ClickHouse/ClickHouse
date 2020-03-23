# Hdfs {#hdfs}

Crea una tabla a partir de archivos en HDFS. Esta función de tabla es similar a [URL](url.md) y [file](file.md) Aquel.

``` sql
hdfs(URI, format, structure)
```

**Parámetros de entrada**

-   `URI` — El URI relativo al archivo en HDFS. Soporte de ruta a archivo siguiendo globs en modo de solo lectura: `*`, `?`, `{abc,def}` y `{N..M}` donde `N`, `M` — numero, \``'abc', 'def'` — cadena.
-   `format` — El [Formato](../../interfaces/formats.md#formats) del archivo.
-   `structure` — Estructura de la mesa. Formato `'column1_name column1_type, column2_name column2_type, ...'`.

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

-   `*` — Sustituye cualquier número de caracteres excepto `/` incluyendo cadena vacía.
-   `?` — Sustituye a cualquier carácter individual.
-   `{some_string,another_string,yet_another_one}` — Sustituye cualquiera de las cadenas `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Sustituye cualquier número en el intervalo de N a M, incluidas ambas fronteras.

Construcciones con `{}` hijo similares a la [función de tabla remota](../../query_language/table_functions/remote.md)).

**Ejemplo**

1.  Supongamos que tenemos varios archivos con los siguientes URI en HDFS:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

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

## Columnas virtuales {#virtual-columns}

-   `_path` — Ruta de acceso al archivo.
-   `_file` — Nombre del expediente.

**Ver también**

-   [Columnas virtuales](https://clickhouse.tech/docs/es/operations/table_engines/#table_engines-virtual_columns)

[Artículo Original](https://clickhouse.tech/docs/es/query_language/table_functions/hdfs/) <!--hide-->
