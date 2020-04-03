---
machine_translated: true
---

# HDFS {#table_engines-hdfs}

Este motor proporciona integración con [Acerca de nosotros](https://en.wikipedia.org/wiki/Apache_Hadoop) permitiendo gestionar datos sobre [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)a través de ClickHouse. Este motor es similar
Angeles [File](file.md) y [URL](url.md) motores, pero proporciona características específicas de Hadoop.

## Uso {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

El `URI` El parámetro es el URI del archivo completo en HDFS.
El `format` parámetro especifica uno de los formatos de archivo disponibles. Realizar
`SELECT` consultas, el formato debe ser compatible para la entrada, y para realizar
`INSERT` consultas – para la salida. Los formatos disponibles se enumeran en el
[Formato](../../interfaces/formats.md#formats) apartado.
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

-   `*` — Sustituye cualquier número de caracteres excepto `/` incluyendo cadena vacía.
-   `?` — Sustituye a cualquier carácter individual.
-   `{some_string,another_string,yet_another_one}` — Sustituye cualquiera de las cadenas `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Sustituye cualquier número en el intervalo de N a M, incluidas ambas fronteras.

Construcciones con `{}` hijo similares a la [remoto](../../query_language/table_functions/remote.md) función de la tabla.

**Ejemplo**

1.  Supongamos que tenemos varios archivos en formato TSV con los siguientes URI en HDFS:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

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

## Columnas virtuales {#virtual-columns}

-   `_path` — Ruta de acceso al archivo.
-   `_file` — Nombre del expediente.

**Ver también**

-   [Columnas virtuales](https://clickhouse.tech/docs/es/operations/table_engines/#table_engines-virtual_columns)

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/hdfs/) <!--hide-->
