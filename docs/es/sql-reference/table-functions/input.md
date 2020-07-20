---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: entrada
---

# entrada {#input}

`input(structure)` - función de tabla que permite convertir e insertar efectivamente los datos enviados al
servidor con estructura dada a la tabla con otra estructura.

`structure` - estructura de los datos enviados al servidor en el siguiente formato `'column1_name column1_type, column2_name column2_type, ...'`.
Por ejemplo, `'id UInt32, name String'`.

Esta función sólo se puede utilizar en `INSERT SELECT` consulta y sólo una vez, pero por lo demás se comporta como función de tabla ordinaria
(por ejemplo, se puede usar en subconsulta, etc.).

Los datos se pueden enviar de cualquier manera como para ordinario `INSERT` consulta y pasado en cualquier disponible [formato](../../interfaces/formats.md#formats)
que debe especificarse al final de la consulta (a diferencia de lo ordinario `INSERT SELECT`).

La característica principal de esta función es que cuando el servidor recibe datos del cliente, los convierte simultáneamente
según la lista de expresiones en el `SELECT` cláusula e inserta en la tabla de destino. Tabla temporal
con todos los datos transferidos no se crea.

**Ejemplos**

-   Deje que el `test` tiene la siguiente estructura `(a String, b String)`
    y datos en `data.csv` tiene una estructura diferente `(col1 String, col2 Date, col3 Int32)`. Consulta de inserción
    datos de la `data.csv` en el `test` con conversión simultánea se ve así:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   Si `data.csv` contiene datos de la misma estructura `test_structure` como la mesa `test` entonces estas dos consultas son iguales:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/table_functions/input/) <!--hide-->
