---
description: 'Función de tabla que permite convertir e insertar eficazmente los datos
  enviados al server con una estructura determinada en una tabla con otra estructura.'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

`input(structure)` - función de tabla que permite convertir e insertar eficazmente los datos enviados al
server con una estructura determinada en una tabla con otra estructura.

`structure` - estructura de los datos enviados al server en el siguiente formato: `'column1_name column1_type, column2_name column2_type, ...'`.
Por ejemplo, `'id UInt32, name String'`.

Esta función solo puede usarse en una consulta `INSERT SELECT` y solo una vez, pero por lo demás se comporta como una función de tabla normal
(por ejemplo, puede usarse en una subconsulta, etc.).

Los datos pueden enviarse de cualquier forma, como en una consulta `INSERT` normal, y pasarse en cualquier [formato](/es/sql-reference/formats)
disponible, que debe especificarse al final de la consulta (a diferencia de un `INSERT SELECT` normal).

La principal característica de esta función es que, cuando el server recibe datos del client, los convierte simultáneamente
según la lista de expresiones de la cláusula `SELECT` y los inserta en la tabla de destino. No se crea una tabla temporal
con todos los datos transferidos.

<div id="examples">
  ## Ejemplos
</div>

* Supongamos que la tabla `test` tiene la siguiente estructura `(a String, b String)`
  y que los datos de `data.csv` tienen una estructura diferente `(col1 String, col2 Date, col3 Int32)`. La consulta para insertar
  datos de `data.csv` en la tabla `test` con conversión simultánea es la siguiente:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

* Si `data.csv` contiene datos con la misma estructura `test_structure` que la tabla `test`, entonces estas dos consultas son equivalentes:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```