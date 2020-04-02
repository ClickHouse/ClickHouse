---
machine_translated: true
---

# Búfer {#buffer}

Almacena los datos para escribir en la memoria RAM, enjuagándolos periódicamente a otra tabla. Durante la operación de lectura, los datos se leen desde el búfer y la otra tabla simultáneamente.

``` sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
```

Parámetros del motor:

-   `database` – Nombre de la base de datos. En lugar del nombre de la base de datos, puede usar una expresión constante que devuelva una cadena.
-   `table` – Tabla para eliminar los datos.
-   `num_layers` – Capa de paralelismo. Físicamente, la tabla se representará como `num_layers` de búferes independientes. Valor recomendado: 16.
-   `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, y `max_bytes` – Condiciones para el lavado de datos del búfer.

Los datos se vacían del búfer y se escriben en la tabla de destino si `min*` condiciones o al menos una `max*` condición se cumplen.

-   `min_time`, `max_time` – Condición para el tiempo en segundos desde el momento de la primera escritura en el búfer.
-   `min_rows`, `max_rows` – Condición para el número de filas en el búfer.
-   `min_bytes`, `max_bytes` – Condición para el número de bytes en el búfer.

Durante la operación de escritura, los datos se insertan en un `num_layers` número de búferes aleatorios. O bien, si la parte de datos para insertar es lo suficientemente grande (mayor que `max_rows` o `max_bytes`), se escribe directamente en la tabla de destino, omitiendo el búfer.

Las condiciones para el lavado de los datos se calculan por separado para cada uno de los `num_layers` búfer. Por ejemplo, si `num_layers = 16` y `max_bytes = 100000000`, el consumo máximo de RAM es de 1.6 GB.

Ejemplo:

``` sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

Creación de un ‘merge.hits\_buffer’ mesa con la misma estructura que ‘merge.hits’ y usando el motor Buffer. Al escribir en esta tabla, los datos se almacenan en la memoria RAM y ‘merge.hits’ tabla. Se crean 16 búferes. Los datos de cada uno de ellos se vacían si han pasado 100 segundos o se han escrito un millón de filas o se han escrito 100 MB de datos; o si simultáneamente han pasado 10 segundos y se han escrito 10.000 filas y 10 MB de datos. Por ejemplo, si solo se ha escrito una fila, después de 100 segundos se vaciará, pase lo que pase. Pero si se han escrito muchas filas, los datos se vaciarán antes.

Cuando se detiene el servidor, con DROP TABLE o DETACH TABLE, los datos del búfer también se vacían a la tabla de destino.

Puede establecer cadenas vacías entre comillas simples para la base de datos y el nombre de la tabla. Esto indica la ausencia de una tabla de destino. En este caso, cuando se alcanzan las condiciones de descarga de datos, el búfer simplemente se borra. Esto puede ser útil para mantener una ventana de datos en la memoria.

Al leer desde una tabla de búfer, los datos se procesan tanto desde el búfer como desde la tabla de destino (si hay uno).
Tenga en cuenta que las tablas Buffer no admiten un índice. En otras palabras, los datos del búfer se analizan por completo, lo que puede ser lento para los búferes grandes. (Para los datos de una tabla subordinada, se utilizará el índice que admite.)

Si el conjunto de columnas de la tabla Buffer no coinciden con el conjunto de columnas de una tabla subordinada, se inserta un subconjunto de columnas que existen en ambas tablas.

Si los tipos no coinciden con una de las columnas de la tabla Búfer y una tabla subordinada, se escribe un mensaje de error en el registro del servidor y se borra el búfer.
Lo mismo sucede si la tabla subordinada no existe cuando se vacía el búfer.

Si necesita ejecutar ALTER para una tabla subordinada y la tabla de búfer, se recomienda eliminar primero la tabla de búfer, ejecutar ALTER para la tabla subordinada y, a continuación, crear la tabla de búfer de nuevo.

Si el servidor se reinicia de forma anormal, se pierden los datos del búfer.

FINAL y SAMPLE no funcionan correctamente para las tablas Buffer. Estas condiciones se pasan a la tabla de destino, pero no se utilizan para procesar datos en el búfer. Si se requieren estas características, recomendamos usar solo la tabla Buffer para escribir, mientras lee desde la tabla de destino.

Al agregar datos a un búfer, uno de los búferes está bloqueado. Esto provoca retrasos si se realiza una operación de lectura simultáneamente desde la tabla.

Los datos que se insertan en una tabla de búfer pueden terminar en la tabla subordinada en un orden diferente y en bloques diferentes. Debido a esto, una tabla Buffer es difícil de usar para escribir en un CollapsingMergeTree correctamente. Para evitar problemas, puede establecer ‘num\_layers’ a 1.

Si se replica la tabla de destino, se pierden algunas características esperadas de las tablas replicadas al escribir en una tabla de búfer. Los cambios aleatorios en el orden de las filas y los tamaños de las partes de datos hacen que la desduplicación de datos deje de funcionar, lo que significa que no es posible tener un ‘exactly once’ escribir en tablas replicadas.

Debido a estas desventajas, solo podemos recomendar el uso de una tabla Buffer en casos raros.

Una tabla de búfer se utiliza cuando se reciben demasiados INSERT de un gran número de servidores durante una unidad de tiempo y los datos no se pueden almacenar en búfer antes de la inserción, lo que significa que los INSERT no pueden ejecutarse lo suficientemente rápido.

Tenga en cuenta que no tiene sentido insertar datos una fila a la vez, incluso para tablas de búfer. Esto solo producirá una velocidad de unos pocos miles de filas por segundo, mientras que la inserción de bloques de datos más grandes puede producir más de un millón de filas por segundo (consulte la sección “Performance”).

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/buffer/) <!--hide-->
