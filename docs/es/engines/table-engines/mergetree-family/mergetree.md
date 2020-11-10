---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 30
toc_title: "M\xE9todo de codificaci\xF3n de datos:"
---

# Método de codificación de datos: {#table_engines-mergetree}

El `MergeTree` motor y otros motores de esta familia (`*MergeTree`) son los motores de mesa ClickHouse más robustos.

Motores en el `MergeTree` familia están diseñados para insertar una gran cantidad de datos en una tabla. Los datos se escriben rápidamente en la tabla parte por parte, luego se aplican reglas para fusionar las partes en segundo plano. Este método es mucho más eficiente que reescribir continuamente los datos en almacenamiento durante la inserción.

Principales características:

-   Almacena datos ordenados por clave principal.

    Esto le permite crear un pequeño índice disperso que ayuda a encontrar datos más rápido.

-   Las particiones se pueden utilizar si [clave de partición](custom-partitioning-key.md) se especifica.

    ClickHouse admite ciertas operaciones con particiones que son más efectivas que las operaciones generales en los mismos datos con el mismo resultado. ClickHouse también corta automáticamente los datos de partición donde se especifica la clave de partición en la consulta. Esto también mejora el rendimiento de las consultas.

-   Soporte de replicación de datos.

    La familia de `ReplicatedMergeTree` proporciona la replicación de datos. Para obtener más información, consulte [Replicación de datos](replication.md).

-   Soporte de muestreo de datos.

    Si es necesario, puede establecer el método de muestreo de datos en la tabla.

!!! info "INFO"
    El [Fusionar](../special/merge.md#merge) el motor no pertenece al `*MergeTree` familia.

## Creación de una tabla {#table_engine-mergetree-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]
```

Para obtener una descripción de los parámetros, consulte [Descripción de la consulta CREATE](../../../sql-reference/statements/create.md).

!!! note "Nota"
    `INDEX` es una característica experimental, ver [Índices de saltos de datos](#table_engine-mergetree-data_skipping-indexes).

### Cláusulas de consulta {#mergetree-query-clauses}

-   `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. El `MergeTree` el motor no tiene parámetros.

-   `PARTITION BY` — The [clave de partición](custom-partitioning-key.md).

    Para particionar por mes, utilice el `toYYYYMM(date_column)` expresión, donde `date_column` es una columna con una fecha del tipo [Fecha](../../../sql-reference/data-types/date.md). Los nombres de partición aquí tienen el `"YYYYMM"` formato.

-   `ORDER BY` — The sorting key.

    Una tupla de columnas o expresiones arbitrarias. Ejemplo: `ORDER BY (CounterID, EventDate)`.

-   `PRIMARY KEY` — The primary key if it [difiere de la clave de clasificación](#choosing-a-primary-key-that-differs-from-the-sorting-key).

    De forma predeterminada, la clave principal es la misma que la clave de ordenación (que se especifica `ORDER BY` clausula). Por lo tanto, en la mayoría de los casos no es necesario especificar un `PRIMARY KEY` clausula.

-   `SAMPLE BY` — An expression for sampling.

    Si se utiliza una expresión de muestreo, la clave principal debe contenerla. Ejemplo: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

-   `TTL` — A list of rules specifying storage duration of rows and defining logic of automatic parts movement [entre discos y volúmenes](#table_engine-mergetree-multiple-volumes).

    La expresión debe tener una `Date` o `DateTime` columna como resultado. Ejemplo:
    `TTL date + INTERVAL 1 DAY`

    Tipo de regla `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'` especifica una acción que debe realizarse con la pieza si la expresión está satisfecha (alcanza la hora actual): eliminación de filas caducadas, mover una pieza (si la expresión está satisfecha para todas las filas de una pieza) al disco especificado (`TO DISK 'xxx'`) o al volumen (`TO VOLUME 'xxx'`). El tipo predeterminado de la regla es la eliminación (`DELETE`). Se puede especificar una lista de varias reglas, pero no debe haber más de una `DELETE` regla.

    Para obtener más información, consulte [TTL para columnas y tablas](#table_engine-mergetree-ttl)

-   `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:

    -   `index_granularity` — Maximum number of data rows between the marks of an index. Default value: 8192. See [Almacenamiento de datos](#mergetree-data-storage).
    -   `index_granularity_bytes` — Maximum size of data granules in bytes. Default value: 10Mb. To restrict the granule size only by number of rows, set to 0 (not recommended). See [Almacenamiento de datos](#mergetree-data-storage).
    -   `enable_mixed_granularity_parts` — Enables or disables transitioning to control the granule size with the `index_granularity_bytes` configuración. Antes de la versión 19.11, sólo existía el `index_granularity` ajuste para restringir el tamaño del gránulo. El `index_granularity_bytes` mejora el rendimiento de ClickHouse al seleccionar datos de tablas con filas grandes (decenas y cientos de megabytes). Si tiene tablas con filas grandes, puede habilitar esta configuración para que las tablas mejoren la eficiencia de `SELECT` consulta.
    -   `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If `use_minimalistic_part_header_in_zookeeper=1`, entonces ZooKeeper almacena menos datos. Para obtener más información, consulte [descripción del ajuste](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) en “Server configuration parameters”.
    -   `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` bytes, ClickHouse lee y escribe los datos en el disco de almacenamiento utilizando la interfaz de E / S directa (`O_DIRECT` opcion). Si `min_merge_bytes_to_use_direct_io = 0`, entonces la E/S directa está deshabilitada. Valor predeterminado: `10 * 1024 * 1024 * 1024` byte.
        <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    -   `merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with TTL. Default value: 86400 (1 day).
    -   `write_final_mark` — Enables or disables writing the final index mark at the end of data part (after the last byte). Default value: 1. Don't turn it off.
    -   `merge_max_block_size` — Maximum number of rows in block for merge operations. Default value: 8192.
    -   `storage_policy` — Storage policy. See [Uso de varios dispositivos de bloque para el almacenamiento de datos](#table_engine-mergetree-multiple-volumes).

**Ejemplo de configuración de secciones**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

En el ejemplo, configuramos la partición por mes.

También establecemos una expresión para el muestreo como un hash por el ID de usuario. Esto le permite pseudoaleatorizar los datos en la tabla para cada `CounterID` y `EventDate`. Si define un [SAMPLE](../../../sql-reference/statements/select/sample.md#select-sample-clause) cláusula al seleccionar los datos, ClickHouse devolverá una muestra de datos pseudoaleatoria uniforme para un subconjunto de usuarios.

El `index_granularity` se puede omitir porque 8192 es el valor predeterminado.

<details markdown="1">

<summary>Método obsoleto para crear una tabla</summary>

!!! attention "Atención"
    No utilice este método en nuevos proyectos. Si es posible, cambie los proyectos antiguos al método descrito anteriormente.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**Parámetros MergeTree()**

-   `date-column` — The name of a column of the [Fecha](../../../sql-reference/data-types/date.md) tipo. ClickHouse crea automáticamente particiones por mes en función de esta columna. Los nombres de partición están en el `"YYYYMM"` formato.
-   `sampling_expression` — An expression for sampling.
-   `(primary, key)` — Primary key. Type: [Tupla()](../../../sql-reference/data-types/tuple.md)
-   `index_granularity` — The granularity of an index. The number of data rows between the “marks” de un índice. El valor 8192 es apropiado para la mayoría de las tareas.

**Ejemplo**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

El `MergeTree` engine se configura de la misma manera que en el ejemplo anterior para el método de configuración del motor principal.
</details>

## Almacenamiento de datos {#mergetree-data-storage}

Una tabla consta de partes de datos ordenadas por clave principal.

Cuando se insertan datos en una tabla, se crean partes de datos separadas y cada una de ellas se ordena lexicográficamente por clave principal. Por ejemplo, si la clave principal es `(CounterID, Date)`, los datos en la parte se ordenan por `CounterID`, y dentro de cada `CounterID` es ordenado por `Date`.

Los datos que pertenecen a diferentes particiones se separan en diferentes partes. En el fondo, ClickHouse combina partes de datos para un almacenamiento más eficiente. Las piezas que pertenecen a particiones diferentes no se fusionan. El mecanismo de combinación no garantiza que todas las filas con la misma clave principal estén en la misma parte de datos.

Cada parte de datos se divide lógicamente en gránulos. Un gránulo es el conjunto de datos indivisibles más pequeño que ClickHouse lee al seleccionar datos. ClickHouse no divide filas o valores, por lo que cada gránulo siempre contiene un número entero de filas. La primera fila de un gránulo está marcada con el valor de la clave principal de la fila. Para cada parte de datos, ClickHouse crea un archivo de índice que almacena las marcas. Para cada columna, ya sea en la clave principal o no, ClickHouse también almacena las mismas marcas. Estas marcas le permiten encontrar datos directamente en archivos de columnas.

El tamaño del gránulo es restringido por `index_granularity` y `index_granularity_bytes` configuración del motor de tabla. El número de filas en un gránulo se encuentra en el `[1, index_granularity]` rango, dependiendo del tamaño de las filas. El tamaño de un gránulo puede exceder `index_granularity_bytes` si el tamaño de una sola fila es mayor que el valor de la configuración. En este caso, el tamaño del gránulo es igual al tamaño de la fila.

## Claves e índices principales en consultas {#primary-keys-and-indexes-in-queries}

Tome el `(CounterID, Date)` clave primaria como ejemplo. En este caso, la clasificación y el índice se pueden ilustrar de la siguiente manera:

      Whole data:     [---------------------------------------------]
      CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
      Marks:           |      |      |      |      |      |      |      |      |      |      |
                      a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      Marks numbers:   0      1      2      3      4      5      6      7      8      9      10

Si la consulta de datos especifica:

-   `CounterID in ('a', 'h')`, el servidor lee los datos en los rangos de marcas `[0, 3)` y `[6, 8)`.
-   `CounterID IN ('a', 'h') AND Date = 3`, el servidor lee los datos en los rangos de marcas `[1, 3)` y `[7, 8)`.
-   `Date = 3`, el servidor lee los datos en el rango de marcas `[1, 10]`.

Los ejemplos anteriores muestran que siempre es más efectivo usar un índice que un análisis completo.

Un índice disperso permite leer datos adicionales. Al leer un único rango de la clave primaria, hasta `index_granularity * 2` se pueden leer filas adicionales en cada bloque de datos.

Los índices dispersos le permiten trabajar con una gran cantidad de filas de tabla, porque en la mayoría de los casos, dichos índices caben en la RAM de la computadora.

ClickHouse no requiere una clave principal única. Puede insertar varias filas con la misma clave principal.

### Selección de la clave principal {#selecting-the-primary-key}

El número de columnas en la clave principal no está explícitamente limitado. Dependiendo de la estructura de datos, puede incluir más o menos columnas en la clave principal. Esto puede:

-   Mejorar el rendimiento de un índice.

    Si la clave principal es `(a, b)`, a continuación, añadir otra columna `c` mejorará el rendimiento si se cumplen las siguientes condiciones:

    -   Hay consultas con una condición en la columna `c`.
    -   Rangos de datos largos (varias veces más `index_granularity`) con valores idénticos para `(a, b)` son comunes. En otras palabras, al agregar otra columna le permite omitir rangos de datos bastante largos.

-   Mejorar la compresión de datos.

    ClickHouse ordena los datos por clave principal, por lo que cuanto mayor sea la consistencia, mejor será la compresión.

-   Proporcione una lógica adicional al fusionar partes de datos en el [ColapsarMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) y [SummingMergeTree](summingmergetree.md) motor.

    En este caso tiene sentido especificar el *clave de clasificación* que es diferente de la clave principal.

Una clave principal larga afectará negativamente al rendimiento de la inserción y al consumo de memoria, pero las columnas adicionales de la clave principal no afectarán al rendimiento de ClickHouse durante `SELECT` consulta.

### Elegir una clave principal que difiere de la clave de ordenación {#choosing-a-primary-key-that-differs-from-the-sorting-key}

Es posible especificar una clave principal (una expresión con valores que se escriben en el archivo de índice para cada marca) que es diferente de la clave de ordenación (una expresión para ordenar las filas en partes de datos). En este caso, la tupla de expresión de clave primaria debe ser un prefijo de la tupla de expresión de clave de ordenación.

Esta característica es útil cuando se [SummingMergeTree](summingmergetree.md) y
[AgregaciónMergeTree](aggregatingmergetree.md) motores de mesa. En un caso común cuando se utilizan estos motores, la tabla tiene dos tipos de columnas: *cota* y *medida*. Las consultas típicas agregan valores de columnas de medida con `GROUP BY` y filtrado por dimensiones. Debido a que SummingMergeTree y AggregatingMergeTree agregan filas con el mismo valor de la clave de ordenación, es natural agregarle todas las dimensiones. Como resultado, la expresión de clave consta de una larga lista de columnas y esta lista debe actualizarse con frecuencia con las dimensiones recién agregadas.

En este caso, tiene sentido dejar solo unas pocas columnas en la clave principal que proporcionarán análisis de rango eficientes y agregarán las columnas de dimensión restantes a la tupla de clave de clasificación.

[ALTER](../../../sql-reference/statements/alter.md) de la clave de ordenación es una operación ligera porque cuando se agrega una nueva columna simultáneamente a la tabla y a la clave de ordenación, las partes de datos existentes no necesitan ser cambiadas. Dado que la clave de ordenación anterior es un prefijo de la nueva clave de ordenación y no hay datos en la columna recién agregada, los datos se ordenan tanto por las claves de ordenación antiguas como por las nuevas en el momento de la modificación de la tabla.

### Uso de índices y particiones en consultas {#use-of-indexes-and-partitions-in-queries}

Para `SELECT` consultas, ClickHouse analiza si se puede usar un índice. Se puede usar un índice si el `WHERE/PREWHERE` clause tiene una expresión (como uno de los elementos de conjunción, o enteramente) que representa una operación de comparación de igualdad o desigualdad, o si tiene `IN` o `LIKE` con un prefijo fijo en columnas o expresiones que están en la clave principal o clave de partición, o en ciertas funciones parcialmente repetitivas de estas columnas, o relaciones lógicas de estas expresiones.

Por lo tanto, es posible ejecutar rápidamente consultas en uno o varios rangos de la clave principal. En este ejemplo, las consultas serán rápidas cuando se ejecuten para una etiqueta de seguimiento específica, para una etiqueta y un intervalo de fechas específicos, para una etiqueta y una fecha específicas, para varias etiquetas con un intervalo de fechas, etc.

Veamos el motor configurado de la siguiente manera:

      ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192

En este caso, en consultas:

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse utilizará el índice de clave principal para recortar datos incorrectos y la clave de partición mensual para recortar particiones que están en intervalos de fechas incorrectos.

Las consultas anteriores muestran que el índice se usa incluso para expresiones complejas. La lectura de la tabla está organizada de modo que el uso del índice no puede ser más lento que un escaneo completo.

En el siguiente ejemplo, el índice no se puede usar.

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

Para comprobar si ClickHouse puede usar el índice al ejecutar una consulta, use la configuración [Fecha de nacimiento](../../../operations/settings/settings.md#settings-force_index_by_date) y [force_primary_key](../../../operations/settings/settings.md).

La clave para particionar por mes permite leer solo aquellos bloques de datos que contienen fechas del rango adecuado. En este caso, el bloque de datos puede contener datos para muchas fechas (hasta un mes). Dentro de un bloque, los datos se ordenan por clave principal, que puede no contener la fecha como la primera columna. Debido a esto, el uso de una consulta con solo una condición de fecha que no especifica el prefijo de clave principal hará que se lean más datos que para una sola fecha.

### Uso del índice para claves primarias parcialmente monótonas {#use-of-index-for-partially-monotonic-primary-keys}

Considere, por ejemplo, los días del mes. Ellos forman un [monótona secuencia](https://en.wikipedia.org/wiki/Monotonic_function) durante un mes, pero no monótono durante períodos más prolongados. Esta es una secuencia parcialmente monotónica. Si un usuario crea la tabla con clave primaria parcialmente monótona, ClickHouse crea un índice disperso como de costumbre. Cuando un usuario selecciona datos de este tipo de tabla, ClickHouse analiza las condiciones de consulta. Si el usuario desea obtener datos entre dos marcas del índice y ambas marcas caen dentro de un mes, ClickHouse puede usar el índice en este caso particular porque puede calcular la distancia entre los parámetros de una consulta y las marcas de índice.

ClickHouse no puede usar un índice si los valores de la clave principal en el rango de parámetros de consulta no representan una secuencia monotónica. En este caso, ClickHouse utiliza el método de análisis completo.

ClickHouse usa esta lógica no solo para secuencias de días del mes, sino para cualquier clave principal que represente una secuencia parcialmente monotónica.

### Índices de saltos de datos (experimental) {#table_engine-mergetree-data_skipping-indexes}

La declaración de índice se encuentra en la sección de columnas del `CREATE` consulta.

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

Para tablas de la `*MergeTree` familia, se pueden especificar índices de omisión de datos.

Estos índices agregan cierta información sobre la expresión especificada en bloques, que consisten en `granularity_value` gránulos (el tamaño del gránulo se especifica utilizando el `index_granularity` ajuste en el motor de la tabla). Entonces estos agregados se usan en `SELECT` consultas para reducir la cantidad de datos a leer desde el disco omitiendo grandes bloques de datos donde el `where` consulta no puede ser satisfecha.

**Ejemplo**

``` sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX a (u64 * i32, s) TYPE minmax GRANULARITY 3,
    INDEX b (u64 * length(s)) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

ClickHouse puede utilizar los índices del ejemplo para reducir la cantidad de datos que se leen desde el disco en las siguientes consultas:

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### Tipos de índices disponibles {#available-types-of-indices}

-   `minmax`

    Almacena los extremos de la expresión especificada (si la expresión `tuple`, entonces almacena extremos para cada elemento de `tuple`), utiliza información almacenada para omitir bloques de datos como la clave principal.

-   `set(max_rows)`

    Almacena valores únicos de la expresión especificada (no más de `max_rows` filas, `max_rows=0` medio “no limits”). Utiliza los valores para comprobar si `WHERE` expresión no es satisfactorio en un bloque de datos.

-   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Tiendas a [Filtro de floración](https://en.wikipedia.org/wiki/Bloom_filter) que contiene todos los ngrams de un bloque de datos. Funciona solo con cadenas. Puede ser utilizado para la optimización de `equals`, `like` y `in` expresiones.

    -   `n` — ngram size,
    -   `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
    -   `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
    -   `random_seed` — The seed for Bloom filter hash functions.

-   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Lo mismo que `ngrambf_v1`, pero almacena tokens en lugar de ngrams. Los tokens son secuencias separadas por caracteres no alfanuméricos.

-   `bloom_filter([false_positive])` — Stores a [Filtro de floración](https://en.wikipedia.org/wiki/Bloom_filter) para las columnas especificadas.

    Opcional `false_positive` parámetro es la probabilidad de recibir una respuesta falsa positiva del filtro. Valores posibles: (0, 1). Valor predeterminado: 0.025.

    Tipos de datos admitidos: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`.

    Las siguientes funciones pueden usarlo: [igual](../../../sql-reference/functions/comparison-functions.md), [notEquals](../../../sql-reference/functions/comparison-functions.md), [en](../../../sql-reference/functions/in-functions.md), [noEn](../../../sql-reference/functions/in-functions.md), [tener](../../../sql-reference/functions/array-functions.md).

<!-- -->

``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

#### Funciones de apoyo {#functions-support}

Condiciones en el `WHERE` cláusula contiene llamadas de las funciones que operan con columnas. Si la columna forma parte de un índice, ClickHouse intenta usar este índice al realizar las funciones. ClickHouse admite diferentes subconjuntos de funciones para usar índices.

El `set` index se puede utilizar con todas las funciones. Subconjuntos de funciones para otros índices se muestran en la siguiente tabla.

| Función (operador) / Índice                                                                              | clave primaria | minmax | Descripción | Sistema abierto. | bloom_filter |
|----------------------------------------------------------------------------------------------------------|----------------|--------|-------------|------------------|---------------|
| [igual (=, ==)](../../../sql-reference/functions/comparison-functions.md#function-equals)                | ✔              | ✔      | ✔           | ✔                | ✔             |
| [notEquals(!=, \<\>)](../../../sql-reference/functions/comparison-functions.md#function-notequals)       | ✔              | ✔      | ✔           | ✔                | ✔             |
| [como](../../../sql-reference/functions/string-search-functions.md#function-like)                        | ✔              | ✔      | ✔           | ✗                | ✗             |
| [No como](../../../sql-reference/functions/string-search-functions.md#function-notlike)                  | ✔              | ✔      | ✔           | ✗                | ✗             |
| [Comienza con](../../../sql-reference/functions/string-functions.md#startswith)                          | ✔              | ✔      | ✔           | ✔                | ✗             |
| [Finaliza con](../../../sql-reference/functions/string-functions.md#endswith)                            | ✗              | ✗      | ✔           | ✔                | ✗             |
| [multiSearchAny](../../../sql-reference/functions/string-search-functions.md#function-multisearchany)    | ✗              | ✗      | ✔           | ✗                | ✗             |
| [en](../../../sql-reference/functions/in-functions.md#in-functions)                                      | ✔              | ✔      | ✔           | ✔                | ✔             |
| [noEn](../../../sql-reference/functions/in-functions.md#in-functions)                                    | ✔              | ✔      | ✔           | ✔                | ✔             |
| [menos (\<)](../../../sql-reference/functions/comparison-functions.md#function-less)                     | ✔              | ✔      | ✗           | ✗                | ✗             |
| [mayor (\>)](../../../sql-reference/functions/comparison-functions.md#function-greater)                  | ✔              | ✔      | ✗           | ✗                | ✗             |
| [menosOrEquals (\<=)](../../../sql-reference/functions/comparison-functions.md#function-lessorequals)    | ✔              | ✔      | ✗           | ✗                | ✗             |
| [mayorOrEquals (\>=)](../../../sql-reference/functions/comparison-functions.md#function-greaterorequals) | ✔              | ✔      | ✗           | ✗                | ✗             |
| [vaciar](../../../sql-reference/functions/array-functions.md#function-empty)                             | ✔              | ✔      | ✗           | ✗                | ✗             |
| [notEmpty](../../../sql-reference/functions/array-functions.md#function-notempty)                        | ✔              | ✔      | ✗           | ✗                | ✗             |
| hasToken                                                                                                 | ✗              | ✗      | ✗           | ✔                | ✗             |

Las funciones con un argumento constante que es menor que el tamaño de ngram no pueden ser utilizadas por `ngrambf_v1` para la optimización de consultas.

Los filtros Bloom pueden tener coincidencias falsas positivas, por lo que `ngrambf_v1`, `tokenbf_v1`, y `bloom_filter` los índices no se pueden usar para optimizar consultas donde se espera que el resultado de una función sea falso, por ejemplo:

-   Puede ser optimizado:
    -   `s LIKE '%test%'`
    -   `NOT s NOT LIKE '%test%'`
    -   `s = 1`
    -   `NOT s != 1`
    -   `startsWith(s, 'test')`
-   No se puede optimizar:
    -   `NOT s LIKE '%test%'`
    -   `s NOT LIKE '%test%'`
    -   `NOT s = 1`
    -   `s != 1`
    -   `NOT startsWith(s, 'test')`

## Acceso a datos simultáneos {#concurrent-data-access}

Para el acceso simultáneo a tablas, usamos versiones múltiples. En otras palabras, cuando una tabla se lee y actualiza simultáneamente, los datos se leen de un conjunto de partes que está actualizado en el momento de la consulta. No hay cerraduras largas. Las inserciones no se interponen en el camino de las operaciones de lectura.

La lectura de una tabla se paralela automáticamente.

## TTL para columnas y tablas {#table_engine-mergetree-ttl}

Determina la duración de los valores.

El `TTL` se puede establecer para toda la tabla y para cada columna individual. TTL de nivel de tabla también puede especificar la lógica de movimiento automático de datos entre discos y volúmenes.

Las expresiones deben evaluar [Fecha](../../../sql-reference/data-types/date.md) o [FechaHora](../../../sql-reference/data-types/datetime.md) tipo de datos.

Ejemplo:

``` sql
TTL time_column
TTL time_column + interval
```

Definir `interval`, utilizar [intervalo de tiempo](../../../sql-reference/operators/index.md#operators-datetime) operador.

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### Columna TTL {#mergetree-column-ttl}

Cuando los valores de la columna caducan, ClickHouse los reemplaza con los valores predeterminados para el tipo de datos de columna. Si todos los valores de columna en la parte de datos caducan, ClickHouse elimina esta columna de la parte de datos en un sistema de archivos.

El `TTL` cláusula no se puede utilizar para columnas clave.

Ejemplos:

Creación de una tabla con TTL

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

Adición de TTL a una columna de una tabla existente

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

Modificación de TTL de la columna

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### Tabla TTL {#mergetree-table-ttl}

La tabla puede tener una expresión para la eliminación de filas caducadas y varias expresiones para el movimiento automático de partes entre [discos o volúmenes](#table_engine-mergetree-multiple-volumes). Cuando las filas de la tabla caducan, ClickHouse elimina todas las filas correspondientes. Para la entidad de movimiento de piezas, todas las filas de una pieza deben cumplir los criterios de expresión de movimiento.

``` sql
TTL expr [DELETE|TO DISK 'aaa'|TO VOLUME 'bbb'], ...
```

El tipo de regla TTL puede seguir cada expresión TTL. Afecta a una acción que debe realizarse una vez que se satisface la expresión (alcanza la hora actual):

-   `DELETE` - Eliminar filas caducadas (acción predeterminada);
-   `TO DISK 'aaa'` - mover parte al disco `aaa`;
-   `TO VOLUME 'bbb'` - mover parte al disco `bbb`.

Ejemplos:

Creación de una tabla con TTL

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH [DELETE],
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

Modificación de TTL de la tabla

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**Eliminación de datos**

Los datos con un TTL caducado se eliminan cuando ClickHouse fusiona partes de datos.

Cuando ClickHouse ve que los datos han caducado, realiza una combinación fuera de programación. Para controlar la frecuencia de tales fusiones, puede establecer `merge_with_ttl_timeout`. Si el valor es demasiado bajo, realizará muchas fusiones fuera de horario que pueden consumir muchos recursos.

Si realiza el `SELECT` consulta entre fusiones, puede obtener datos caducados. Para evitarlo, use el [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) consulta antes `SELECT`.

## Uso de varios dispositivos de bloque para el almacenamiento de datos {#table_engine-mergetree-multiple-volumes}

### Implantación {#introduction}

`MergeTree` Los motores de tablas familiares pueden almacenar datos en múltiples dispositivos de bloque. Por ejemplo, puede ser útil cuando los datos de una determinada tabla se dividen implícitamente en “hot” y “cold”. Los datos más recientes se solicitan regularmente, pero solo requieren una pequeña cantidad de espacio. Por el contrario, los datos históricos de cola gorda se solicitan raramente. Si hay varios discos disponibles, el “hot” los datos pueden estar ubicados en discos rápidos (por ejemplo, SSD NVMe o en memoria), mientras que “cold” datos - en los relativamente lentos (por ejemplo, HDD).

La parte de datos es la unidad móvil mínima para `MergeTree`-mesas de motor. Los datos que pertenecen a una parte se almacenan en un disco. Las partes de datos se pueden mover entre discos en segundo plano (según la configuración del usuario) así como por medio de la [ALTER](../../../sql-reference/statements/alter.md#alter_move-partition) consulta.

### Plazo {#terms}

-   Disk — Block device mounted to the filesystem.
-   Default disk — Disk that stores the path specified in the [camino](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) configuración del servidor.
-   Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
-   Storage policy — Set of volumes and the rules for moving data between them.

Los nombres dados a las entidades descritas se pueden encontrar en las tablas del sistema, [sistema.almacenamiento_policies](../../../operations/system-tables.md#system_tables-storage_policies) y [sistema.disco](../../../operations/system-tables.md#system_tables-disks). Para aplicar una de las directivas de almacenamiento configuradas para una tabla, `storage_policy` establecimiento de `MergeTree`-mesas de la familia del motor.

### Configuración {#table_engine-mergetree-multiple-volumes_configure}

Los discos, los volúmenes y las políticas de almacenamiento deben declararse `<storage_configuration>` etiqueta ya sea en el archivo principal `config.xml` o en un archivo distinto en el `config.d` directorio.

Estructura de configuración:

``` xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

Tags:

-   `<disk_name_N>` — Disk name. Names must be different for all disks.
-   `path` — path under which a server will store data (`data` y `shadow` carpetas), debe terminarse con ‘/’.
-   `keep_free_space_bytes` — the amount of free disk space to be reserved.

El orden de la definición del disco no es importante.

Marcado de configuración de directivas de almacenamiento:

``` xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

Tags:

-   `policy_name_N` — Policy name. Policy names must be unique.
-   `volume_name_N` — Volume name. Volume names must be unique.
-   `disk` — a disk within a volume.
-   `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume's disks.
-   `move_factor` — when the amount of available space gets lower than this factor, data automatically start to move on the next volume if any (by default, 0.1).

Cofiguration ejemplos:

``` xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>
    </policies>
    ...
</storage_configuration>
```

En un ejemplo dado, el `hdd_in_order` la política implementa el [Ronda-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) enfoque. Por lo tanto, esta política define solo un volumen (`single`), las partes de datos se almacenan en todos sus discos en orden circular. Dicha política puede ser bastante útil si hay varios discos similares montados en el sistema, pero RAID no está configurado. Tenga en cuenta que cada unidad de disco individual no es confiable y es posible que desee compensarlo con un factor de replicación de 3 o más.

Si hay diferentes tipos de discos disponibles en el sistema, `moving_from_ssd_to_hdd` política se puede utilizar en su lugar. Volumen `hot` consta de un disco SSD (`fast_ssd`), y el tamaño máximo de una pieza que se puede almacenar en este volumen es de 1 GB. Todas las piezas con el tamaño más grande que 1GB serán almacenadas directamente en `cold` volumen, que contiene un disco duro `disk1`.
Además, una vez que el disco `fast_ssd` se llena en más del 80%, los datos se transferirán al `disk1` por un proceso en segundo plano.

El orden de enumeración de volúmenes dentro de una directiva de almacenamiento es importante. Una vez que un volumen está sobrellenado, los datos se mueven al siguiente. El orden de la enumeración del disco también es importante porque los datos se almacenan en ellos por turnos.

Al crear una tabla, se puede aplicarle una de las directivas de almacenamiento configuradas:

``` sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

El `default` política de almacenamiento implica el uso de un solo volumen, que consiste en un solo disco dado en `<path>`. Una vez que se crea una tabla, no se puede cambiar su política de almacenamiento.

### Detalles {#details}

En el caso de `MergeTree` tablas, los datos están llegando al disco de diferentes maneras:

-   Como resultado de un inserto (`INSERT` consulta).
-   Durante las fusiones de fondo y [mutación](../../../sql-reference/statements/alter.md#alter-mutations).
-   Al descargar desde otra réplica.
-   Como resultado de la congelación de particiones [ALTER TABLE … FREEZE PARTITION](../../../sql-reference/statements/alter.md#alter_freeze-partition).

En todos estos casos, excepto las mutaciones y la congelación de particiones, una pieza se almacena en un volumen y un disco de acuerdo con la política de almacenamiento dada:

1.  El primer volumen (en el orden de definición) que tiene suficiente espacio en disco para almacenar una pieza (`unreserved_space > current_part_size`) y permite almacenar partes de un tamaño determinado (`max_data_part_size_bytes > current_part_size`) se elige.
2.  Dentro de este volumen, se elige ese disco que sigue al que se utilizó para almacenar el fragmento de datos anterior y que tiene espacio libre más que el tamaño de la pieza (`unreserved_space - keep_free_space_bytes > current_part_size`).

Bajo el capó, las mutaciones y la congelación de particiones hacen uso de [enlaces duros](https://en.wikipedia.org/wiki/Hard_link). Los enlaces duros entre diferentes discos no son compatibles, por lo tanto, en tales casos las partes resultantes se almacenan en los mismos discos que los iniciales.

En el fondo, las partes se mueven entre volúmenes en función de la cantidad de espacio libre (`move_factor` parámetro) según el orden en que se declaran los volúmenes en el archivo de configuración.
Los datos nunca se transfieren desde el último y al primero. Uno puede usar tablas del sistema [sistema.part_log](../../../operations/system-tables.md#system_tables-part-log) (campo `type = MOVE_PART`) y [sistema.parte](../../../operations/system-tables.md#system_tables-parts) (campo `path` y `disk`) para monitorear movimientos de fondo. Además, la información detallada se puede encontrar en los registros del servidor.

El usuario puede forzar el movimiento de una pieza o una partición de un volumen a otro mediante la consulta [ALTER TABLE … MOVE PART\|PARTITION … TO VOLUME\|DISK …](../../../sql-reference/statements/alter.md#alter_move-partition), todas las restricciones para las operaciones en segundo plano se tienen en cuenta. La consulta inicia un movimiento por sí misma y no espera a que se completen las operaciones en segundo plano. El usuario recibirá un mensaje de error si no hay suficiente espacio libre disponible o si no se cumple alguna de las condiciones requeridas.

Mover datos no interfiere con la replicación de datos. Por lo tanto, se pueden especificar diferentes directivas de almacenamiento para la misma tabla en diferentes réplicas.

Después de la finalización de las fusiones y mutaciones de fondo, las partes viejas se eliminan solo después de un cierto período de tiempo (`old_parts_lifetime`).
Durante este tiempo, no se mueven a otros volúmenes o discos. Por lo tanto, hasta que las partes finalmente se eliminen, aún se tienen en cuenta para la evaluación del espacio en disco ocupado.

[Artículo Original](https://clickhouse.tech/docs/ru/operations/table_engines/mergetree/) <!--hide-->
