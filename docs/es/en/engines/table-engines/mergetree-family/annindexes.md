---
description: 'Documentación sobre la búsqueda vectorial exacta y aproximada'
keywords: ['búsqueda de similitud vectorial', 'ann', 'knn', 'hnsw', 'índices', 'índice', 'vecino más cercano', 'búsqueda vectorial']
sidebar_label: 'Búsqueda vectorial exacta y aproximada'
slug: /engines/table-engines/mergetree-family/annindexes
title: 'Búsqueda vectorial exacta y aproximada'
doc_type: 'guide'
---

El problema de encontrar los N puntos más cercanos en un espacio multidimensional (vectorial) para un punto dado se conoce como [búsqueda del vecino más cercano](https://en.wikipedia.org/wiki/Nearest_neighbor_search) o, en resumen, búsqueda vectorial.
Existen dos enfoques generales para resolver la búsqueda vectorial:

* La búsqueda vectorial exacta calcula la distancia entre el punto dado y todos los puntos del espacio vectorial. Esto garantiza la mejor precisión posible; es decir, los puntos devueltos son, con certeza, los vecinos más cercanos reales. Como el espacio vectorial se recorre de forma exhaustiva, la búsqueda vectorial exacta puede ser demasiado lenta para su uso en el mundo real.
* La búsqueda vectorial aproximada se refiere a un conjunto de técnicas (por ejemplo, estructuras de datos especiales como grafos y bosques aleatorios) que calculan resultados mucho más rápido que la búsqueda vectorial exacta. La precisión de los resultados suele ser &quot;suficientemente buena&quot; para un uso práctico. Muchas técnicas aproximadas ofrecen parámetros para ajustar el equilibrio entre la precisión de los resultados y el tiempo de búsqueda.

Una búsqueda vectorial (exacta o aproximada) puede expresarse en SQL de la siguiente manera:

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

Los puntos en el espacio vectorial se almacenan en una columna `vectors` de tipo Array, p. ej., [Array(Float64)](../../../sql-reference/data-types/array.md), [Array(Float32)](../../../sql-reference/data-types/array.md) o [Array(BFloat16)](../../../sql-reference/data-types/array.md).
El vector de referencia es un array constante y se define como una expresión común de tabla.
`<DistanceFunction>` calcula la distancia entre el punto de referencia y todos los puntos almacenados.
Para ello, se puede usar cualquiera de las [funciones de distancia](/es/sql-reference/functions/distance-functions) disponibles.
`<N>` especifica cuántos vecinos deben devolverse.

<div id="exact-nearest-neighbor-search">
  ## Búsqueda vectorial exacta
</div>

Se puede realizar una búsqueda vectorial exacta usando la consulta SELECT anterior tal cual.
El tiempo de ejecución de estas consultas suele ser proporcional al número de vectores almacenados y a su dimensión, es decir, al número de elementos del array.
Además, dado que ClickHouse realiza un escaneo exhaustivo de todos los vectores, el tiempo de ejecución también depende del número de hilos que utilice la consulta (consulte la configuración [max&#95;threads](../../../operations/settings/settings.md#max_threads)).

<div id="exact-nearest-neighbor-search-example">
  ### Ejemplo
</div>

```sql
CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

Devuelve

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

<div id="approximate-nearest-neighbor-search">
  ## Búsqueda vectorial aproximada
</div>

<div id="vector-similarity-index">
  ### Índices de similitud vectorial
</div>

ClickHouse proporciona un índice especial de &quot;similitud vectorial&quot; para realizar búsquedas vectoriales aproximadas.

:::note
Los índices de similitud vectorial están disponibles en ClickHouse versión 25.8 o superior.
Si tiene algún problema, abra un issue en el [repositorio de ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

<div id="creating-a-vector-similarity-index">
  #### Creación de un índice de similitud vectorial
</div>

Un índice de similitud vectorial se puede crear en una nueva tabla de la siguiente manera:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>]
)
ENGINE = MergeTree
ORDER BY [...]
```

Como alternativa, para agregar un índice de similitud vectorial a una tabla existente:

```sql
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>];
```

Los índices de similitud vectorial son tipos especiales de índices de omisión (véase [aquí](mergetree.md#table_engine-mergetree-data_skipping-indexes) y [aquí](../../../optimize/skipping-indexes)).
Por lo tanto, la instrucción `ALTER TABLE` anterior solo hace que el índice se construya para los nuevos datos que se inserten en la tabla en el futuro.
Para construir el índice también sobre los datos existentes, es necesario materializarlo:

```sql
ALTER TABLE table MATERIALIZE INDEX <index_name> SETTINGS mutations_sync = 2;
```

La función `<distance_function>` debe ser

* `L2Distance`, la [distancia euclidiana](https://en.wikipedia.org/wiki/Euclidean_distance), que representa la longitud de la línea que une dos puntos en el espacio euclidiano,
* `cosineDistance`, la [distancia de coseno](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance), que representa el ángulo entre dos vectores no nulos, o
* `dotProduct`, el [producto escalar](https://en.wikipedia.org/wiki/Dot_product) (producto interno), que representa la suma de los productos elemento a elemento de dos vectores. Equivale a `cosineDistance` en datos normalizados.

Para datos normalizados, `L2Distance` suele ser la mejor opción; de lo contrario, se recomienda `cosineDistance` para compensar la escala.

:::note
Para las funciones de distancia `L2Distance` y `cosineDistance`, un valor menor indica mayor similitud, mientras que para `dotProduct`, un valor mayor indica mayor similitud.
Por lo tanto, los índices vectoriales con `L2Distance` y `cosineDistance` solo pueden ser utilizados por consultas `SELECT [...] ORDER BY [...] ASC` (`ASC` es el valor predeterminado para `ORDER BY`), mientras que los índices vectoriales construidos para `dotProduct` solo pueden ser utilizados por consultas `SELECT [...] ORDER BY [...] DESC`.
:::

`<dimensions>` especifica la cardinalidad del array (número de elementos) en la columna subyacente.
Si ClickHouse encuentra un array con una cardinalidad diferente durante la creación del índice, el índice se descarta y se devuelve un error.

El parámetro opcional GRANULARITY `<N>` hace referencia al tamaño de los gránulos del índice (véase [aquí](../../../optimize/skipping-indexes)).
A diferencia de los skip indexes regulares, que utilizan una granularidad de índice predeterminada de 1, los vector similarity indexes utilizan 100 millones como granularidad de índice predeterminada.
Este valor garantiza que solo se construyan internamente unos pocos índices, incluso para partes de gran tamaño.
Se recomienda modificar la granularidad del índice únicamente para usuarios avanzados que comprendan las implicaciones de lo que están haciendo (véase [más abajo](#differences-to-regular-skipping-indexes)).

Los índices de similitud vectorial son genéricos en el sentido de que pueden admitir diferentes métodos de búsqueda aproximada.
El método utilizado se especifica mediante el parámetro `<type>`.
Por ahora, el único método disponible es HNSW ([artículo académico](https://arxiv.org/abs/1603.09320)), una técnica popular y de última generación para la búsqueda vectorial aproximada basada en grafos de proximidad jerárquicos.
Si se utiliza HNSW como tipo, los usuarios pueden especificar opcionalmente parámetros específicos de HNSW:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, <quantization>, <hnsw_max_connections_per_layer>, <hnsw_candidate_list_size_for_construction>]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

Los siguientes parámetros específicos de HNSW están disponibles:

* `<quantization>` controla la cuantización de los vectores en el grafo de proximidad. Los posibles valores son `f64`, `f32`, `f16`, `bf16`, `i8` o `b1`. El valor predeterminado es `bf16`. Tenga en cuenta que este parámetro no afecta a la representación de los vectores en la columna subyacente.
* `<hnsw_max_connections_per_layer>` controla el número de vecinos por nodo del grafo, también conocido como el hiperparámetro HNSW `M`. El valor predeterminado es `32`. El valor `0` significa usar el valor predeterminado.
* `<hnsw_candidate_list_size_for_construction>` controla el tamaño de la lista dinámica de candidatos durante la construcción del grafo HNSW, también conocido como el hiperparámetro HNSW `ef_construction`. El valor predeterminado es `128`. El valor `0` significa usar el valor predeterminado.

Los valores predeterminados de todos los parámetros específicos de HNSW funcionan razonablemente bien en la mayoría de los casos de uso.
Por lo tanto, no recomendamos personalizar los parámetros específicos de HNSW.

Se aplican además las siguientes restricciones:

* Los índices de similitud vectorial solo pueden construirse sobre columnas de tipo [Array(Float32)](../../../sql-reference/data-types/array.md), [Array(Float64)](../../../sql-reference/data-types/array.md) o [Array(BFloat16)](../../../sql-reference/data-types/array.md). No se permiten arrays de valores de coma flotante anulables ni de baja cardinalidad, como `Array(Nullable(Float32))` y `Array(LowCardinality(Float32))`.
* Los índices de similitud vectorial deben construirse sobre una sola columna.
* Los índices de similitud vectorial pueden construirse sobre expresiones calculadas (por ejemplo, `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`), pero esos índices no pueden usarse posteriormente para la búsqueda aproximada de vecinos.
* Los índices de similitud vectorial requieren que todos los arrays de la columna subyacente tengan `<dimension>` elementos; esto se comprueba durante la creación del índice. Para detectar incumplimientos de este requisito lo antes posible, los usuarios pueden añadir una [restricción](/es/sql-reference/statements/create/table.md#constraints) para la columna vectorial, por ejemplo, `CONSTRAINT same_length CHECK length(vectors) = 256`.
* Del mismo modo, los valores de array de la columna subyacente no deben estar vacíos (`[]`) ni tener un valor predeterminado (también `[]`).

**Estimación del consumo de almacenamiento y memoria**

Un vector generado para su uso con un modelo de IA típico (p. ej., un Large Language Model, [LLMs](https://en.wikipedia.org/wiki/Large_language_model)) consta de cientos o miles de valores de coma flotante.
Por tanto, un único valor vectorial puede consumir varios kilobytes de memoria.
Los usuarios que quieran estimar el almacenamiento necesario para la columna vectorial subyacente de la tabla, así como la memoria principal necesaria para el índice de similitud vectorial, pueden usar las dos fórmulas siguientes:

Consumo de almacenamiento de la columna vectorial en la tabla (sin comprimir):

```text
Storage consumption = Number of vectors * Dimension * Size of column data type
```

Ejemplo con el [conjunto de datos de DBpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M):

```text
Storage consumption = 1 million * 1536 * 4 (for Float32) = 6.1 GB
```

El índice de similitud vectorial debe cargarse por completo desde el disco a la memoria principal para poder realizar búsquedas.
Del mismo modo, el índice vectorial también se construye íntegramente en memoria y luego se guarda en disco.

Consumo de memoria necesario para cargar un índice vectorial:

```text
Memory for vectors in the index (mv) = Number of vectors * Dimension * Size of quantized data type
Memory for in-memory graph (mg) = Number of vectors * hnsw_max_connections_per_layer * Bytes_per_node_id (= 4) * Layer_node_repetition_factor (= 2)

Memory consumption: mv + mg
```

Ejemplo con el [conjunto de datos de DBpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M):

```text
Memory for vectors in the index (mv) = 1 million * 1536 * 2 (for BFloat16) = 3072 MB
Memory for in-memory graph (mg) = 1 million * 64 * 2 * 4 = 512 MB

Memory consumption = 3072 + 512 = 3584 MB
```

La fórmula anterior no contempla la memoria adicional que requieren los índices de similitud vectorial para asignar estructuras de datos en tiempo de ejecución, como búferes y cachés preasignados.

<div id="using-a-vector-similarity-index">
  #### Uso de un índice de similitud vectorial
</div>

:::note
Para usar índices de similitud vectorial, la configuración [compatibility](../../../operations/settings/settings.md) debe estar establecida en `''` (el valor predeterminado), `'25.1'` o una versión posterior.
:::

Los índices de similitud vectorial admiten consultas SELECT de esta forma:

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

El optimizador de consultas de ClickHouse intenta reconocer la plantilla de consulta anterior y aprovechar los índices de similitud vectorial disponibles.
Una consulta solo puede usar un índice de similitud vectorial si la función de distancia de la consulta SELECT es la misma que la función de distancia de la definición del índice.

Los usuarios avanzados pueden proporcionar un valor personalizado para la configuración [hnsw&#95;candidate&#95;list&#95;size&#95;for&#95;search](../../../operations/settings/settings.md#hnsw_candidate_list_size_for_search) (también conocida como el hiperparámetro HNSW &quot;ef&#95;search&quot;) para ajustar el tamaño de la lista de candidatos durante la búsqueda (p. ej., `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`).
El valor predeterminado de la configuración, 256, funciona bien en la mayoría de los casos de uso.
Los valores más altos de esta configuración mejoran la precisión a costa de un rendimiento más lento.

Si la consulta puede usar un índice de similitud vectorial, ClickHouse comprueba que el LIMIT `<N>` proporcionado en las consultas SELECT esté dentro de unos límites razonables.
Más concretamente, se devuelve un error si `<N>` es mayor que el valor de la configuración [max&#95;limit&#95;for&#95;vector&#95;search&#95;queries](../../../operations/settings/settings.md#max_limit_for_vector_search_queries), cuyo valor predeterminado es 100.
Los valores de LIMIT demasiado grandes pueden ralentizar las búsquedas y normalmente indican un error de uso.

Para comprobar si una consulta SELECT usa un índice de similitud vectorial, puede anteponer `EXPLAIN indexes = 1` a la consulta.

Por ejemplo, la consulta

```sql
EXPLAIN indexes = 1
WITH [0.462, 0.084, ..., -0.110] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10;
```

puede devolver

```result
    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                      │
 2. │   Limit (preliminary LIMIT (without OFFSET))                                                    │
 3. │     Sorting (Sorting for ORDER BY)                                                              │
 4. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers))) │
 5. │         ReadFromMergeTree (default.tab)                                                         │
 6. │         Indexes:                                                                                │
 7. │           PrimaryKey                                                                            │
 8. │             Condition: true                                                                     │
 9. │             Parts: 1/1                                                                          │
10. │             Granules: 575/575                                                                   │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 10/575                                                                    │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

En este ejemplo, 1 millón de vectores del [conjunto de datos dbpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M), cada uno con una dimensión de 1536, se almacenan en 575 gránulos, es decir, 1,7 mil filas por gránulo.
La consulta solicita 10 vecinos y el índice de similitud vectorial encuentra esos 10 vecinos en 10 gránulos distintos.
Estos 10 gránulos se leerán durante la ejecución de la consulta.

Los índices de similitud vectorial se usan si la salida contiene `Skip`, así como el nombre y el tipo del índice vectorial (en el ejemplo, `idx` y `vector_similarity`).
En este caso, el índice de similitud vectorial descartó dos de cuatro gránulos, es decir, el 50 % de los datos.
Cuantos más gránulos se puedan descartar, más eficaz será el uso del índice.

:::tip
Para forzar el uso del índice, puedes ejecutar la consulta SELECT con la configuración [force&#95;data&#95;skipping&#95;indexes](../../../operations/settings/settings#force_data_skipping_indices) (indica el nombre del índice como valor de la configuración).
:::

**Posfiltrado y prefiltrado**

Los usuarios pueden especificar opcionalmente una cláusula `WHERE` con condiciones de filtrado adicionales para la consulta SELECT.
ClickHouse evaluará estas condiciones de filtrado mediante una estrategia de posfiltrado o prefiltrado.
En resumen, ambas estrategias determinan el orden en que se evalúan los filtros:

* Posfiltrado significa que primero se evalúa el índice de similitud vectorial y, después, ClickHouse evalúa los filtros adicionales especificados en la cláusula `WHERE`.
* Prefiltrado significa que el orden de evaluación de los filtros es el inverso.

Las estrategias tienen distintas ventajas e inconvenientes:

* El posfiltrado presenta el problema general de que puede devolver menos filas de las solicitadas en la cláusula `LIMIT <N>`. Esta situación se produce cuando una o más filas de resultado devueltas por el índice de similitud vectorial no cumplen los filtros adicionales.
* El prefiltrado sigue siendo, por lo general, un problema sin resolver. Algunas bases de datos vectoriales especializadas ofrecen algoritmos de prefiltrado, pero la mayoría de las bases de datos relacionales (incluida ClickHouse) recurren a una búsqueda exacta de vecinos; es decir, a un escaneo exhaustivo sin índice.

La estrategia utilizada depende de la condición de filtrado.

*Los filtros adicionales forman parte de la clave de partición*

Si la condición de filtrado adicional forma parte de la clave de partición, ClickHouse aplicará poda de particiones.
Por ejemplo, una tabla está particionada por rango según la columna `year` y se ejecuta la siguiente consulta:

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

ClickHouse descartará todas las particiones excepto la de 2025.

*Los filtros adicionales no pueden evaluarse mediante índices*

Si las condiciones de filtro adicionales no pueden evaluarse mediante índices (índice de clave primaria, índice de omisión), ClickHouse aplicará postfiltrado.

*Los filtros adicionales pueden evaluarse mediante el índice de clave primaria*

Si las condiciones de filtro adicionales pueden evaluarse mediante la [clave primaria](mergetree.md#primary-key) (es decir, forman un prefijo de la clave primaria) y

* la condición de filtro elimina al menos una fila dentro de una parte, ClickHouse recurrirá al prefiltrado para los rangos &quot;supervivientes&quot; dentro de la parte,
* la condición de filtro no elimina ninguna fila dentro de una parte, ClickHouse aplicará postfiltrado a la parte.

En casos de uso prácticos, este último supuesto es bastante poco probable.

*Los filtros adicionales pueden evaluarse mediante un índice de omisión*

Si las condiciones de filtro adicionales pueden evaluarse mediante [índices de omisión](mergetree.md#table_engine-mergetree-data_skipping-indexes) (índice minmax, índice set, etc.), ClickHouse aplica postfiltrado.
En esos casos, el índice de similitud vectorial se evalúa primero, ya que se espera que descarte la mayor cantidad de filas en comparación con otros índices de omisión.

Para un control más preciso del postfiltrado frente al prefiltrado, se pueden usar dos configuraciones:

La configuración [vector&#95;search&#95;filter&#95;strategy](../../../operations/settings/settings#vector_search_filter_strategy) (valor predeterminado: `auto`, que implementa las heurísticas anteriores) puede establecerse en `prefilter`.
Esto resulta útil para forzar el prefiltrado en casos en los que las condiciones de filtro adicionales son extremadamente selectivas.
Por ejemplo, la siguiente consulta puede beneficiarse del prefiltrado:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
```

Suponiendo que solo un número muy pequeño de libros cuesta menos de 2 dólares, el posfiltrado puede devolver cero filas, porque las 10 coincidencias principales devueltas por el índice vectorial podrían tener un precio superior a 2 dólares.
Al forzar el prefiltrado (añada `SETTINGS vector_search_filter_strategy = 'prefilter'` a la consulta), ClickHouse primero encuentra todos los libros con un precio inferior a 2 dólares y luego ejecuta una búsqueda vectorial por fuerza bruta sobre los libros encontrados.

Como enfoque alternativo para resolver el issue anterior, [vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier) (valor predeterminado: `1.0`; máximo: `1000.0`) puede configurarse con un valor &gt; `1.0` (por ejemplo, `2.0`).
El número de vecinos más cercanos recuperados del índice vectorial se multiplica por el valor de la configuración y, después, se aplica el filtro adicional sobre esas filas para devolver tantas filas como indique LIMIT.
Como ejemplo, podemos volver a ejecutar la consulta, pero con un multiplicador de `3.0`:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
SETTING vector_search_index_fetch_multiplier = 3.0;
```

ClickHouse recuperará 3.0 x 10 = 30 vecinos más cercanos del índice vectorial en cada parte y después evaluará los filtros adicionales.
Solo se devolverán los diez vecinos más cercanos.
Cabe señalar que establecer `vector_search_index_fetch_multiplier` puede mitigar el problema, pero en casos extremos (una condición WHERE muy selectiva), sigue siendo posible que se devuelvan menos de las N filas solicitadas.

**Reevaluación**

Los skip indexes en ClickHouse generalmente filtran a nivel de gránulo; es decir, una búsqueda en un skip index devuelve (internamente) una lista de gránulos que podrían coincidir, lo que reduce la cantidad de datos leídos en el escaneo posterior.
Esto funciona bien para los skip indexes en general, pero en el caso de los índices de similitud vectorial, crea un &quot;desajuste de granularidad&quot;.
En más detalle, el índice de similitud vectorial determina los números de fila de los N vectores más similares para un vector de referencia dado, pero luego necesita extrapolar esos números de fila a números de gránulo.
A continuación, ClickHouse cargará estos gránulos desde disco y repetirá el cálculo de distancias para todos los vectores de esos gránulos.
Este paso se denomina reevaluación y, aunque teóricamente puede mejorar la precisión —recuerde que el índice de similitud vectorial solo devuelve un resultado *aproximado*—, obviamente no es óptimo en términos de rendimiento.

Por lo tanto, ClickHouse proporciona una optimización que desactiva la reevaluación y devuelve directamente desde el índice los vectores más similares y sus distancias.
La optimización está habilitada de forma predeterminada; consulte la configuración [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring).
A grandes rasgos, funciona así: ClickHouse pone a disposición los vectores más similares y sus distancias como una columna virtual `_distances`.
Para verlo, ejecute una consulta de búsqueda vectorial con `EXPLAIN header = 1`:

```sql
EXPLAIN header = 1
WITH [0., 2.] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
```

```result
Query id: a2a9d0c8-a525-45c1-96ca-c5a11fa66f47

    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                              │
 2. │ Header: id Int32                                                                                        │
 3. │   Limit (preliminary LIMIT (without OFFSET))                                                            │
 4. │   Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64     │
 5. │           __table1.id Int32                                                                             │
 6. │     Sorting (Sorting for ORDER BY)                                                                      │
 7. │     Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64   │
 8. │             __table1.id Int32                                                                           │
 9. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers)))         │
10. │       Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64 │
11. │               __table1.id Int32                                                                         │
12. │         ReadFromMergeTree (default.tab)                                                                 │
13. │         Header: id Int32                                                                                │
14. │                 _distance Float32                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

:::note
Una consulta ejecutada sin reevaluación (`vector_search_with_rescoring = 0`) y con las réplicas paralelas habilitadas puede volver a usar reevaluación.
:::

<div id="performance-tuning">
  #### Optimización del rendimiento
</div>

**Optimización de la compresión**

En prácticamente todos los casos de uso, los vectores de la columna subyacente son densos y no se comprimen bien.
Como resultado, la [compresión](/es/sql-reference/statements/create/table.md#column_compression_codec) ralentiza las inserciones y las lecturas en la columna vectorial.
Por lo tanto, recomendamos desactivar la compresión.
Para hacerlo, especifique `CODEC(NONE)` para la columna vectorial de esta manera:

```sql
CREATE TABLE tab(id Int32, vec Array(Float32) CODEC(NONE), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
```

**Ajuste de la creación de índices**

El ciclo de vida de los índices de similitud vectorial está ligado al ciclo de vida de las partes.
En otras palabras, cada vez que se crea una nueva parte con un índice de similitud vectorial definido, también se crea el índice.
Esto suele ocurrir cuando se [insertan](https://clickhouse.com/docs/guides/inserting-data) datos o durante las [fusiones](https://clickhouse.com/docs/merges).
Por desgracia, HNSW es conocido por sus largos tiempos de creación de índices, lo que puede ralentizar significativamente las inserciones y las fusiones.
Lo ideal es usar índices de similitud vectorial solo si los datos son inmutables o cambian rara vez.

Para acelerar la creación de índices, se pueden usar las siguientes técnicas:

En primer lugar, la creación de índices se puede paralelizar.
El número máximo de hilos para la creación de índices puede configurarse mediante la configuración del servidor [max&#95;build&#95;vector&#95;similarity&#95;index&#95;thread&#95;pool&#95;size](/es/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size).
Para un rendimiento óptimo, este valor debe configurarse según el número de núcleos de CPU.

En segundo lugar, para acelerar las sentencias INSERT, los usuarios pueden deshabilitar la creación de índices de omisión en las partes recién insertadas mediante la configuración de sesión [materialize&#95;skip&#95;indexes&#95;on&#95;insert](../../../operations/settings/settings.md#materialize_skip_indexes_on_insert).
Las consultas SELECT sobre esas partes recurrirán a la búsqueda exacta.
Como las partes insertadas suelen ser pequeñas en comparación con el tamaño total de la tabla, se espera que el impacto en el rendimiento sea insignificante.

En tercer lugar, para acelerar las fusiones, los usuarios pueden deshabilitar la creación de índices de omisión en las partes fusionadas mediante la configuración de sesión [materialize&#95;skip&#95;indexes&#95;on&#95;merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge).
Esto, junto con la sentencia [ALTER TABLE [...] MATERIALIZE INDEX [...]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index), proporciona un control explícito sobre el ciclo de vida de los índices de similitud vectorial.
Por ejemplo, la creación de índices puede posponerse hasta que se hayan ingestado todos los datos o hasta un período de baja carga del sistema, como el fin de semana.

**Ajuste del uso de índices**

Las consultas SELECT necesitan cargar los índices de similitud vectorial en la memoria principal para poder usarlos.
Para evitar que el mismo índice de similitud vectorial se cargue repetidamente en la memoria principal, ClickHouse proporciona una caché en memoria dedicada para estos índices.
Cuanto mayor sea esta caché, menos cargas innecesarias se producirán.
El tamaño máximo de la caché puede configurarse mediante la configuración del servidor [vector&#95;similarity&#95;index&#95;cache&#95;size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size).
De forma predeterminada, la caché puede crecer hasta 5 GB.

Los siguientes mensajes de registro (`system.text_log`) indican que se está cargando el índice de similitud vectorial.
Si estos mensajes aparecen repetidamente en distintas consultas de búsqueda vectorial, esto indica que el tamaño de la caché es demasiado pequeño.

```text
2026-02-03 07:39:10.351635 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Start loading vector similarity index

<...>

2026-02-03 07:40:25.217603 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Loaded vector similarity index: max_level = 2, connectivity = 64, size = 1808111, capacity = 1808111, memory_usage = 8.00 GiB, bytes_per_vector = 4096, scalar_words = 1024, nodes = 1808111, edges = 51356964, max_edges = 233395072
```

:::note
La caché del índice de similitud vectorial almacena gránulos del índice vectorial.
Si los gránulos individuales del índice vectorial son más grandes que el tamaño de la caché, no se almacenarán en ella.
Por lo tanto, asegúrese de calcular el tamaño del índice vectorial (según la fórmula de &quot;Estimación del consumo de almacenamiento y memoria&quot; o [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices)) y de dimensionar la caché en consecuencia.
:::

*Reiteramos que comprobar y, si es necesario, aumentar la caché del índice vectorial debe ser el primer paso al investigar consultas lentas de búsqueda vectorial.*

El tamaño actual de la caché del índice de similitud vectorial se muestra en [system.metrics](../../../operations/system-tables/metrics.md):

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheBytes'
```

Los aciertos y fallos de la caché para una consulta con un determinado identificador de consulta se pueden obtener de [system.query&#95;log](../../../operations/system-tables/query_log.md):

```sql
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VectorSimilarityIndexCacheHits'], ProfileEvents['VectorSimilarityIndexCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '<...>'
ORDER BY event_time_microseconds;
```

Para casos de uso en producción, recomendamos que la caché tenga un tamaño lo bastante grande como para que todos los índices vectoriales permanezcan en memoria en todo momento.

**Ajuste de la cuantización**

La [cuantización](https://huggingface.co/blog/embedding-quantization) es una técnica para reducir la huella de memoria de los vectores y el coste computacional de crear y recorrer índices vectoriales.
Los índices vectoriales de ClickHouse admiten las siguientes opciones de cuantización:

| Cuantización          | Nombre                        | Almacenamiento por dimensión |
| --------------------- | ----------------------------- | ---------------------------- |
| f32                   | Precisión simple              | 4 bytes                      |
| f16                   | Media precisión               | 2 bytes                      |
| bf16 (predeterminada) | Media precisión (brain float) | 2 bytes                      |
| i8                    | Cuarta precisión              | 1 byte                       |
| b1                    | Binaria                       | 1 bit                        |

La cuantización reduce la precisión de las búsquedas vectoriales en comparación con la búsqueda sobre los valores originales de coma flotante con precisión completa (`f32`).
Sin embargo, en la mayoría de los conjuntos de datos, la cuantización brain float de media precisión (`bf16`) produce una pérdida de precisión insignificante; por lo tanto, los índices de similitud vectorial utilizan esta técnica de cuantización de forma predeterminada.
La cuantización de cuarta precisión (`i8`) y la binaria (`b1`) provocan una pérdida de precisión apreciable en las búsquedas vectoriales.
Recomendamos ambas cuantizaciones solo si el tamaño del índice de similitud vectorial es significativamente mayor que la DRAM disponible.
En este caso, también sugerimos habilitar el rescoring ([vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier), [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring)) para mejorar la exactitud.
La cuantización binaria solo se recomienda para 1) embeddings normalizados (es decir, longitud del vector = 1; los modelos de OpenAI suelen estar normalizados) y 2) si se usa la distancia de coseno como función de distancia.
La cuantización binaria utiliza internamente la distancia de Hamming para construir y recorrer el grafo de proximidad.
El paso de rescoring utiliza los vectores originales de precisión completa almacenados en la tabla para identificar los vecinos más cercanos mediante la distancia de coseno.

**Ajuste de la transferencia de datos**

El vector de referencia en una consulta de búsqueda vectorial lo proporciona el usuario y, por lo general, se obtiene realizando una llamada a un modelo de lenguaje grande (LLM).
El código típico de Python que ejecuta una búsqueda vectorial en ClickHouse podría verse así

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'search_v': search_v}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, %(search_v)s)
    LIMIT 10",
    parameters = params)
```

Los vectores de embedding (`search_v` en el fragmento anterior) pueden tener una dimensión muy grande.
Por ejemplo, OpenAI ofrece modelos que generan vectores de embeddings con 1536 o incluso 3072 dimensiones.
En el código anterior, el driver de Python de ClickHouse sustituye el vector de embedding por una cadena legible para humanos y, a continuación, envía la consulta SELECT íntegramente como una cadena.
Suponiendo que el vector de embedding consta de 1536 valores de punto flotante de precisión simple, la cadena enviada alcanza una longitud de 20 kB.
Esto provoca un alto uso de CPU para la tokenización, el parsing y la realización de miles de conversiones de cadena a flotante.
Además, se requiere una cantidad considerable de espacio en el archivo de registro del servidor de ClickHouse, lo que también provoca un crecimiento excesivo de `system.query_log`.

Tenga en cuenta que la mayoría de los modelos LLM devuelven un vector de embedding como una lista o un array de NumPy de flotantes nativos.
Por lo tanto, recomendamos que las aplicaciones de Python enlacen el parámetro del vector de referencia en forma binaria usando el siguiente estilo:

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'$search_v_binary$': np.array(search_v, dtype=np.float32).tobytes()}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, reinterpret($search_v_binary$, 'Array(Float32)'))
    LIMIT 10"
    parameters = params)
```

En el ejemplo, el vector de referencia se envía tal cual en formato binario y se reinterpreta como un array de números de coma flotante en el servidor.
Esto ahorra tiempo de CPU en el servidor y evita aumentar el tamaño de los registros del servidor y de `system.query_log`.

<div id="administration">
  #### Administración y monitorización
</div>

El tamaño en disco de los índices de similitud vectorial puede obtenerse en [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices):

```sql
SELECT database, table, name, formatReadableSize(data_compressed_bytes)
FROM system.data_skipping_indices
WHERE type = 'vector_similarity';
```

Salida de ejemplo:

```result
┌─database─┬─table─┬─name─┬─formatReadab⋯ssed_bytes)─┐
│ default  │ tab   │ idx  │ 348.00 MB                │
└──────────┴───────┴──────┴──────────────────────────┘
```

<div id="differences-to-regular-skipping-indexes">
  #### Diferencias con los índices de omisión normales
</div>

Al igual que todos los [índices de omisión](/es/optimize/skipping-indexes) normales, los índices de similitud vectorial se construyen sobre gránulos, y cada bloque indexado consta de `GRANULARITY = [N]` gránulos (`[N]` = 1 de forma predeterminada para los índices de omisión normales).
Por ejemplo, si la granularidad del índice primario de la tabla es 8192 (configuración `index_granularity = 8192`) y `GRANULARITY = 2`, entonces cada bloque indexado contendrá 16384 filas.
Sin embargo, las estructuras de datos y los algoritmos para la búsqueda aproximada de vecinos son inherentemente orientados a filas.
Almacenan una representación compacta de un conjunto de filas y también devuelven filas para consultas de búsqueda vectorial.
Esto da lugar a algunas diferencias bastante poco intuitivas en la forma en que se comportan los índices de similitud vectorial en comparación con los índices de omisión normales.

Cuando un usuario define un índice de similitud vectorial sobre una columna, ClickHouse crea internamente un &quot;subíndice&quot; de similitud vectorial para cada bloque del índice.
El subíndice es &quot;local&quot; en el sentido de que solo conoce las filas del bloque de índice al que pertenece.
En el ejemplo anterior, y suponiendo que una columna tiene 65536 filas, obtenemos cuatro bloques de índice (que abarcan ocho gránulos) y un subíndice de similitud vectorial para cada bloque de índice.
En teoría, un subíndice puede devolver directamente las filas con los N puntos más cercanos dentro de su bloque de índice.
Sin embargo, dado que ClickHouse carga los datos del disco en memoria con granularidad de gránulo, los subíndices extrapolan las filas coincidentes a esa misma granularidad.
Esto es distinto de los índices de omisión normales, que omiten datos con la granularidad de los bloques de índice.

El parámetro `GRANULARITY` determina cuántos subíndices de similitud vectorial se crean.
Los valores más altos de `GRANULARITY` implican menos subíndices de similitud vectorial, pero de mayor tamaño, hasta el punto en que una columna (o una data part de una columna) tiene un solo subíndice.
En ese caso, el subíndice tiene una vista &quot;global&quot; de todas las filas de la columna y puede devolver directamente todos los gránulos de la columna (part) que contienen filas relevantes (hay como máximo `LIMIT [N]` gránulos de ese tipo).
En un segundo paso, ClickHouse cargará estos gránulos e identificará las filas realmente mejores realizando un cálculo de distancia por fuerza bruta sobre todas las filas de esos gránulos.
Con un valor pequeño de `GRANULARITY`, cada subíndice devuelve hasta `LIMIT N` gránulos.
Como resultado, hay que cargar más gránulos y aplicarles posfiltrado.
Tenga en cuenta que la precisión de la búsqueda es igual de buena en ambos casos; solo cambia el rendimiento del procesamiento.
En general, se recomienda usar un `GRANULARITY` alto para los índices de similitud vectorial y recurrir a valores más bajos de `GRANULARITY` solo en caso de problemas, como un consumo excesivo de memoria de las estructuras de similitud vectorial.
Si no se especifica `GRANULARITY` para los índices de similitud vectorial, el valor predeterminado es 100 millones.

<div id="approximate-nearest-neighbor-search-example">
  #### Ejemplo
</div>

Consultas:

```sql title="Query"
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

```result title="Response"
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

Otros conjuntos de datos de ejemplo que usan búsqueda vectorial aproximada:

* [LAION-400M](../../../getting-started/example-datasets/laion-400m-dataset)
* [LAION-5B](../../../getting-started/example-datasets/laion-5b-dataset)
* [dbpedia](../../../getting-started/example-datasets/dbpedia-dataset)
* [hackernews](../../../getting-started/example-datasets/hackernews-vector-search-dataset)

<div id="approximate-nearest-neighbor-search-qbit">
  ### Quantized Bit (QBit)
</div>

Un enfoque habitual para acelerar la búsqueda vectorial exacta es usar un [tipo de dato float](../../../sql-reference/data-types/float.md) de menor precisión.
Por ejemplo, si los vectores se almacenan como `Array(BFloat16)` en lugar de `Array(Float32)`, el tamaño de los datos se reduce a la mitad y cabe esperar que el tiempo de ejecución de las consultas disminuya proporcionalmente.
Este método se conoce como cuantización. Aunque acelera el cálculo, puede reducir la precisión de los resultados pese a realizar un escaneo exhaustivo de todos los vectores.

Con la cuantización tradicional, se pierde precisión tanto durante la búsqueda como al almacenar los datos. En el ejemplo anterior, almacenaríamos `BFloat16` en lugar de `Float32`, lo que significa que después nunca podríamos realizar una búsqueda más precisa, aunque quisiéramos. Un enfoque alternativo es almacenar dos copias de los datos: una cuantizada y otra de precisión completa. Aunque esto funciona, requiere almacenamiento redundante. Considera un caso en el que tenemos `Float64` como datos originales y queremos ejecutar búsquedas con distinta precisión (16 bits, 32 bits o 64 bits completos). Necesitaríamos almacenar tres copias independientes de los datos.

ClickHouse ofrece el tipo de dato Quantized Bit (`QBit`), que resuelve estas limitaciones de la siguiente manera:

1. Almacena los datos originales con precisión completa.
2. Permite especificar la precisión de cuantización en tiempo de consulta.

Esto se consigue almacenando los datos en un formato agrupado por bits (es decir, todos los bits en la posición i de todos los vectores se almacenan juntos), lo que permite leer solo el nivel de precisión solicitado. Así, obtienes las ventajas de velocidad de la cuantización, con menos IO y menos cálculo, sin dejar de tener disponibles todos los datos originales cuando los necesites. Cuando se selecciona la precisión máxima, la búsqueda pasa a ser exacta.

Para declarar una columna de tipo `QBit`, usa la siguiente sintaxis:

```sql
column_name QBit(element_type, dimension[, stride])
```

Donde:

* `element_type` – el tipo de cada elemento del vector. Los tipos compatibles son `Int8`, `BFloat16`, `Float32` y `Float64`
* `dimension` – el número de elementos de cada vector
* `stride` – opcional. Un divisor de `dimension` que divide las dimensiones en `dimension / stride` grupos contiguos almacenados en streams separados, de modo que una búsqueda solo en las dimensiones iniciales lea menos streams (útil para embeddings Matryoshka). El valor predeterminado es `dimension`, en cuyo caso el tipo es idéntico byte por byte a un `QBit` sin `stride`. Consulta la [página del tipo de dato `QBit`](/es/sql-reference/data-types/qbit) para obtener más información.

<div id="qbit-create">
  #### Crear una tabla `QBit` y añadir datos
</div>

```sql
CREATE TABLE fruit_animal (
    word String,
    vec QBit(Float64, 5)
) ENGINE = MergeTree
ORDER BY word;

INSERT INTO fruit_animal VALUES
    ('apple', [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
    ('banana', [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
    ('orange', [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
    ('dog', [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
    ('cat', [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
    ('horse', [-0.61435682, 0.48542571, 1.21091247, -0.62530446, -1.33082533]);
```

<div id="qbit-search">
  #### Búsqueda vectorial con `QBit`
</div>

Busquemos los vecinos más cercanos de un vector que representa la palabra &#39;lemon&#39; mediante la distancia L2. El tercer parámetro de la función de distancia especifica la precisión en bits: cuanto mayor sea el valor, mayor será la exactitud, pero también el cómputo requerido.

Puedes encontrar todas las funciones de distancia disponibles para `QBit` [aquí](../../../sql-reference/data-types/qbit.md#vector-search-functions).

**Búsqueda con precisión completa (64 bits):**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 64) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬────────────distance─┐
1. │ apple  │ 0.14639757188169716 │
2. │ banana │   1.998961369007679 │
3. │ orange │   2.039041552613732 │
4. │ cat    │   2.752802631487914 │
5. │ horse  │  2.7555776805484813 │
6. │ dog    │   3.382295083120104 │
   └────────┴─────────────────────┘
```

**Búsqueda con precisión reducida:**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 12) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬───────────distance─┐
1. │ apple  │  0.757668703053566 │
2. │ orange │ 1.5499475034938677 │
3. │ banana │ 1.6168396735102937 │
4. │ cat    │  2.429752230904804 │
5. │ horse  │  2.524650475528617 │
6. │ dog    │   3.17766975527459 │
   └────────┴────────────────────┘
```

Tenga en cuenta que, con cuantización de 12 bits, obtenemos una buena aproximación de las distancias con una ejecución de las consultas más rápida. El orden relativo se mantiene en gran medida, y &#39;apple&#39; sigue siendo la coincidencia más cercana.

<div id="qbit-performance">
  #### Consideraciones de rendimiento
</div>

La mejora de rendimiento de `QBit` proviene de la reducción de las operaciones de E/S, ya que, al usar una precisión menor, es necesario leer menos datos del almacenamiento. Además, cuando `QBit` contiene datos `Float32`, si el parámetro de precisión es 16 o menos, también se obtienen beneficios adicionales gracias a la reducción del cálculo. El parámetro de precisión controla directamente el equilibrio entre exactitud y velocidad:

* **Mayor precisión** (más cercana al ancho original de los datos): resultados más precisos, consultas más lentas
* **Menor precisión**: consultas más rápidas con resultados aproximados y menor uso de memoria

<div id="references">
  ### Referencias
</div>

Blogs:

* [Búsqueda vectorial con ClickHouse - Parte 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
* [Búsqueda vectorial con ClickHouse - Parte 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)
* [Creamos un motor de búsqueda vectorial que te permite elegir la precisión en tiempo de consulta](https://clickhouse.com/blog/qbit-vector-search)