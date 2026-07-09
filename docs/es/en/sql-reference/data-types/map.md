---
description: 'Documentación del tipo de dato Map en ClickHouse'
sidebar_label: 'Map(K, V)'
sidebar_position: 36
slug: /sql-reference/data-types/map
title: 'Map(K, V)'
doc_type: 'referencia'
---

El tipo de dato `Map(K, V)` almacena pares clave-valor.

A diferencia de otras bases de datos, los mapas no requieren claves únicas en ClickHouse; es decir, un mapa puede contener dos elementos con la misma clave.
(La razón es que los mapas se implementan internamente como `Array(Tuple(K, V))`.)

Puede usar la sintaxis `m[k]` para obtener el valor de la clave `k` del mapa `m`.
Además, `m[k]` recorre el mapa; es decir, el tiempo de ejecución de esta operación es lineal con respecto al tamaño del mapa.

**Parámetros**

* `K` — El tipo de las claves de Map. Cualquier tipo, excepto [Nullable](../../sql-reference/data-types/nullable.md) y [LowCardinality](../../sql-reference/data-types/lowcardinality.md) anidado con tipos [Nullable](../../sql-reference/data-types/nullable.md).
* `V` — El tipo de los valores de Map. Cualquier tipo.

**Ejemplos**

Cree una tabla con una columna de tipo Map:

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

Para seleccionar los valores de `key2`:

```sql title="Query"
SELECT m['key2'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

Si la clave solicitada `k` no está en el mapa, `m[k]` devuelve el valor predeterminado del tipo de valor, por ejemplo, `0` para los tipos enteros y `''` para los tipos de cadena.
Para comprobar si una clave existe en un mapa, puede usar la función [mapContains](/es/sql-reference/functions/tuple-map-functions#mapContainsKey).

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

<div id="converting-tuple-to-map">
  ## Convertir Tuple a Map
</div>

Los valores de tipo `Tuple()` pueden convertirse a valores de tipo `Map()` mediante la función [CAST](/es/sql-reference/functions/type-conversion-functions#CAST):

**Ejemplo**

```sql title="Query"
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

```text title="Response"
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

<div id="reading-subcolumns-of-map">
  ## Lectura de subcolumnas de Map
</div>

Para evitar leer el mapa completo, en algunos casos puedes usar las subcolumnas `keys` y `values`.

**Ejemplo**

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

```text title="Response"
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

<div id="bucketed-map-serialization">
  ## Serialización en buckets de Map en MergeTree
</div>

De forma predeterminada, una columna `Map` en MergeTree se almacena como un único flujo `Array(Tuple(K, V))`.
Leer una sola clave con `m['key']` requiere recorrer toda la columna —todos los pares clave-valor de cada fila— incluso si solo se necesita una clave.
En mapas con muchas claves distintas, esto se convierte en un cuello de botella.

La serialización en buckets (`with_buckets`) divide los pares clave-valor en varios subflujos (buckets) independientes aplicando un hash a la clave.
Cuando una consulta accede a `m['key']`, solo se lee del disco el bucket que contiene esa clave, omitiendo todos los demás buckets.

<div id="enabling-bucketed-serialization">
  ### Activar la serialización en buckets
</div>

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

Para no ralentizar las inserciones, puedes mantener la serialización `basic` para las partes de nivel cero (creadas durante `INSERT`) y usar `with_buckets` solo para las partes fusionadas:

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

<div id="how-it-works">
  ### Cómo funciona
</div>

Cuando una parte de datos se escribe con la serialización `with_buckets`:

1. El número medio de claves por fila se calcula a partir de las estadísticas del bloque.
2. El número de buckets se determina según la estrategia configurada (consulta [Settings](#bucketed-map-settings)).
3. Cada par clave-valor se asigna a un bucket aplicando una función hash a la clave: `bucket = hash(key) % num_buckets`.
4. Cada bucket se almacena como un subflujo independiente con sus propias claves, valores y desplazamientos.
5. Un flujo de metadatos `buckets_info` registra la cantidad de buckets y las estadísticas.

Cuando una consulta lee una clave específica (`m['key']`), el optimizador reescribe la expresión como una subcolumna de clave (`m.key_<serialized_key>`).
La capa de serialización calcula a qué bucket pertenece la clave solicitada y lee únicamente ese bucket del disco.

Cuando se lee el mapa completo (p. ej., `SELECT m`), se leen todos los buckets y se reconstruyen en el mapa original. Esto es más lento que la serialización `basic` debido a la sobrecarga de leer y fusionar varios subflujos.

:::note
El orden de las claves dentro de un valor de mapa puede diferir del orden de inserción original al usar la serialización `with_buckets`. Las claves se distribuyen entre buckets mediante hash y se reconstruyen en el orden de los buckets, no en el orden de inserción. Con la serialización `basic`, se conserva el orden de las claves de los mapas insertados.
:::

La cantidad de buckets puede variar entre partes. Cuando se fusionan partes con distintas cantidades de buckets, la cantidad de buckets de la nueva parte se recalcula a partir de las estadísticas fusionadas. Las partes con serialización `basic` y `with_buckets` pueden coexistir en la misma tabla y se fusionan de forma transparente.

<div id="bucketed-map-settings">
  ### Ajustes
</div>

| Ajuste                                           | Predeterminado | Descripción                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------ | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `map_serialization_version`                      | `basic`        | Formato de serialización para las columnas `Map`. `basic` almacena los datos en un único flujo de array. `with_buckets` divide las claves en buckets para acelerar las lecturas por clave individual.                                                                                              |
| `map_serialization_version_for_zero_level_parts` | `basic`        | Formato de serialización para las partes de nivel cero (creadas por `INSERT`). Permite mantener `basic` para las inserciones y evitar la sobrecarga de escritura, mientras que las partes fusionadas usan `with_buckets`.                                                                          |
| `max_buckets_in_map`                             | `32`           | Límite superior del número de buckets. La cantidad real depende de `map_buckets_strategy`. El valor máximo permitido es 256.                                                                                                                                                                       |
| `map_buckets_strategy`                           | `sqrt`         | Estrategia para calcular el número de buckets a partir del tamaño medio del mapa: `constant` — usar siempre `max_buckets_in_map`; `sqrt` — usar `round(coefficient * sqrt(avg_size))`; `linear` — usar `round(coefficient * avg_size)`. El resultado se limita al rango `[1, max_buckets_in_map]`. |
| `map_buckets_coefficient`                        | `1.0`          | Multiplicador para las estrategias `sqrt` y `linear`. Se ignora cuando la estrategia es `constant`.                                                                                                                                                                                                |
| `map_buckets_min_avg_size`                       | `32`           | Número medio mínimo de claves por fila para habilitar el uso de buckets. Si la media está por debajo de este umbral, se usa un solo bucket independientemente de los demás ajustes. Establézcalo en `0` para desactivar el umbral.                                                                 |

<div id="performance-trade-offs">
  ### Compromisos de rendimiento
</div>

La siguiente tabla resume el impacto en el rendimiento de `with_buckets` en comparación con la serialización `basic` para distintos tamaños de mapa (de 10 a 10.000 claves por fila). El número de buckets se determinó mediante la estrategia `sqrt`, con un máximo de 32. Las cifras exactas dependen de los tipos de clave/valor, la distribución de los datos y el hardware.

| Operación                                      | 10 claves           | 100 claves          | 1.000 claves        | 10.000 claves       | Notas                                                                                                                                                                                                                                |
| ---------------------------------------------- | ------------------- | ------------------- | ------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Búsqueda de una sola clave** (`m['key']`)    | 1,6–3,2x más rápido | 4,5–7,7x más rápido | 16–39x más rápido   | 21–49x más rápido   | Lee solo un bucket en lugar de toda la columna.                                                                                                                                                                                      |
| **5 búsquedas de claves**                      | ~1x                 | 1,5–3,1x más rápido | 2,9–8,3x más rápido | 4,5–6,7x más rápido | Cada clave lee su propio bucket; algunos buckets pueden solaparse.                                                                                                                                                                   |
| **PREWHERE** (`SELECT m WHERE m['key'] = ...`) | 1,5–3,0x más rápido | 2,9–7,3x más rápido | 5,3–31x más rápido  | 20–45x más rápido   | El filtro PREWHERE lee solo un bucket; el mapa completo solo se lee para las filas coincidentes. La mejora de velocidad depende de la selectividad: cuanto menos gránulos coincidan, menor será la E/S de lectura del mapa completo. |
| **Escaneo completo del mapa** (`SELECT m`)     | ~2x más lento       | ~2x más lento       | ~2x más lento       | ~2x más lento       | Debe leer y volver a ensamblar todos los buckets.                                                                                                                                                                                    |
| **INSERT**                                     | 1,5–2,5x más lento  | 1,5–2,5x más lento  | 1,5–2,5x más lento  | 1,5–2,5x más lento  | Sobrecarga de aplicar hash a las claves y escribir en varios subflujos.                                                                                                                                                              |

<div id="recommendations">
  ### Recomendaciones
</div>

* **Mapas pequeños (&lt; 32 claves de media):** Mantén la serialización `basic`. La sobrecarga de usar buckets no se justifica para mapas pequeños. El valor predeterminado `map_buckets_min_avg_size = 32` aplica esto automáticamente.
* **Mapas medianos (32–100 claves):** Usa `with_buckets` con la estrategia `sqrt` si las consultas acceden con frecuencia a claves individuales. La aceleración es de 4–8x para búsquedas de una sola clave.
* **Mapas grandes (100+ claves):** Usa `with_buckets`. Las búsquedas de una sola clave son 16–49x más rápidas. Considera `map_serialization_version_for_zero_level_parts = 'basic'` para mantener la velocidad de inserción cerca del valor de referencia.
* **Los escaneos completos de mapas dominan la carga de trabajo:** Mantén `basic`. La serialización en buckets añade una sobrecarga de ~2x para los escaneos completos.
* **Carga de trabajo mixta (algunas búsquedas de claves y algunos escaneos completos):** Usa `with_buckets` con las partes de nivel cero configuradas en `basic`. La optimización `PREWHERE` lee solo el bucket relevante para el filtro y luego lee el mapa completo solo para las filas coincidentes, lo que ofrece una aceleración neta significativa.

<div id="map-alternatives">
  ### Enfoques alternativos
</div>

Si la serialización en buckets de `Map` no se ajusta a tu caso de uso, existen dos enfoques alternativos para mejorar el rendimiento del acceso por clave:

<div id="using-the-json-data-type">
  #### Uso del tipo de datos JSON
</div>

El tipo de datos [JSON](/es/sql-reference/data-types/newjson) almacena cada ruta frecuente como una subcolumna dinámica independiente. Las rutas que superan el límite de `max_dynamic_paths` pasan a una [estructura de datos compartidos](/es/sql-reference/data-types/newjson#shared-data-structure), que puede usar la serialización `advanced` para optimizar la lectura de una sola ruta. Consulta la [entrada del blog](https://clickhouse.com/blog/json-data-type-gets-even-better) para obtener una descripción detallada de la serialización `advanced`.

| Aspecto                               | `Map` con buckets                                                                                    | `JSON`                                                                                                                                                                                                 |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Lectura de una sola clave             | Lee un bucket (puede contener otras claves). Todos los pares clave-valor del bucket se deserializan. | Las rutas frecuentes se leen directamente desde subcolumnas dinámicas. Las rutas poco frecuentes pasan a datos compartidos; con la serialización `advanced`, solo se leen los datos de la ruta exacta. |
| Tipos de valor                        | Todos los valores comparten el mismo tipo `V`                                                        | Cada ruta puede tener su propio tipo. Las rutas sin una indicación de tipo usan `Dynamic`.                                                                                                             |
| Compatibilidad con índices de omisión | Funciona con algunos tipos de índice creados sobre `mapKeys`/`mapValues`                             | Los índices de omisión solo pueden crearse sobre subcolumnas de rutas específicas, no sobre todas las rutas o valores a la vez.                                                                        |
| Lectura de columna completa           | ~2x más lenta que `basic` debido al reensamblado de buckets                                          | Sobrecarga derivada de la codificación del tipo `Dynamic` y de la reconstrucción de rutas.                                                                                                             |
| Sobrecarga de almacenamiento          | Metadatos adicionales mínimos                                                                        | Mayor debido a la codificación del tipo `Dynamic`, al almacenamiento de los nombres de las rutas y a los metadatos adicionales de la serialización `advanced`.                                         |
| Flexibilidad del esquema              | Tipos de clave y valor fijos al crear la tabla                                                       | Totalmente dinámica: las claves y los tipos de valor pueden variar en cada fila. Se pueden declarar indicaciones de rutas tipadas para rutas conocidas.                                                |

Usa `JSON` cuando distintas claves necesiten distintos tipos de valor, cuando el conjunto de claves varíe significativamente entre filas, o cuando las claves a las que se accede con frecuencia se conozcan de antemano y puedan declararse como rutas tipadas para acceder directamente a sus subcolumnas.

<div id="manual-sharding-into-multiple-map-columns">
  #### Segmentación manual en varias columnas Map
</div>

Puede dividir manualmente un único `Map` en varias columnas en función del hash de la clave, a nivel de la aplicación:

```sql
CREATE TABLE tab (
    id UInt64,
    m0 Map(String, UInt64),
    m1 Map(String, UInt64),
    m2 Map(String, UInt64),
    m3 Map(String, UInt64)
) ENGINE = MergeTree ORDER BY id;
```

Durante la inserción, dirige cada par clave-valor a la columna `m{hash(key) % 4}`. Durante las consultas, lee de la columna específica: `m{hash('target_key') % 4}['target_key']`.

| Aspecto                 | `Map` con buckets                                               | Segmentación manual                                                                           |
| ----------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Facilidad de uso        | Transparente — lo gestiona el engine de almacenamiento          | Requiere lógica de enrutamiento a nivel de la aplicación para inserts y selects               |
| Vertical merge          | No compatible — todos los buckets pertenecen a una sola columna | Compatible — cada columna `Map` es una columna independiente y puede fusionarse verticalmente |
| Cambios de esquema      | El número de buckets se adapta automáticamente por parte        | Cambiar el número de segmentos requiere reescribir los datos o añadir columnas nuevas         |
| Sintaxis de consulta    | `m['key']` funciona directamente                                | Hay que calcular la columna correcta: `m0['key']`, `m1['key']`, etc.                          |
| Granularidad de buckets | Por parte, se adapta a las estadísticas de los datos            | Fija al crear la tabla                                                                        |

La segmentación manual resulta útil cuando los vertical merges son importantes para reducir el uso de memoria durante los merges de tablas con muchas columnas, o cuando el número de segmentos debe quedar fijo y controlarse explícitamente. Para la mayoría de los casos de uso, la serialización automática en buckets es más simple y suficiente.

**Véase también**

* función [map()](/es/sql-reference/functions/tuple-map-functions#map)
* función [CAST()](/es/sql-reference/functions/type-conversion-functions#CAST)
* [combinador -Map para el tipo de dato Map](../aggregate-functions/combinators.md#-map)

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo crear una solución de observabilidad con ClickHouse - Parte 2 - Trazas](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)