---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: arrayJoin
---

# arrayJoin función {#functions_arrayjoin}

Esta es una función muy inusual.

Las funciones normales no cambian un conjunto de filas, sino que simplemente cambian los valores en cada fila (mapa).
Las funciones agregadas comprimen un conjunto de filas (doblar o reducir).
El ‘arrayJoin’ función toma cada fila y genera un conjunto de filas (desplegar).

Esta función toma una matriz como argumento y propaga la fila de origen a varias filas para el número de elementos de la matriz.
Todos los valores de las columnas simplemente se copian, excepto los valores de la columna donde se aplica esta función; se reemplaza con el valor de matriz correspondiente.

Una consulta puede usar múltiples `arrayJoin` función. En este caso, la transformación se realiza varias veces.

Tenga en cuenta la sintaxis ARRAY JOIN en la consulta SELECT, que proporciona posibilidades más amplias.

Ejemplo:

``` sql
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

``` text
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/array_join/) <!--hide-->
