---
description: 'Documentación de la función arrayJoin'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'función arrayJoin'
doc_type: 'reference'
---

Esta es una función muy inusual.

Las funciones regulares no cambian un conjunto de filas, sino solo los valores de cada fila (mapeo).
Las funciones de agregación condensan un conjunto de filas (fold o reduce).
La función `arrayJoin` toma cada fila y genera un conjunto de filas (expansión).

Esta función toma un array como argumento y replica la fila de origen en múltiples filas, una por cada elemento del array.
Todos los valores de las columnas se copian tal cual, excepto los valores de la columna en la que se aplica esta función; se sustituyen por el valor correspondiente del array.

:::note
Si el array está vacío, `arrayJoin` no produce ninguna fila.
Para devolver una sola fila que contenga el valor predeterminado del tipo de array, puede envolverlo con [emptyArrayToSingle](./array-functions.md#emptyArrayToSingle), por ejemplo: `arrayJoin(emptyArrayToSingle(...))`.
:::

Por ejemplo:

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

La función `arrayJoin` afecta a todas las secciones de la consulta, incluida la sección `WHERE`. Tenga en cuenta que el resultado de la consulta siguiente es `2`, aunque la subconsulta devolvió 1 fila.

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

Una consulta puede usar varias funciones `arrayJoin`. En ese caso, la transformación se realiza varias veces y el número de filas se multiplica.
Por ejemplo:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### Práctica recomendada
</div>

Usar varios `arrayJoin` con la misma expresión puede no producir los resultados esperados debido a la eliminación de subexpresiones comunes.
En esos casos, considere modificar las expresiones de array repetidas con operaciones adicionales que no afecten al resultado del join. Por ejemplo, `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

Ejemplo:

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

Observe la sintaxis de [`ARRAY JOIN`](../statements/select/array-join.md) en la consulta SELECT, que ofrece más posibilidades.
`ARRAY JOIN` le permite convertir varias arrays con el mismo número de elementos al mismo tiempo.

Ejemplo:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

O también puedes usar [`Tuple`](../data-types/tuple.md)

Ejemplo:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

El nombre `arrayJoin` en ClickHouse proviene de su similitud conceptual con la operación JOIN, pero aplicada a arrays dentro de una sola fila. Mientras que los JOIN tradicionales combinan filas de distintas tablas, `arrayJoin` &quot;une&quot; cada elemento de un array de una fila y produce múltiples filas —una por cada elemento del array—, duplicando al mismo tiempo los valores de las demás columnas. ClickHouse también proporciona la sintaxis de cláusula [`ARRAY JOIN`](/es/sql-reference/statements/select/array-join), lo que hace aún más explícita esta relación con las operaciones JOIN tradicionales al usar la terminología habitual de SQL JOIN. Este proceso también se conoce como &quot;desplegar&quot; el array, pero el término &quot;join&quot; se usa tanto en el nombre de la función como en la cláusula porque se asemeja a unir la tabla con los elementos del array, expandiendo así el conjunto de datos de forma similar a una operación JOIN.