---
description: 'Documentación de flipCoordinates'
sidebar_label: 'Invertir coordenadas'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: 'Invertir coordenadas'
doc_type: 'reference'
---

<div id="flipcoordinates">
  ## flipCoordinates
</div>

La función `flipCoordinates` intercambia las coordenadas de un Point, Ring, Polygon o multipolygon. Esto resulta útil, por ejemplo, al convertir entre sistemas de coordenadas en los que el orden de la latitud y la longitud es distinto.

```sql
flipCoordinates(coordinates)
```

<div id="input-parameters">
  ### Parámetros de entrada
</div>

* `coordinates` — Una tupla que representa un punto `(x, y)`, o un array de esas tuplas que representa un anillo, un polígono o un multipolígono. Los tipos de entrada admitidos incluyen:
  * [**Point**](../../data-types/geo.md#point): Una tupla `(x, y)` donde `x` e `y` son valores de [Float64](../../data-types/float.md).
  * [**Ring**](../../data-types/geo.md#ring): Un array de puntos `[(x1, y1), (x2, y2), ...]`.
  * [**Polygon**](../../data-types/geo.md#polygon): Un array de anillos `[ring1, ring2, ...]`, donde cada anillo es un array de puntos.
  * [**Multipolygon**](../../data-types/geo.md#multipolygon): Un array de polígonos `[polygon1, polygon2, ...]`.

<div id="returned-value">
  ### Valor devuelto
</div>

La función devuelve el valor de entrada con las coordenadas intercambiadas. Por ejemplo:

* Un punto `(x, y)` se convierte en `(y, x)`.
* Un anillo `[(x1, y1), (x2, y2)]` se convierte en `[(y1, x1), (y2, x2)]`.
* Las estructuras anidadas, como los polígonos y los multipolígonos, se procesan de forma recursiva.

<div id="examples">
  ### Ejemplos
</div>

<div id="example-1">
  #### Ejemplo 1: Invertir las coordenadas de un solo punto
</div>

```sql
SELECT flipCoordinates((10, 20)) AS flipped_point
```

```text
┌─flipped_point─┐
│ (20,10)       │
└───────────────┘
```

<div id="example-2">
  #### Ejemplo 2: Invertir un Array de puntos (Ring)
</div>

```sql
SELECT flipCoordinates([(10, 20), (30, 40)]) AS flipped_ring
```

```text
┌─flipped_ring──────────────┐
│ [(20,10),(40,30)]         │
└───────────────────────────┘
```

<div id="example-3">
  #### Ejemplo 3: Invertir un Polygon
</div>

```sql
SELECT flipCoordinates([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]) AS flipped_polygon
```

```text
┌─flipped_polygon──────────────────────────────┐
│ [[(20,10),(40,30)],[(60,50),(80,70)]]        │
└──────────────────────────────────────────────┘
```

<div id="example-4">
  #### Ejemplo 4: Invertir un Multipolygon
</div>

```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```