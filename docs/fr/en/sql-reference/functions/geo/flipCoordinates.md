---
description: 'Documentation de flipCoordinates'
sidebar_label: 'Inversion des coordonnées'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: 'Inversion des coordonnées'
doc_type: 'reference'
---

<div id="flipcoordinates">
  ## flipCoordinates
</div>

La fonction `flipCoordinates` inverse les coordonnées d’un point, d’un ring, d’un Polygon ou d’un multipolygon. C’est utile, par exemple, lors de la conversion entre systèmes de coordonnées où l’ordre de la latitude et de la longitude diffère.

```sql
flipCoordinates(coordinates)
```

<div id="input-parameters">
  ### Paramètres d&#39;entrée
</div>

* `coordinates` — Un n-uplet représentant un point `(x, y)`, ou un tableau de tels n-uplets représentant un Ring, un Polygon ou un Multipolygon. Les types d&#39;entrée pris en charge incluent :
  * [**Point**](../../data-types/geo.md#point) : Un n-uplet `(x, y)` où `x` et `y` sont des valeurs [Float64](../../data-types/float.md).
  * [**Ring**](../../data-types/geo.md#ring) : Un tableau de points `[(x1, y1), (x2, y2), ...]`.
  * [**Polygon**](../../data-types/geo.md#polygon) : Un tableau de Rings `[ring1, ring2, ...]`, où chaque Ring est un tableau de points.
  * [**Multipolygon**](../../data-types/geo.md#multipolygon) : Un tableau de Polygons `[polygon1, polygon2, ...]`.

<div id="returned-value">
  ### Valeur renvoyée
</div>

La fonction renvoie la valeur d’entrée avec les coordonnées inversées. Par exemple :

* Un point `(x, y)` devient `(y, x)`.
* Un Ring `[(x1, y1), (x2, y2)]` devient `[(y1, x1), (y2, x2)]`.
* Les structures imbriquées, comme les Polygons et les Multipolygons, sont traitées récursivement.

<div id="examples">
  ### Exemples
</div>

<div id="example-1">
  #### Exemple 1 : inversion d’un seul point
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
  #### Exemple 2 : inversion d’un Array de Points (Ring)
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
  #### Exemple 3 : inversion d’un Polygon
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
  #### Exemple 4 : Inversion des coordonnées d’un Multipolygon
</div>

```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```