---
description: 'Документация по flipCoordinates'
sidebar_label: 'Перестановка координат'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: 'Перестановка координат'
doc_type: 'reference'
---

<div id="flipcoordinates">
  ## flipCoordinates
</div>

Функция `flipCoordinates` меняет местами координаты объектов типа Point, Ring, Polygon или Multipolygon. Это полезно, например, при преобразовании между системами координат, в которых порядок широты и долготы различается.

```sql
flipCoordinates(coordinates)
```

<div id="input-parameters">
  ### Входные параметры
</div>

* `coordinates` — кортеж, представляющий точку `(x, y)`, или массив таких кортежей, представляющих Ring, Polygon или Multipolygon. Поддерживаются следующие типы входных данных:
  * [**Point**](../../data-types/geo.md#point): кортеж `(x, y)`, где `x` и `y` — значения [Float64](../../data-types/float.md).
  * [**Ring**](../../data-types/geo.md#ring): массив точек `[(x1, y1), (x2, y2), ...]`.
  * [**Polygon**](../../data-types/geo.md#polygon): массив Ring `[ring1, ring2, ...]`, где каждый Ring представляет собой массив точек.
  * [**Multipolygon**](../../data-types/geo.md#multipolygon): массив Polygon `[polygon1, polygon2, ...]`.

<div id="returned-value">
  ### Возвращаемое значение
</div>

Функция возвращает входные данные с переставленными координатами. Например:

* Точка `(x, y)` становится `(y, x)`.
* Ring `[(x1, y1), (x2, y2)]` становится `[(y1, x1), (y2, x2)]`.
* Вложенные структуры, такие как полигоны и мультиполигоны, обрабатываются рекурсивно.

<div id="examples">
  ### Примеры
</div>

<div id="example-1">
  #### Пример 1: Инверсия координат одного Point
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
  #### Пример 2: Перестановка координат в массиве точек Point (Ring)
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
  #### Пример 3: Переворачивание типа Polygon
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
  #### Пример 4: Перестановка координат в Multipolygon
</div>

```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```