---
sidebar_position: 62
sidebar_label: Географические структуры
---

# Типы данных для работы с географическими структурами {#geo-data-types}

ClickHouse поддерживает типы данных для отображения географических объектов — точек (местоположений), территорий и т.п.

:::danger "Предупреждение"
    Сейчас использование типов данных для работы с географическими структурами является экспериментальной возможностью. Чтобы использовать эти типы данных, включите настройку `allow_experimental_geo_types = 1`.

**См. также**
- [Хранение географических структур данных](https://ru.wikipedia.org/wiki/GeoJSON).
- Настройка [allow_experimental_geo_types](../../operations/settings/settings.md#allow-experimental-geo-types).

## Point {#point-data-type}

Тип `Point` (точка) определяется парой координат X и Y и хранится в виде кортежа [Tuple](tuple.md)([Float64](float.md), [Float64](float.md)).

**Пример**

Запрос:

```sql
SET allow_experimental_geo_types = 1;
CREATE TABLE geo_point (p Point) ENGINE = Memory();
INSERT INTO geo_point VALUES((10, 10));
SELECT p, toTypeName(p) FROM geo_point;
```
Результат:

``` text
┌─p─────┬─toTypeName(p)─┐
│ (10,10) │ Point         │
└───────┴───────────────┘
```

## Ring {#ring-data-type}

Тип `Ring` описывает простой многоугольник без внутренних областей (дыр) и хранится в виде массива точек: [Array](array.md)([Point](#point-data-type)).

**Пример**

Запрос:

```sql
SET allow_experimental_geo_types = 1;
CREATE TABLE geo_ring (r Ring) ENGINE = Memory();
INSERT INTO geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT r, toTypeName(r) FROM geo_ring;
```
Результат:

``` text
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ Ring          │
└───────────────────────────────┴───────────────┘
```

## Polygon {#polygon-data-type}

Тип `Polygon` описывает многоугольник с внутренними областями (дырами) и хранится в виде массива: [Array](array.md)([Ring](#ring-data-type)). Первый элемент массива описывает внешний многоугольник (контур), а остальные элементы описывают дыры.

**Пример**

Запись в этой таблице описывает многоугольник с одной дырой:

```sql
SET allow_experimental_geo_types = 1;
CREATE TABLE geo_polygon (pg Polygon) ENGINE = Memory();
INSERT INTO geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);
SELECT pg, toTypeName(pg) FROM geo_polygon;
```

Результат:

``` text
┌─pg────────────────────────────────────────────────────────────┬─toTypeName(pg)─┐
│ [[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]] │ Polygon        │
└───────────────────────────────────────────────────────────────┴────────────────┘
```

## MultiPolygon {#multipolygon-data-type}

Тип `MultiPolygon` описывает элемент, состоящий из нескольких простых многоугольников (полигональную сетку). Он хранится в виде массива многоугольников: [Array](array.md)([Polygon](#polygon-data-type)).

**Пример**

Запись в этой таблице описывает элемент, состоящий из двух многоугольников — первый без дыр, а второй с одной дырой:

```sql
SET allow_experimental_geo_types = 1;
CREATE TABLE geo_multipolygon (mpg MultiPolygon) ENGINE = Memory();
INSERT INTO geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);
SELECT mpg, toTypeName(mpg) FROM geo_multipolygon;
```
Result:

``` text
┌─mpg─────────────────────────────────────────────────────────────────────────────────────────────┬─toTypeName(mpg)─┐
│ [[[(0,0),(10,0),(10,10),(0,10)]],[[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]]] │ MultiPolygon    │
└─────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────┘
```

