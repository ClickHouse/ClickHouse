---
description: 'Documentação do flipCoordinates'
sidebar_label: 'Inversão de Coordenadas'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: 'Inversão de Coordenadas'
doc_type: 'reference'
---

<div id="flipcoordinates">
  ## flipCoordinates
</div>

A função `flipCoordinates` inverte as coordenadas de um ponto, anel, polígono ou multipolígono. Isso é útil, por exemplo, ao converter entre sistemas de coordenadas em que a ordem de latitude e longitude varia.

```sql
flipCoordinates(coordinates)
```

<div id="input-parameters">
  ### Parâmetros de entrada
</div>

* `coordinates` — Uma tupla que representa um ponto `(x, y)` ou um array dessas tuplas que representa um anel, um polígono ou um multipolígono. Os tipos de entrada compatíveis incluem:
  * [**Ponto**](../../data-types/geo.md#point): Uma tupla `(x, y)` em que `x` e `y` são valores [Float64](../../data-types/float.md).
  * [**Anel**](../../data-types/geo.md#ring): Um array de pontos `[(x1, y1), (x2, y2), ...]`.
  * [**Polygon**](../../data-types/geo.md#polygon): Um array de anéis `[ring1, ring2, ...]`, em que cada anel é um array de pontos.
  * [**Multipolígono**](../../data-types/geo.md#multipolygon): Um array de polígonos `[polygon1, polygon2, ...]`.

<div id="returned-value">
  ### Valor retornado
</div>

A função retorna o valor de entrada com as coordenadas invertidas. Por exemplo:

* Um ponto `(x, y)` passa a ser `(y, x)`.
* Um anel `[(x1, y1), (x2, y2)]` passa a ser `[(y1, x1), (y2, x2)]`.
* Estruturas aninhadas, como polígonos e multipolígonos, são processadas recursivamente.

<div id="examples">
  ### Exemplos
</div>

<div id="example-1">
  #### Exemplo 1: Invertendo um único ponto
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
  #### Exemplo 2: Invertendo um Array de Pontos (Ring)
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
  #### Exemplo 3: Invertendo um Polygon
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
  #### Exemplo 4: Invertendo um multipolígono
</div>

```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```