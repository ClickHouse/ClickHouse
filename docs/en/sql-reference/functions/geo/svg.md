---
slug: /en/sql-reference/functions/geo/svg
sidebar_label: SVG
title: "Functions for Generating SVG images from Geo data"
---

## Syntax

``` sql
SVG(geometry,[style])
```

### Parameters

- `geometry` — Geo data
- `style` — Optional style name

### Returned value

- The SVG representation of the geometry:
  - SVG circle
  - SVG polygon
  - SVG path

Type: String

## Examples

### Circle
```sql
SELECT SVG((0., 0.))
```
```response
<circle cx="0" cy="0" r="5" style=""/>
```

### Polygon
```sql
SELECT SVG([(0., 0.), (10, 0), (10, 10), (0, 10)])
```
```response
<polygon points="0,0 0,10 10,10 10,0 0,0" style=""/>
```

### Path
```sql
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]])
```
```response
<g fill-rule="evenodd"><path d="M 0,0 L 0,10 L 10,10 L 10,0 L 0,0M 4,4 L 5,4 L 5,5 L 4,5 L 4,4 z " style=""/></g>
```

