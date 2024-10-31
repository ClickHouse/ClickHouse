---
slug: /en/sql-reference/functions/geo/svg
sidebar_label: SVG
title: "Functions for Generating SVG images from Geo data"
---

## Svg

Returns a string of select SVG element tags from Geo data.

**Syntax**

``` sql
Svg(geometry,[style])
```

Aliases: `SVG`, `svg`

**Parameters**

- `geometry` — Geo data. [Geo](../../data-types/geo).
- `style` — Optional style name. [String](../../data-types/string).

**Returned value**

- The SVG representation of the geometry. [String](../../data-types/string).
  - SVG circle
  - SVG polygon
  - SVG path

**Examples**

**Circle**

Query:

```sql
SELECT SVG((0., 0.))
```

Result:

```response
<circle cx="0" cy="0" r="5" style=""/>
```

**Polygon**

Query:

```sql
SELECT SVG([(0., 0.), (10, 0), (10, 10), (0, 10)])
```

Result:

```response
<polygon points="0,0 0,10 10,10 10,0 0,0" style=""/>
```

**Path**

Query:

```sql
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]])
```

Result:

```response
<g fill-rule="evenodd"><path d="M 0,0 L 0,10 L 10,10 L 10,0 L 0,0M 4,4 L 5,4 L 5,5 L 4,5 L 4,4 z " style=""/></g>
```

