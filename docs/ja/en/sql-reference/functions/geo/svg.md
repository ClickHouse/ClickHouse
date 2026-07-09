---
description: 'Svg のドキュメント'
sidebar_label: 'SVG'
slug: /sql-reference/functions/geo/svg
title: 'GeoデータからSVGイメージを生成する関数'
doc_type: 'reference'
---

<div id="svg">
  ## Svg
</div>

Geoデータから、一部のSVG要素タグを含む文字列を返します。

**構文**

```sql
Svg(geometry,[style])
```

別名: `SVG`, `svg`

**パラメータ**

* `geometry` — Geoデータ。[Geo](../../data-types/geo)。
* `style` — オプションのスタイル名。[String](../../data-types/string)。

**戻り値**

* ジオメトリのSVG表現。[String](../../data-types/string)。
  * SVGの円
  * SVGのPolygon
  * SVGのパス

**例**

**円**

```sql title="Query"
SELECT SVG((0., 0.))
```

```response title="Response"
<circle cx="0" cy="0" r="5" style=""/>
```

**Polygon**

```sql title="Query"
SELECT SVG([(0., 0.), (10, 0), (10, 10), (0, 10)])
```

```response title="Response"
<polygon points="0,0 0,10 10,10 10,0 0,0" style=""/>
```

**パス**

```sql title="Query"
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]])
```

```response title="Response"
<g fill-rule="evenodd"><path d="M 0,0 L 0,10 L 10,10 L 10,0 L 0,0M 4,4 L 5,4 L 5,5 L 4,5 L 4,4 z " style=""/></g>
```