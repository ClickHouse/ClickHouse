---
description: 'Documentation de Svg'
sidebar_label: 'SVG'
slug: /sql-reference/functions/geo/svg
title: 'Fonctions pour générer des images SVG à partir de données Geo'
doc_type: 'reference'
---

<div id="svg">
  ## Svg
</div>

Renvoie une chaîne contenant certaines balises d’éléments SVG issues de données Geo.

**Syntaxe**

```sql
Svg(geometry,[style])
```

Alias : `SVG`, `svg`

**Paramètres**

* `geometry` — Donnée Geo. [Geo](../../data-types/geo).
* `style` — Nom de style facultatif. [String](../../data-types/string).

**Valeur renvoyée**

* La représentation SVG de la géométrie. [String](../../data-types/string).
  * Cercle SVG
  * Polygone SVG
  * Tracé SVG

**Exemples**

**Cercle**

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

**Chemin**

```sql title="Query"
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]])
```

```response title="Response"
<g fill-rule="evenodd"><path d="M 0,0 L 0,10 L 10,10 L 10,0 L 0,0M 4,4 L 5,4 L 5,5 L 4,5 L 4,4 z " style=""/></g>
```