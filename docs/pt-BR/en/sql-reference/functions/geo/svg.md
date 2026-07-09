---
description: 'Documentação da função Svg'
sidebar_label: 'SVG'
slug: /sql-reference/functions/geo/svg
title: 'Funções para gerar imagens em SVG a partir de dados Geo'
doc_type: 'referência'
---

<div id="svg">
  ## Svg
</div>

Retorna uma string com tags de elementos SVG selecionados a partir de dados Geo.

**Sintaxe**

```sql
Svg(geometry,[style])
```

Aliases: `SVG`, `svg`

**Parâmetros**

* `geometry` — dados Geo. [Geo](../../data-types/geo).
* `style` — Nome de estilo opcional. [String](../../data-types/string).

**Valor retornado**

* A representação SVG da geometria. [String](../../data-types/string).
  * Círculo em SVG
  * Polígono em SVG
  * Caminho em SVG

**Exemplos**

**Círculo**

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

**Caminho**

```sql title="Query"
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]])
```

```response title="Response"
<g fill-rule="evenodd"><path d="M 0,0 L 0,10 L 10,10 L 10,0 L 0,0M 4,4 L 5,4 L 5,5 L 4,5 L 4,4 z " style=""/></g>
```