---
description: 'Documentation de référence SQL de ClickHouse'
keywords: ['clickhouse', 'documentation', 'référence SQL', 'instructions SQL', 'SQL', 'syntaxe']
slug: /sql-reference
title: 'Référence SQL'
doc_type: 'reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

<div id="clickhouse-sql-reference">
  # Référence SQL de ClickHouse
</div>

ClickHouse prend en charge un langage de requête déclaratif basé sur SQL, qui est conforme à la norme ANSI SQL dans de nombreux cas.

Les requêtes prises en charge incluent GROUP BY, ORDER BY, les sous-requêtes dans FROM, la clause JOIN, l’opérateur IN, les fonctions de fenêtre et les sous-requêtes scalaires.

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />