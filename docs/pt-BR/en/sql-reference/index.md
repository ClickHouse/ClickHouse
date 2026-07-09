---
description: 'Documentação da Referência SQL do ClickHouse'
keywords: ['clickhouse', 'documentação', 'referência SQL', 'instruções SQL', 'sql', 'sintaxe']
slug: /sql-reference
title: 'Referência SQL'
doc_type: 'reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

<div id="clickhouse-sql-reference">
  # Referência SQL do ClickHouse
</div>

O ClickHouse oferece suporte a uma linguagem de consulta declarativa baseada em SQL que, em muitos casos, é idêntica ao padrão ANSI SQL.

Entre as consultas compatíveis estão GROUP BY, ORDER BY, subconsultas em FROM, cláusula JOIN, operador IN, funções de janela e subconsultas escalares.

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />