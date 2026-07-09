---
description: 'Documentación de referencia de ClickHouse SQL'
keywords: ['clickhouse', 'docs', 'referencia de sql', 'sentencias sql', 'sql', 'sintaxis']
slug: /sql-reference
title: 'Referencia de SQL'
doc_type: 'reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

<div id="clickhouse-sql-reference">
  # Referencia de ClickHouse SQL
</div>

ClickHouse admite un lenguaje de consulta declarativo basado en SQL que, en muchos casos, es idéntico al estándar ANSI SQL.

Las consultas admitidas incluyen GROUP BY, ORDER BY, subconsultas en FROM, la cláusula JOIN, el operador IN, funciones de ventana y subconsultas escalares.

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />