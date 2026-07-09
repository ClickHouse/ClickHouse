---
description: 'Документация для справочника по ClickHouse SQL'
keywords: ['clickhouse', 'docs', 'справочник по ClickHouse SQL', 'команды SQL', 'sql', 'синтаксис']
slug: /sql-reference
title: 'Справочник по ClickHouse SQL'
doc_type: 'reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

<div id="clickhouse-sql-reference">
  # справочник по ClickHouse SQL
</div>

ClickHouse поддерживает декларативный язык запросов на основе SQL, который во многих случаях полностью соответствует стандарту ANSI SQL.

Среди поддерживаемых запросов и конструкций — GROUP BY, ORDER BY, подзапросы в FROM, предложение JOIN, оператор IN, оконные функции и скалярные подзапросы.

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />