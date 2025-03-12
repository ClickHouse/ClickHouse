---
slug: /sql-reference
keywords: ['clickhouse', 'docs', 'sql reference', 'sql statements', 'sql', 'syntax']
title: 'SQL Reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

# ClickHouse SQL Reference

ClickHouse supports a declarative query language based on SQL that is identical to the ANSI SQL standard in many cases.

Supported queries include GROUP BY, ORDER BY, subqueries in FROM, JOIN clause, IN operator, window functions and scalar subqueries.

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />