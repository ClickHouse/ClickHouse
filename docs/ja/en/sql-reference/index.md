---
description: 'ClickHouse SQLリファレンスのドキュメント'
keywords: ['clickhouse', 'docs', 'sql reference', 'sql statements', 'sql', 'syntax']
slug: /sql-reference
title: 'SQLリファレンス'
doc_type: 'reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

<div id="clickhouse-sql-reference">
  # ClickHouse SQL リファレンス
</div>

ClickHouse は、多くの点で ANSI SQL 標準と同等の、SQL ベースの宣言型クエリ言語をサポートしています。

サポートされるクエリには、GROUP BY、ORDER BY、FROM 内のサブクエリ、JOIN 句、IN 演算子、ウィンドウ関数、スカラーサブクエリが含まれます。

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />