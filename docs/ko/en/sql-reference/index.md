---
description: 'ClickHouse SQL 참고 문서'
keywords: ['ClickHouse', '문서', 'SQL 참고', 'SQL 문', 'SQL', '구문']
slug: /sql-reference
title: 'SQL 참고'
doc_type: 'reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

<div id="clickhouse-sql-reference">
  # ClickHouse SQL 참고
</div>

ClickHouse는 많은 경우 ANSI SQL 표준과 동일한 SQL 기반 선언형 쿼리 언어를 지원합니다.

지원되는 쿼리에는 GROUP BY, ORDER BY, FROM 절의 서브쿼리, JOIN 절, IN 연산자, 윈도우 함수 및 스칼라 서브쿼리가 포함됩니다.

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />