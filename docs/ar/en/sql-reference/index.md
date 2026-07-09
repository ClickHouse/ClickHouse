---
description: 'توثيق مرجع SQL في ClickHouse'
keywords: ['clickhouse', 'الوثائق', 'مرجع sql', 'عبارات sql', 'sql', 'البنية']
slug: /sql-reference
title: 'مرجع SQL'
doc_type: 'reference'
---

import { TwoColumnList } from '/src/components/two_column_list'
import { ClickableSquare } from '/src/components/clickable_square'
import { HorizontalDivide } from '/src/components/horizontal_divide'
import { ViewAllLink } from '/src/components/view_all_link'
import { VideoContainer } from '/src/components/video_container'

import LinksDeployment from './sql-reference-links.json'

<div id="clickhouse-sql-reference">
  # مرجع SQL في ClickHouse
</div>

يدعم ClickHouse لغة استعلامات تعريفية تستند إلى SQL، وتتطابق في كثير من الحالات مع معيار ANSI SQL.

تشمل الاستعلامات المدعومة GROUP BY وORDER BY والاستعلامات الفرعية في FROM وعبارة JOIN وعامل التشغيل IN ودوال النوافذ والاستعلامات الفرعية ذات القيمة المفردة.

<HorizontalDivide />

<TwoColumnList items={LinksDeployment} />