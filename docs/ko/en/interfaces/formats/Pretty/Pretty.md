---
alias: []
description: 'Pretty 형식에 대한 문서'
input_format: false
keywords: ['Pretty']
output_format: true
slug: /interfaces/formats/Pretty
title: 'Pretty'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

`Pretty` 포맷은 데이터를 유니코드 문자로 만든 표 형태로 출력하며,
터미널에 색상을 표시하기 위해 ANSI 이스케이프 시퀀스를 사용합니다.
테이블의 전체 격자가 그려지며, 각 행은 터미널에서 두 줄을 차지합니다.
각 결과 블록은 별도의 테이블로 출력됩니다.
이 방식은 결과를 버퍼링하지 않고 블록을 출력할 수 있게 하기 위해 필요합니다(모든 값의 표시 너비를 미리 계산하려면 버퍼링이 필요합니다).

[NULL](/ko/sql-reference/syntax.md)은 `ᴺᵁᴸᴸ`로 출력됩니다.

<div id="example-usage">
  ## 사용 예시
</div>

예시([`PrettyCompact`](./PrettyCompact.md) 포맷):

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

어떤 `Pretty` 포맷에서도 행에는 이스케이프 처리가 적용되지 않습니다. 다음 예시는 [`PrettyCompact`](./PrettyCompact.md) 포맷을 기준으로 합니다:

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

터미널에 너무 많은 데이터가 덤프되지 않도록 처음 `10,000`개 행만 출력됩니다.
행 수가 `10,000`개 이상이면 &quot;처음 10 000개를 표시했습니다&quot;라는 메시지가 출력됩니다.

:::note
이 포맷은 쿼리 결과를 출력하는 용도로만 적합하며, 데이터를 파싱하는 데는 적합하지 않습니다.
:::

Pretty 형식은 합계 값(`WITH TOTALS`를 사용하는 경우)과 극값(&#39;extremes&#39;가 1로 설정된 경우)을 출력할 수 있습니다.
이 경우 합계 값과 극값은 기본 데이터 뒤에 별도의 테이블로 출력됩니다.
이는 [`PrettyCompact`](./PrettyCompact.md) 포맷을 사용하는 다음 예시에서 확인할 수 있습니다:

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<div id="format-settings">
  ## 포맷 설정
</div>

<PrettyFormatSettings />