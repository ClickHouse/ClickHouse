---
alias: []
description: 'PrettyCompactMonoBlock 포맷 문서'
input_format: false
keywords: ['PrettyCompactMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactMonoBlock
title: 'PrettyCompactMonoBlock'
doc_type: '참고'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

최대 `10,000`개의 행을 버퍼에 저장한 뒤 하나의 테이블로 출력하며, [blocks](/ko/development/architecture#block) 단위로 출력하지 않는다는 점에서 [`PrettyCompact`](./PrettyCompact.md) 포맷과 다릅니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="format-settings">
  ## 포맷 설정
</div>

<PrettyFormatSettings />