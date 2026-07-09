---
alias: []
description: 'PrettySpaceMonoBlock 포맷 문서'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: '참고'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

[`PrettySpace`](./PrettySpace.md) 포맷과는 달리, 최대 `10,000`개의 행을 버퍼에 모았다가
[블록](/ko/development/architecture#block) 단위가 아니라 하나의 테이블로 출력합니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="format-settings">
  ## 포맷 설정
</div>

<PrettyFormatSettings />