---
alias: []
description: 'PrettyMonoBlock 포맷 문서'
input_format: false
keywords: ['PrettyMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyMonoBlock
title: 'PrettyMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

최대 `10,000`개의 행이 버퍼링된 후 [블록](/ko/development/architecture#block) 단위가 아니라
하나의 테이블(table)로 출력된다는 점에서 [`Pretty`](/ko/interfaces/formats/Pretty) 포맷과 다릅니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="format-settings">
  ## 포맷 설정
</div>

<PrettyFormatSettings />