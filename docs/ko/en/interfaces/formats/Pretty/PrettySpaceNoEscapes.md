---
alias: []
description: 'PrettySpaceNoEscapes 포맷 문서'
input_format: false
keywords: ['PrettySpaceNoEscapes']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapes
title: 'PrettySpaceNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

[`PrettySpace`](./PrettySpace.md) 포맷과 다른 점은 [ANSI 이스케이프 시퀀스](http://en.wikipedia.org/wiki/ANSI_escape_code)를 사용하지 않는다는 것입니다.
이것은 브라우저에서 이 포맷을 표시하거나 `watch` 명령줄 유틸리티를 사용할 때 필요합니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="format-settings">
  ## 포맷 설정
</div>

<PrettyFormatSettings />