---
alias: []
description: 'PrettyNoEscapes 포맷 문서'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: '참고'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 설명
</div>

[ANSI 이스케이프 시퀀스](http://en.wikipedia.org/wiki/ANSI_escape_code)를 사용하지 않는다는 점에서 [Pretty](/ko/interfaces/formats/Pretty)와 다릅니다.
이는 브라우저에서 이 포맷을 표시하거나 `watch` 명령줄 유틸리티를 사용할 때 필요합니다.

<div id="example-usage">
  ## 사용 예시
</div>

예시:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
브라우저에서 이 포맷을 표시하려면 [HTTP 인터페이스](/ko/interfaces/http)를 사용할 수 있습니다.
:::

<div id="format-settings">
  ## 포맷 설정
</div>

<PrettyFormatSettings />