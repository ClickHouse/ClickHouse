---
description: 'RowBinaryWithNames 포맷 문서'
input_format: true
keywords: ['RowBinaryWithNames']
output_format: true
slug: /interfaces/formats/RowBinaryWithNames
title: 'RowBinaryWithNames'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[`RowBinary`](./RowBinary.md) 포맷과 유사하지만, 헤더가 추가되어 있습니다:

* [`LEB128`](https://en.wikipedia.org/wiki/LEB128)로 인코딩된 컬럼 수(N)
* 컬럼 이름을 지정하는 N개의 `String`

<div id="example-usage">
  ## 사용 예시
</div>

<div id="format-settings">
  ## 포맷 설정
</div>

<RowBinaryFormatSettings />

:::note

* [`input_format_with_names_use_header`](/ko/operations/settings/settings-formats.md/#input_format_with_names_use_header) 설정이 `1`이면 입력 데이터의 컬럼이 이름을 기준으로 테이블 컬럼에 매핑되며, 이름을 알 수 없는 컬럼은 건너뜁니다.
* [`input_format_skip_unknown_fields`](/ko/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) 설정이 `1`이면 이름을 알 수 없는 컬럼을 건너뜁니다.
  그렇지 않으면 첫 번째 행을 건너뜁니다.
  :::