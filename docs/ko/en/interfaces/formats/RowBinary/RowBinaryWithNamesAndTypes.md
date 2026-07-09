---
alias: []
description: 'RowBinaryWithNamesAndTypes 포맷 문서'
input_format: true
keywords: ['RowBinaryWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/RowBinaryWithNamesAndTypes
title: 'RowBinaryWithNamesAndTypes'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[RowBinary](./RowBinary.md) 포맷과 유사하지만, 다음 헤더가 추가됩니다:

* [`LEB128`](https://en.wikipedia.org/wiki/LEB128)로 인코딩된 컬럼 수(N)
* 컬럼 이름을 지정하는 N개의 `String`
* 컬럼 타입을 지정하는 N개의 `String`

<div id="example-usage">
  ## 사용 예시
</div>

<div id="format-settings">
  ## 포맷 설정
</div>

<RowBinaryFormatSettings />

:::note
설정 [`input_format_with_names_use_header`](/ko/operations/settings/settings-formats.md/#input_format_with_names_use_header)가 1로 설정되면,
입력 데이터의 컬럼이 이름을 기준으로 테이블 컬럼에 매핑되며, 설정 [input&#95;format&#95;skip&#95;unknown&#95;fields](/ko/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)가 1로 설정된 경우 알 수 없는 이름의 컬럼은 건너뜁니다.
그렇지 않으면 첫 번째 행을 건너뜁니다.
설정 [`input_format_with_types_use_header`](/ko/operations/settings/settings-formats.md/#input_format_with_types_use_header)가 `1`로 설정되면,
입력 데이터의 타입을 테이블의 해당 컬럼 타입과 비교합니다. 그렇지 않으면 두 번째 행을 건너뜁니다.
:::