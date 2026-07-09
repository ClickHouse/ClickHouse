---
alias: []
description: 'RowBinaryWithDefaults 포맷 문서'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
doc_type: '참고'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 설명
</div>

[`RowBinary`](./RowBinary.md) 포맷과 비슷하지만, 각 컬럼 앞에 기본값 사용 여부를 나타내는 추가 바이트가 있습니다.

<div id="example-usage">
  ## 사용 예시
</div>

예시:

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```

```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

* 컬럼 `x`에는 기본값을 사용해야 함을 나타내는 `01` 바이트 1개만 있으며, 이 바이트 뒤에는 다른 데이터가 제공되지 않습니다.
* 컬럼 `y`의 데이터는 `00` 바이트로 시작하며, 이는 해당 컬럼에 실제 값이 있고 그 값은 뒤따르는 데이터 `01000000`에서 읽어야 함을 나타냅니다.

<div id="format-settings">
  ## 포맷 설정
</div>

<RowBinaryFormatSettings />