---
alias: []
description: 'JSONColumnsWithMetadata 포맷 문서'
input_format: true
keywords: ['JSONColumnsWithMetadata']
output_format: true
slug: /interfaces/formats/JSONColumnsWithMetadata
title: 'JSONColumnsWithMetadata'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[`JSONColumns`](./JSONColumns.md) 포맷과 달리, 일부 메타데이터와 통계도 포함합니다([`JSON`](./JSON.md) 포맷과 유사함).

:::note
`JSONColumnsWithMetadata` 포맷은 모든 데이터를 메모리에 버퍼링한 후 하나의 block으로 출력하므로 메모리 사용량이 높아질 수 있습니다.
:::

<div id="example-usage">
  ## 사용 예시
</div>

예시:

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },

                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

`JSONColumnsWithMetadata` 입력 형식에서 설정 [`input_format_json_validate_types_from_metadata`](/ko/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata)를 `1`로 설정하면,
입력 데이터의 메타데이터에 있는 타입을 테이블의 해당 컬럼 타입과 비교합니다.

<div id="format-settings">
  ## 포맷 설정
</div>
