---
alias: []
description: 'JSON 포맷 문서'
input_format: true
keywords: ['JSON']
output_format: true
slug: /interfaces/formats/JSON
title: 'JSON'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

`JSON` 포맷은 JSON 포맷으로 데이터를 읽고 출력합니다.

`JSON` 포맷은 다음을 반환합니다.

| Parameter                    | Description                                                                                                                                                                                                  |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `meta`                       | 컬럼 이름과 타입입니다.                                                                                                                                                                                                |
| `data`                       | 데이터 테이블입니다.                                                                                                                                                                                                  |
| `rows`                       | 출력된 전체 행 수입니다.                                                                                                                                                                                               |
| `rows_before_limit_at_least` | LIMIT이 없었다면 반환되었을 행 수의 하한 추정치입니다. 쿼리에 LIMIT이 포함된 경우에만 출력됩니다. 이 추정치는 limit 변환 이전에 쿼리 파이프라인에서 처리된 데이터 block을 기준으로 계산되지만, 이후 limit 변환에 의해 제외될 수 있습니다. block이 쿼리 파이프라인에서 limit 변환에 도달하지 않은 경우에는 이 추정에 포함되지 않습니다. |
| `statistics`                 | `elapsed`, `rows_read`, `bytes_read` 등의 통계입니다.                                                                                                                                                               |
| `totals`                     | 전체 합계 값입니다(WITH TOTALS 사용 시).                                                                                                                                                                                |
| `extremes`                   | 극값입니다(extremes가 1로 설정된 경우).                                                                                                                                                                                  |

`JSON` 형식은 JavaScript와 호환됩니다. 이를 보장하기 위해 일부 문자는 추가로 이스케이프됩니다.

* 슬래시 `/`는 `\/`로 이스케이프됩니다.
* 일부 브라우저에서 문제가 되는 대체 줄바꿈 문자 `U+2028` 및 `U+2029`는 `\uXXXX`로 이스케이프됩니다.
* ASCII 제어 문자는 이스케이프됩니다. 백스페이스, form feed, line feed, 캐리지 리턴, 가로 탭은 `\b`, `\f`, `\n`, `\r`, `\t`로 대체되며, 00-1F 범위의 나머지 바이트도 `\uXXXX` 시퀀스를 사용해 이스케이프됩니다.
* 잘못된 UTF-8 시퀀스는 대체 문자 �로 바뀌므로 출력 텍스트는 유효한 UTF-8 시퀀스로만 구성됩니다.

JavaScript와의 호환성을 위해 Int64 및 UInt64 정수는 기본적으로 큰따옴표로 묶입니다.
따옴표를 제거하려면 구성 매개변수 [`output_format_json_quote_64bit_integers`](/ko/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)를 `0`으로 설정할 수 있습니다.

ClickHouse는 [NULL](/ko/sql-reference/syntax.md)를 지원하며, JSON 출력에서는 `null`로 표시됩니다. 출력에서 `+nan`, `-nan`, `+inf`, `-inf` 값을 사용하려면 [output&#95;format&#95;json&#95;quote&#95;denormals](/ko/operations/settings/settings-formats.md/#output_format_json_quote_denormals)를 `1`로 설정하십시오.

<div id="example-usage">
  ## 사용 예시
</div>

예시:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

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
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

<div id="format-settings">
  ## 포맷 설정
</div>

JSON 입력 형식에서 설정 [`input_format_json_validate_types_from_metadata`](/ko/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata)이 `1`로 설정되면,
입력 데이터의 메타데이터에 있는 타입이 테이블의 해당 컬럼 타입과 비교됩니다.

<div id="see-also">
  ## 관련 항목
</div>

* [JSONEachRow](/ko/interfaces/formats/JSONEachRow) 포맷
* [output&#95;format&#95;json&#95;array&#95;of&#95;rows](/ko/operations/settings/settings-formats.md/#output_format_json_array_of_rows) 설정