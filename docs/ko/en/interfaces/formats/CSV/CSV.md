---
alias: []
description: 'CSV 형식 문서'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: '참고'
---

<div id="description">
  ## 설명
</div>

쉼표로 구분된 값 포맷([RFC](https://tools.ietf.org/html/rfc4180))입니다.
포맷할 때는 행을 큰따옴표로 묶습니다. 문자열 안의 큰따옴표는 큰따옴표 2개를 연속해서 출력합니다.
그 외의 문자 이스케이프 규칙은 없습니다.

* Date 및 date-time은 큰따옴표로 묶습니다.
* 숫자는 따옴표 없이 출력됩니다.
* 값은 구분자 문자로 구분하며, 기본값은 `,`입니다. 구분자 문자는 설정 [format&#95;csv&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_csv_delimiter)에서 정의됩니다.
* 행은 Unix 줄바꿈(line feed, LF)으로 구분됩니다.
* 배열은 CSV에서 다음과 같이 직렬화됩니다.
  * 먼저 배열을 TabSeparated 포맷과 동일하게 문자열로 직렬화합니다.
  * 결과 문자열은 큰따옴표로 묶어 CSV에 출력합니다.
* CSV 형식의 튜플은 각각 별도의 컬럼으로 직렬화됩니다(즉, 튜플 내부의 중첩 구조는 유지되지 않습니다).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
기본적으로 구분자는 `,`입니다.
자세한 내용은 [format&#95;csv&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_csv_delimiter) 설정을 참조하십시오.
:::

파싱 시 모든 값은 따옴표가 있거나 없는 형식으로 파싱할 수 있습니다. 큰따옴표와 작은따옴표를 모두 지원합니다.

행 데이터도 따옴표 없이 배치할 수 있습니다. 이 경우 구분자 문자 또는 줄바꿈 문자(CR 또는 LF)가 나올 때까지 파싱됩니다.
하지만 RFC와 다르게, 따옴표 없는 행을 파싱할 때는 앞뒤 공백과 탭이 무시됩니다.
줄바꿈 문자는 Unix(LF), Windows(CR LF), Mac OS Classic(CR LF) 유형을 지원합니다.

`NULL`은 [format&#95;csv&#95;null&#95;representation](/ko/operations/settings/settings-formats.md/#format_csv_null_representation) 설정에 따라 포맷됩니다(기본값은 `\N`입니다).

입력 데이터에서 `ENUM` 값은 이름 또는 id로 표현할 수 있습니다.
먼저 입력 값을 `ENUM` 이름과 일치시키려고 시도합니다.
일치하지 않고 입력 값이 숫자이면, 이 숫자를 `ENUM` id와 일치시키려고 시도합니다.
입력 데이터에 `ENUM` id만 포함된 경우 `ENUM` 파싱을 최적화하기 위해 [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ko/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) 설정을 활성화하는 것이 좋습니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                                                                                                                                                                       | 설명                                                                  | 기본값     | 비고                                                                                                                                                                                  |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [format&#95;csv&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | CSV 데이터에서 구분자로 사용할 문자를 지정합니다.                                       | `,`     |                                                                                                                                                                                     |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/ko/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | 작은따옴표로 묶인 문자열을 허용합니다.                                               | `true`  |                                                                                                                                                                                     |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/ko/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | 큰따옴표로 묶인 문자열을 허용합니다.                                                | `true`  |                                                                                                                                                                                     |
| [format&#95;csv&#95;null&#95;representation](/ko/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | CSV 형식에서 사용할 사용자 지정 NULL 표현입니다.                                     | `\N`    |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/ko/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | CSV 입력의 빈 필드를 기본값으로 처리합니다.                                          | `true`  | 복잡한 기본값 표현식을 사용하는 경우 [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ko/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)도 활성화해야 합니다. |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ko/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | CSV 형식에서 삽입된 enum 값을 enum 인덱스로 처리합니다.                               | `false` |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/ko/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | CSV 형식에서 스키마 추론 시 일부 보정과 휴리스틱을 사용합니다. 비활성화하면 모든 필드가 Strings로 추론됩니다. | `true`  |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/ko/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | CSV에서 배열(Array)을 읽을 때 요소가 중첩된 CSV로 직렬화된 후 문자열에 담겨 있다고 가정합니다.        | `false` |                                                                                                                                                                                     |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/ko/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | `true`로 설정하면 CSV 출력 형식의 줄 끝이 `\n` 대신 `\r\n`이 됩니다.                   | `false` |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/ko/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | 데이터 시작 부분에서 지정된 수만큼 줄을 건너뜁니다.                                       | `0`     |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;detect&#95;header](/ko/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | CSV 형식에서 이름과 타입이 포함된 헤더를 자동으로 감지합니다.                                | `true`  |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/ko/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | 데이터 끝의 빈 줄을 건너뜁니다.                                                  | `false` |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/ko/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | 따옴표로 묶지 않은 CSV 문자열의 공백과 탭을 제거합니다.                                   | `true`  |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/ko/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | CSV 문자열에서 공백이나 탭을 필드 구분자로 사용할 수 있도록 합니다.                            | `false` |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/ko/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | CSV 형식에서 가변 개수의 컬럼을 허용하고, 추가 컬럼은 무시하며 누락된 컬럼에는 기본값을 사용합니다.          | `false` |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/ko/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | 잘못된 값 때문에 CSV 필드의 역직렬화에 실패한 경우 해당 컬럼에 기본값을 설정할 수 있도록 합니다.           | `false` |                                                                                                                                                                                     |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ko/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | 스키마 추론 중 문자열 필드에서 숫자를 추론하려고 시도합니다.                                  | `false` |                                                                                                                                                                                     |