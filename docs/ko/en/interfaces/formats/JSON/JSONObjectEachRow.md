---
alias: []
description: 'JSONObjectEachRow 포맷 문서'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

이 포맷에서는 모든 데이터가 하나의 JSON 객체로 표현되며, 각 행은 [`JSONEachRow`](./JSONEachRow.md) 포맷과 마찬가지로 해당 객체의 개별 필드로 표현됩니다.

<div id="example-usage">
  ## 예시 사용법
</div>

<div id="basic-example">
  ### 기본 예시
</div>

다음과 같은 JSON이 있을 때:

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

객체 이름을 컬럼 값으로 사용하려면 특수 설정 [`format_json_object_each_row_column_for_object_name`](/ko/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name)을 사용할 수 있습니다.
이 설정의 값에는 컬럼 이름을 지정하며, 이 컬럼 이름은 결과 객체에서 각 행의 JSON 키로 사용됩니다.

<div id="output">
  #### 출력
</div>

두 개의 컬럼이 있는 `test` 테이블이 있다고 가정해 보겠습니다:

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

`JSONObjectEachRow` 포맷으로 출력하고 `format_json_object_each_row_column_for_object_name` 설정을 사용해 보겠습니다:

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

<div id="input">
  #### 입력
</div>

이전 예시의 출력을 `data.json` 파일에 저장해 두었다고 가정하겠습니다:

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

스키마 추론에도 활용할 수 있습니다:

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

<div id="json-inserting-data">
  ### 데이터 삽입
</div>

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse에서는 다음 사항을 허용합니다:

* 객체 내 key-value 쌍의 순서는 임의여도 됩니다.
* 일부 값은 생략할 수 있습니다.

ClickHouse는 요소 사이의 공백과 객체 뒤에 오는 쉼표를 무시합니다. 모든 객체를 한 줄로 전달할 수 있습니다. 줄바꿈으로 구분할 필요는 없습니다.

<div id="omitted-values-processing">
  #### 생략된 값 처리
</div>

ClickHouse는 생략된 값을 해당 [데이터 타입](/ko/sql-reference/data-types/index.md)의 기본값으로 치환합니다.

`DEFAULT expr`가 지정된 경우, ClickHouse는 [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ko/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) 설정에 따라 다른 치환 규칙을 사용합니다.

다음 테이블을 살펴보십시오.

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

* `input_format_defaults_for_omitted_fields = 0`인 경우, `x`와 `a`의 기본값은 `0`입니다(`UInt32` 데이터 타입의 기본값).
* `input_format_defaults_for_omitted_fields = 1`인 경우, `x`의 기본값은 `0`이지만 `a`의 기본값은 `x * 2`입니다.

:::note
`input_format_defaults_for_omitted_fields = 1`로 데이터를 삽입하면 `input_format_defaults_for_omitted_fields = 0`으로 삽입할 때보다 ClickHouse에서 더 많은 계산 리소스를 사용합니다.
:::

<div id="json-selecting-data">
  ### 데이터 조회
</div>

`UserActivity` 테이블을 예시로 살펴보겠습니다:

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

쿼리 `SELECT * FROM UserActivity FORMAT JSONEachRow`의 반환 결과는 다음과 같습니다:

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

[JSON](/ko/interfaces/formats/JSON) 포맷과 달리, 잘못된 UTF-8 시퀀스를 치환하지 않습니다. 값은 `JSON`과 동일한 방식으로 이스케이프됩니다.

:::info
문자열에는 임의의 바이트 집합을 출력할 수 있습니다. 테이블의 데이터를 정보 손실 없이 JSON으로 포맷할 수 있다고 확신하는 경우 [`JSONEachRow`](./JSONEachRow.md) 포맷을 사용하십시오.
:::

<div id="jsoneachrow-nested">
  ### Nested 구조 활용
</div>

[`Nested`](/ko/sql-reference/data-types/nested-data-structures/index.md) 데이터 타입 컬럼이 있는 테이블에서는 동일한 구조의 JSON 데이터를 삽입할 수 있습니다. 이 기능을 사용하려면 [input&#95;format&#95;import&#95;nested&#95;json](/ko/operations/settings/settings-formats.md/#input_format_import_nested_json) 설정을 활성화하십시오.

예를 들어, 다음 테이블을 살펴보겠습니다:

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

`Nested` 데이터 타입 설명에서 볼 수 있듯이, ClickHouse는 중첩된 구조의 각 구성 요소를 개별 컬럼(이 테이블에서는 `n.s` 및 `n.i`)으로 처리합니다. 다음과 같이 데이터를 삽입할 수 있습니다:

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

계층적 JSON 객체로 데이터를 삽입하려면 [`input_format_import_nested_json=1`](/ko/operations/settings/settings-formats.md/#input_format_import_nested_json)로 설정하십시오.

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

이 설정이 없으면 ClickHouse에서 예외가 발생합니다.

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                                                                                                                                                           | 설명                                                                                                                  | 기본값      | 참고 사항                                                                                                                                                              |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`input_format_import_nested_json`](/ko/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                               | 중첩된 JSON 데이터를 중첩 테이블에 매핑합니다(JSONEachRow 포맷에서만 작동합니다).                                                               | `false`  |                                                                                                                                                                    |
| [`input_format_json_read_bools_as_numbers`](/ko/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                               | JSON 입력 형식에서 불리언 값을 숫자로 파싱할 수 있게 합니다.                                                                               | `true`   |                                                                                                                                                                    |
| [`input_format_json_read_bools_as_strings`](/ko/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                               | JSON 입력 형식에서 Bool 값을 문자열로 읽을 수 있도록 합니다.                                                                             | `true`   |                                                                                                                                                                    |
| [`input_format_json_read_numbers_as_strings`](/ko/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                           | JSON 입력 형식에서 숫자 값을 문자열로 읽을 수 있도록 합니다.                                                                               | `true`   |                                                                                                                                                                    |
| [`input_format_json_read_arrays_as_strings`](/ko/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                             | JSON 입력 형식에서 JSON 배열을 문자열로 읽을 수 있도록 합니다.                                                                            | `true`   |                                                                                                                                                                    |
| [`input_format_json_read_objects_as_strings`](/ko/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                           | JSON 입력 형식에서 JSON 객체를 문자열로 파싱할 수 있게 합니다.                                                                            | `true`   |                                                                                                                                                                    |
| [`input_format_json_named_tuples_as_objects`](/ko/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                           | named tuple 컬럼을 JSON 객체로 파싱합니다.                                                                                     | `true`   |                                                                                                                                                                    |
| [`input_format_json_try_infer_numbers_from_strings`](/ko/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                             | 스키마 추론 시 문자열 필드에서 숫자를 추론하도록 시도합니다.                                                                                  | `false`  |                                                                                                                                                                    |
| [`input_format_json_try_infer_named_tuples_from_objects`](/ko/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                   | 스키마 추론 중 JSON 객체에서 named tuple을 추론하도록 시도합니다.                                                                        | `true`   |                                                                                                                                                                    |
| [`input_format_json_infer_incomplete_types_as_strings`](/ko/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                       | JSON 입력 형식에서 스키마 추론 중 Null만 있거나 빈 객체/배열만 있는 키에는 String 유형을 사용합니다.                                                   | `true`   |                                                                                                                                                                    |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/ko/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | named tuple을 파싱하는 동안 JSON 객체에서 누락된 요소에 기본값을 삽입합니다.                                                                  | `true`   |                                                                                                                                                                    |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/ko/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                     | 이름이 지정된 Tuple용 JSON 객체에서 알 수 없는 키를 무시합니다.                                                                           | `false`  |                                                                                                                                                                    |
| [`input_format_json_compact_allow_variable_number_of_columns`](/ko/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)         | JSONCompact/JSONCompactEachRow 포맷에서 가변적인 수의 컬럼을 허용하고, 추가 컬럼은 무시하며, 누락된 컬럼에는 기본값을 사용합니다.                             | `false`  |                                                                                                                                                                    |
| [`input_format_json_throw_on_bad_escape_sequence`](/ko/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                 | JSON string에 잘못된 이스케이프 시퀀스가 있으면 예외를 발생시킵니다. 비활성화하면 잘못된 이스케이프 시퀀스가 데이터에 그대로 남습니다.                                    | `true`   |                                                                                                                                                                    |
| [`input_format_json_empty_as_default`](/ko/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                         | JSON 입력의 빈 field를 기본값으로 처리합니다.                                                                                      | `false`. | 복잡한 기본 표현식을 사용하려면 [`input_format_defaults_for_omitted_fields`](/ko/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)도 활성화되어 있어야 합니다. |
| [`output_format_json_quote_64bit_integers`](/ko/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                               | JSON 출력 형식에서 64비트 정수에 따옴표를 사용할지 제어합니다.                                                                              | `true`   |                                                                                                                                                                    |
| [`output_format_json_quote_64bit_floats`](/ko/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                   | JSON 출력 형식에서 64비트 부동소수점 수에 따옴표를 사용할지 제어합니다.                                                                         | `false`  |                                                                                                                                                                    |
| [`output_format_json_quote_denormals`](/ko/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                         | JSON 출력 형식에서 &#39;+nan&#39;, &#39;-nan&#39;, &#39;+inf&#39;, &#39;-inf&#39;를 출력할 수 있도록 합니다.                         | `false`  |                                                                                                                                                                    |
| [`output_format_json_quote_decimals`](/ko/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                           | JSON 출력 형식에서 Decimal 값에 따옴표를 사용할지 제어합니다.                                                                            | `false`  |                                                                                                                                                                    |
| [`output_format_json_escape_forward_slashes`](/ko/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                           | JSON 출력 형식에서 String 출력의 슬래시(/) 이스케이프 처리를 제어합니다.                                                                     | `true`   |                                                                                                                                                                    |
| [`output_format_json_named_tuples_as_objects`](/ko/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                         | named tuple 컬럼을 JSON 객체로 serialize합니다.                                                                              | `true`   |                                                                                                                                                                    |
| [`output_format_json_array_of_rows`](/ko/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                             | 모든 행을 JSONEachRow(Compact) 포맷의 JSON 배열로 출력합니다.                                                                      | `false`  |                                                                                                                                                                    |
| [`output_format_json_validate_utf8`](/ko/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                             | JSON 출력 형식에서 UTF-8 시퀀스 검사를 활성화합니다(JSON/JSONCompact/JSONColumnsWithMetadata 포맷에는 영향을 주지 않으며, 이 포맷들은 항상 UTF8을 검사합니다). | `false`  |                                                                                                                                                                    |