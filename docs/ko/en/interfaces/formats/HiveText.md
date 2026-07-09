---
alias: []
description: 'HiveText 포맷 문서'
input_format: true
keywords: ['HiveText']
output_format: false
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 설명
</div>

`HiveText`는 [Apache Hive](https://hive.apache.org/) 테이블에서 사용하는 텍스트 직렬화 포맷(Hive의 `LazySimpleSerDe`가 생성하는 포맷)을 읽습니다. 이는 [`CSV`](/ko/interfaces/formats/CSV)와 유사한 구분된 텍스트 포맷으로, 필드는 Hive 기본 구분 기호인 `\x01` (Ctrl-A)로 분리됩니다. 필드 구분 기호는 [`input_format_hive_text_fields_delimiter`](#format-settings)로 구성할 수 있습니다.

`HiveText`는 입력 전용 포맷입니다. 데이터에는 헤더 행이 없습니다. 값은 위치에 따라 대상 테이블의 컬럼에 매핑되므로, 컬럼 이름과 타입은 데이터에서 추론하지 않고 테이블(또는 명시적으로 제공된 구조)에서 가져옵니다. 읽는 동안 ClickHouse는 날짜와 시간을 최선형 모드로 파싱하고([`date_time_input_format`](/ko/operations/settings/formats#date_time_input_format) 참조), 생략된 후행 필드는 컬럼 기본값으로 채우며, 인식하지 못하는 필드는 건너뜁니다.

필드 내부에서는 Hive의 중첩 구분 기호가 아니라 `CSV`와 동일한 이스케이프 규칙을 사용해 값을 파싱합니다. 특히 [`Array`](/ko/sql-reference/data-types/array) 타입의 컬럼은 대괄호로 둘러싸인 표현(예: `"['a','b','c']"`)에서 읽으며, Hive collection 구분 기호 `\x02`로 구분된 값으로 읽지 않습니다.

:::note 중첩 구분 기호 설정은 영향을 주지 않습니다
[`input_format_hive_text_collection_items_delimiter`](#format-settings) 및
[`input_format_hive_text_map_keys_delimiter`](#format-settings) 설정은 호환성을 위해 허용되지만, 현재 파싱 중에는 사용되지 않습니다.
:::

기본적으로 행에는 가변 개수의 필드를 허용합니다([`input_format_hive_text_allow_variable_number_of_columns`](#format-settings) 참조). 즉, 테이블보다 필드 수가 적은 행은 누락된 컬럼이 기본값으로 채워지고, 추가 후행 필드가 있는 행은 해당 추가 필드를 건너뜁니다.

<div id="example-usage">
  ## 사용 예시
</div>

아래 예시에서는 입력 파일을 더 쉽게 읽을 수 있도록
[`input_format_hive_text_fields_delimiter`](#format-settings)를 사용해 기본 필드 구분 기호를 쉼표(`,`)로 재정의합니다.

<div id="reading-data">
  ### HiveText 파일 읽기
</div>

쉼표로 구분된 필드가 포함된 파일 `hive_data.txt`가 있다고 가정합니다:

```text title="hive_data.txt"
1,3
3,5,9
```

컬럼 이름과 타입을 정의하는 테이블을 생성한 다음, `FORMAT HiveText`를 사용하여 해당 테이블에 파일을 삽입합니다:

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

첫 번째 행인 `1,3`에는 필드가 2개뿐이므로, 빠진 컬럼 `c`는
기본값 `0`으로 채워집니다.

<div id="variable-number-of-columns">
  ### 가변 개수의 컬럼
</div>

기본값 `input_format_hive_text_allow_variable_number_of_columns = 1`에서는
테이블(table)의 필드 수보다 많은 필드를 가진 행이 있어도, 끝부분의 추가 필드는
단순히 건너뜁니다:

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

대신 `input_format_hive_text_allow_variable_number_of_columns = 0`로 설정하면
필드 수를 엄격하게 적용하며, 테이블보다 필드 수가 적은
행이 있으면 파싱 예외가 발생합니다.

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                                        | 설명                                                                    | 기본값    |
| --------------------------------------------------------- | --------------------------------------------------------------------- | ------ |
| `input_format_hive_text_fields_delimiter`                 | Hive Text File의 필드 사이 구분자                                             | `\x01` |
| `input_format_hive_text_collection_items_delimiter`       | Hive Text File의 컬렉션(배열 또는 맵) 항목 사이 구분자입니다. 허용되지만 현재 파싱 중에는 사용되지 않습니다. | `\x02` |
| `input_format_hive_text_map_keys_delimiter`               | Hive Text File의 맵 키/값 쌍 사이 구분자입니다. 허용되지만 현재 파싱 중에는 사용되지 않습니다.         | `\x03` |
| `input_format_hive_text_allow_variable_number_of_columns` | Hive Text 입력에서 추가 컬럼은 무시하고(파일에 예상보다 많은 컬럼이 있는 경우) 누락된 필드는 기본값으로 처리합니다 | `1`    |