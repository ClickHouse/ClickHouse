---
alias: ['TSV']
description: 'TSV 포맷 문서'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭    |
| -- | -- | ----- |
| ✔  | ✔  | `TSV` |

<div id="description">
  ## 설명
</div>

TabSeparated 포맷에서는 데이터가 행 단위로 기록됩니다. 각 행은 탭으로 구분된 값들로 구성됩니다. 각 값 뒤에는 탭이 오고, 행의 마지막 값 뒤에는 줄 바꿈(line feed)이 옵니다. 모든 곳에서 엄격한 Unix 줄 바꿈을 사용한다고 가정합니다. 마지막 행의 끝에도 반드시 줄 바꿈이 있어야 합니다. 값은 따옴표로 감싸지 않은 텍스트 포맷으로 기록되며, 특수 문자는 이스케이프됩니다.

이 포맷은 `TSV`라는 이름으로도 사용할 수 있습니다.

`TabSeparated` 포맷은 사용자 정의 프로그램과 스크립트로 데이터를 처리할 때 편리합니다. 이 포맷은 HTTP 인터페이스와 command-line client의 batch mode에서 기본적으로 사용됩니다. 또한 이 포맷을 사용하면 서로 다른 DBMS 간에 데이터를 전송할 수도 있습니다. 예를 들어 MySQL에서 dump를 가져와 ClickHouse에 업로드하거나, 반대로 ClickHouse에서 MySQL로 전송할 수 있습니다.

`TabSeparated` 포맷은 합계 값 출력(WITH TOTALS 사용 시)과 극값 출력(&#39;extremes&#39;가 1로 설정된 경우)을 지원합니다. 이런 경우 합계 값과 극값은 기본 데이터 뒤에 출력됩니다. 기본 결과, 합계 값, 극값은 서로 빈 줄로 구분됩니다. 예시:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## 데이터 포맷팅
</div>

정수는 10진수 형식으로 작성됩니다. 숫자 시작 부분에는 추가로 `+` 문자를 넣을 수 있습니다(파싱 시에는 무시되고, 포맷팅 시에는 기록되지 않습니다). 0 이상의 숫자에는 음수 기호를 사용할 수 없습니다. 읽을 때는 빈 문자열을 0으로, 또는(부호 있는 타입의 경우) 마이너스 기호만 있는 문자열을 0으로 파싱할 수 있습니다. 해당 데이터 타입에 맞지 않는 숫자는 오류 메시지 없이 다른 숫자로 파싱될 수 있습니다.

부동소수점 수는 10진수 형식으로 작성됩니다. 소수 구분자로 점을 사용합니다. 지수 표기를 지원하며, `inf`, `+inf`, `-inf`, `nan`도 지원합니다. 부동소수점 수 항목은 소수점으로 시작하거나 소수점으로 끝날 수 있습니다.
포맷팅 시에는 부동소수점 수의 정밀도가 손실될 수 있습니다.
파싱 시에는 기계에서 표현 가능한 가장 가까운 수를 반드시 읽을 필요는 없습니다.

날짜는 YYYY-MM-DD 포맷으로 작성되며, 같은 포맷으로 파싱되지만 구분자로는 어떤 문자든 사용할 수 있습니다.
시간이 포함된 날짜는 `YYYY-MM-DD hh:mm:ss` 포맷으로 작성되며, 같은 포맷으로 파싱되지만 구분자로는 어떤 문자든 사용할 수 있습니다.
이 모든 작업은 클라이언트 또는 서버가 시작될 당시의 시스템 시간대(time zone)에서 수행됩니다(데이터를 포맷팅하는 쪽에 따라 달라집니다). 시간이 포함된 날짜의 경우 일광 절약 시간제는 지정되지 않습니다. 따라서 dump에 일광 절약 시간제 기간의 시간이 포함되어 있으면, 해당 dump는 데이터와 일의적으로 대응하지 않으며 파싱 시 두 시간 중 하나가 선택됩니다.
읽기 작업 중에는 잘못된 날짜와 시간이 포함된 날짜가 오류 메시지 없이 자연스러운 오버플로우 방식으로 또는 null 날짜 및 시간으로 파싱될 수 있습니다.

예외적으로, 시간이 포함된 날짜를 파싱할 때 정확히 10자리 10진수로 이루어진 경우 Unix timestamp 포맷도 지원합니다. 결과는 시간대의 영향을 받지 않습니다. `YYYY-MM-DD hh:mm:ss` 포맷과 `NNNNNNNNNN` 포맷은 자동으로 구분됩니다.

문자열은 특수 문자를 백슬래시로 이스케이프하여 출력됩니다. 출력에는 다음 이스케이프 시퀀스를 사용합니다: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. 파싱은 `\a`, `\v`, `\xHH`(16진수 이스케이프 시퀀스)와 `\c` 시퀀스도 지원합니다. 여기서 `c`는 임의의 문자이며(이 시퀀스는 `c`로 변환됩니다). 따라서 데이터를 읽을 때는 줄바꿈 문자를 `\n`, `\`, 또는 실제 줄바꿈 문자로 작성한 포맷도 지원합니다. 예를 들어, 공백 대신 단어 사이에 줄바꿈 문자가 들어간 문자열 `Hello world`는 다음 변형 중 어느 것으로도 파싱할 수 있습니다:

```text
Hello\nworld

Hello\
world
```

두 번째 방식도 지원됩니다. MySQL이 탭으로 구분된 dump를 기록할 때 이 방식을 사용하기 때문입니다.

TabSeparated 포맷으로 데이터를 전달할 때 최소한 이스케이프해야 하는 문자는 탭, 줄 바꿈(LF), 백슬래시입니다.

이스케이프되는 기호는 일부에 불과합니다. 따라서 출력할 때 터미널이 문자열 값을 쉽게 손상시킬 수 있습니다.

배열은 `[]` 안에 쉼표로 구분된 값 목록으로 기록됩니다. 배열의 숫자 항목은 일반적인 방식으로 포맷됩니다. `Date` 및 `DateTime` 타입은 작은따옴표로 기록됩니다. 문자열은 위와 동일한 이스케이프 규칙을 적용해 작은따옴표로 기록됩니다.

[NULL](/ko/sql-reference/syntax.md)은 [format&#95;tsv&#95;null&#95;representation](/ko/operations/settings/settings-formats.md/#format_tsv_null_representation) 설정에 따라 포맷됩니다(기본값은 `\N`).

입력 데이터에서 ENUM 값은 이름이나 id로 표현할 수 있습니다. 먼저 입력 값을 ENUM 이름과 매칭해 봅니다. 실패하고 입력 값이 숫자이면, 이 숫자를 ENUM id와 매칭해 봅니다.
입력 데이터에 ENUM id만 포함되어 있다면 ENUM 파싱을 최적화하기 위해 [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/ko/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) 설정을 활성화하는 것이 좋습니다.

[Nested](/ko/sql-reference/data-types/nested-data-structures/index.md) 구조의 각 요소는 배열로 표현됩니다.

예시는 다음과 같습니다:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

다음과 같이 `football.tsv`라는 이름의 TSV 파일을 사용합니다:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

데이터를 삽입하세요:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

다음과 같이 `TabSeparated` 포맷을 사용해 데이터를 읽습니다:

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

출력은 탭으로 구분된 포맷으로 제공됩니다:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                                                                                                                                       | 설명                                                                                                                                                                                                           | 기본값     |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| [`format_tsv_null_representation`](/ko/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | TSV 포맷에서 사용하는 사용자 지정 NULL 표현입니다.                                                                                                                                                                             | `\N`    |
| [`input_format_tsv_empty_as_default`](/ko/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | TSV 입력에서 빈 필드를 기본값으로 처리합니다. 복잡한 기본 표현식을 사용하려면 [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ko/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)도 활성화해야 합니다. | `false` |
| [`input_format_tsv_enum_as_number`](/ko/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | TSV 포맷에서 삽입된 enum 값을 enum 인덱스로 처리합니다.                                                                                                                                                                        | `false` |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/ko/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | TSV 포맷에서 스키마를 추론하기 위해 일부 조정과 휴리스틱을 사용합니다. 비활성화하면 모든 필드가 Strings로 추론됩니다.                                                                                                                                      | `true`  |
| [`output_format_tsv_crlf_end_of_line`](/ko/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | `true`로 설정하면 TSV 출력 형식의 줄 끝이 `\n` 대신 `\r\n`이 됩니다.                                                                                                                                                            | `false` |
| [`input_format_tsv_crlf_end_of_line`](/ko/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | `true`로 설정하면 TSV 입력 형식의 줄 끝이 `\n` 대신 `\r\n`이 됩니다.                                                                                                                                                            | `false` |
| [`input_format_tsv_skip_first_lines`](/ko/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | 데이터 시작 부분에서 지정한 개수만큼의 줄을 건너뜁니다.                                                                                                                                                                              | `0`     |
| [`input_format_tsv_detect_header`](/ko/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | TSV 포맷에서 이름과 타입이 포함된 헤더를 자동으로 감지합니다.                                                                                                                                                                         | `true`  |
| [`input_format_tsv_skip_trailing_empty_lines`](/ko/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | 데이터 끝의 빈 줄을 건너뜁니다.                                                                                                                                                                                           | `false` |
| [`input_format_tsv_allow_variable_number_of_columns`](/ko/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | TSV 포맷에서 가변적인 개수의 컬럼을 허용하며, 추가 컬럼은 무시하고 누락된 컬럼에는 기본값을 사용합니다.                                                                                                                                                 | `false` |