---
alias: []
description: 'CustomSeparated 포맷 문서'
input_format: true
keywords: ['CustomSeparated']
output_format: true
slug: /interfaces/formats/CustomSeparated
title: 'CustomSeparated'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[Template](../Template/Template.md)와 비슷하지만, 모든 컬럼의 이름과 타입을 출력하거나 읽고, [format&#95;custom&#95;escaping&#95;rule](../../../operations/settings/settings-formats.md/#format_custom_escaping_rule) 설정의 이스케이프 규칙과 다음 설정에 지정된 구분 기호를 사용합니다.

* [format&#95;custom&#95;field&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_custom_field_delimiter)
* [format&#95;custom&#95;row&#95;before&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
* [format&#95;custom&#95;row&#95;after&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
* [format&#95;custom&#95;row&#95;between&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
* [format&#95;custom&#95;result&#95;before&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
* [format&#95;custom&#95;result&#95;after&#95;delimiter](/ko/operations/settings/settings-formats.md/#format_custom_result_after_delimiter)

:::note
포맷 문자열의 이스케이프 규칙 설정과 구분 기호는 사용하지 않습니다.
:::

또한 [TemplateIgnoreSpaces](../Template//TemplateIgnoreSpaces.md)와 유사한 [`CustomSeparatedIgnoreSpaces`](../CustomSeparated/CustomSeparatedIgnoreSpaces.md) 포맷도 제공합니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

`football.txt`라는 다음 txt 파일을 사용합니다:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

사용자 지정 구분자 설정을 다음과 같이 구성하세요:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

데이터를 삽입하세요:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparated;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

사용자 지정 구분자 설정을 구성하세요:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

`CustomSeparated` 포맷으로 데이터를 읽습니다:

```sql
SELECT *
FROM football
FORMAT CustomSeparated
```

출력은 설정된 사용자 지정 포맷으로 제공됩니다:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

<div id="format-settings">
  ## 포맷 설정
</div>

추가 설정:

| 설정                                                                                                                                                                                         | 설명                                                                          | 기본값     |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- | ------- |
| [input&#95;format&#95;custom&#95;detect&#95;header](../../../operations/settings/settings-formats.md/#input_format_custom_detect_header)                                                   | 이름과 타입이 포함된 헤더가 있으면 자동으로 감지합니다.                                             | `true`  |
| [input&#95;format&#95;custom&#95;skip&#95;trailing&#95;empty&#95;lines](../../../operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines)                   | 파일 끝의 후행 빈 줄을 건너뜁니다.                                                        | `false` |
| [input&#95;format&#95;custom&#95;allow&#95;variable&#95;number&#95;of&#95;columns](../../../operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) | 포맷에서 가변적인 수의 컬럼을 허용하며, 추가 컬럼은 무시하고 누락된 컬럼에는 기본값을 사용합니다. | `false` |