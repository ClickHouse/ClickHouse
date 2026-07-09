---
alias: []
description: 'JSONColumns 포맷 문서'
input_format: true
keywords: ['JSONColumns']
output_format: true
slug: /interfaces/formats/JSONColumns
title: 'JSONColumns'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

:::tip
`JSONColumns*` 포맷의 출력은 먼저 ClickHouse 필드 이름을 표시한 다음, 해당 필드에 대한 테이블의 각 행 내용을 표시합니다;
시각적으로는 데이터가 왼쪽으로 90도 회전한 형태입니다.
:::

이 포맷에서는 모든 데이터가 하나의 JSON 객체로 표현됩니다.

:::note
`JSONColumns` 포맷은 모든 데이터를 메모리에 버퍼링한 뒤 하나의 블록으로 출력하므로 메모리 사용량이 크게 증가할 수 있습니다.
:::

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

다음 데이터가 들어 있는 `football.json` JSON 파일을 사용합니다:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

데이터를 삽입하세요:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONColumns;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

`JSONColumns` 포맷을 사용해 데이터를 읽습니다:

```sql
SELECT *
FROM football
FORMAT JSONColumns
```

출력은 JSON 포맷으로 제공됩니다:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

<div id="format-settings">
  ## 포맷 설정
</div>

가져오는 동안 이름을 알 수 없는 컬럼은 [`input_format_skip_unknown_fields`](/ko/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) 설정이 `1`로 지정된 경우 건너뜁니다.
블록에 없는 컬럼은 기본값으로 채워집니다(이 경우 [`input_format_defaults_for_omitted_fields`](/ko/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) 설정을 사용할 수 있습니다)