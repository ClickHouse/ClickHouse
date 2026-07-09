---
alias: []
description: 'CSVWithNamesAndTypes 포맷 문서'
input_format: true
keywords: ['CSVWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/CSVWithNamesAndTypes
title: 'CSVWithNamesAndTypes'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

또한 [TabSeparatedWithNamesAndTypes](../formats/TabSeparatedWithNamesAndTypes)와 마찬가지로 컬럼 이름과 타입이 포함된 헤더 행 2개를 출력합니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

:::tip
[버전](https://github.com/ClickHouse/ClickHouse/releases) 23.1부터 ClickHouse는 `CSV` 포맷 사용 시 CSV 파일의 헤더를 자동으로 감지하므로 `CSVWithNames` 또는 `CSVWithNamesAndTypes`를 사용할 필요가 없습니다.
:::

이름이 `football_types.csv`인 다음 CSV 파일을 사용합니다:

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
Date,Int16,LowCardinality(String),LowCardinality(String),Int8,Int8
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

테이블을 생성합니다:

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

`CSVWithNamesAndTypes` 포맷으로 데이터를 삽입합니다:

```sql
INSERT INTO football FROM INFILE 'football_types.csv' FORMAT CSVWithNamesAndTypes;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

`CSVWithNamesAndTypes` 포맷으로 데이터를 읽습니다:

```sql
SELECT *
FROM football
FORMAT CSVWithNamesAndTypes
```

출력은 컬럼 이름과 타입을 나타내는 2개의 헤더 행이 포함된 CSV 형식입니다:

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"Date","Int16","LowCardinality(String)","LowCardinality(String)","Int8","Int8"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

<div id="format-settings">
  ## 포맷 설정
</div>

:::note
설정 [input&#95;format&#95;with&#95;names&#95;use&#95;header](/ko/operations/settings/settings-formats.md/#input_format_with_names_use_header)의 값이 `1`이면,
입력 데이터의 컬럼이 이름을 기준으로 테이블의 컬럼에 매핑되며, 설정 [input&#95;format&#95;skip&#95;unknown&#95;fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields)의 값이 `1`이면 알 수 없는 이름의 컬럼은 건너뜁니다.
그렇지 않으면 첫 번째 행을 건너뜁니다.
:::

:::note
설정 [input&#95;format&#95;with&#95;types&#95;use&#95;header](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header)의 값이 `1`이면,
입력 데이터의 타입을 테이블의 해당 컬럼 타입과 비교합니다. 그렇지 않으면 두 번째 행을 건너뜁니다.
:::