---
alias: ['TSVRawWithNames', 'RawWithNames']
description: 'TabSeparatedRawWithNames 포맷 문서'
input_format: true
keywords: ['TabSeparatedRawWithNames', 'TSVRawWithNames', 'RawWithNames']
output_format: true
slug: /interfaces/formats/TabSeparatedRawWithNames
title: 'TabSeparatedRawWithNames'
doc_type: '참고'
---

| 입력 | 출력 | 별칭                                |
| -- | -- | --------------------------------- |
| ✔  | ✔  | `TSVRawWithNames`, `RawWithNames` |

<div id="description">
  ## 설명
</div>

[`TabSeparatedWithNames`](./TabSeparatedWithNames.md) 포맷과는 달리,
행을 이스케이프 처리하지 않고 작성합니다.

:::note
이 포맷으로 파싱할 때는 각 필드에 탭이나 줄바꿈 문자를 사용할 수 없습니다.
:::

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

다음 TSV 파일 `football.tsv`를 사용합니다:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
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
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRawWithNames;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

다음과 같이 `TabSeparatedRawWithNames` 포맷으로 데이터를 읽습니다:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRawWithNames
```

출력은 한 줄 헤더가 있는 탭 구분 포맷입니다.

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
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
