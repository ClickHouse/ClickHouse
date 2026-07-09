---
alias: ['TSVWithNames']
description: 'TabSeparatedWithNames 포맷에 대한 참고 문서'
input_format: true
keywords: ['TabSeparatedWithNames']
output_format: true
slug: /interfaces/formats/TabSeparatedWithNames
title: 'TabSeparatedWithNames'
doc_type: '참고'
---

| 입력 | 출력 | 별칭             |
| -- | -- | -------------- |
| ✔  | ✔  | `TSVWithNames` |

<div id="description">
  ## 설명
</div>

첫 번째 행에 컬럼 이름이 작성된다는 점에서 [`TabSeparated`](./TabSeparated.md) 포맷과 다릅니다.

파싱 시 첫 번째 행에는 컬럼 이름이 있어야 합니다. 컬럼 이름을 사용해 각 컬럼의 위치를 파악하고 이름이 올바른지 확인할 수 있습니다.

:::note
설정 [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header)가 `1`로 설정되면,
입력 데이터의 컬럼은 이름을 기준으로 테이블의 컬럼에 매핑되며, 설정 [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields)가 `1`로 설정된 경우 알 수 없는 이름의 컬럼은 건너뜁니다.
그렇지 않으면 첫 번째 행은 건너뜁니다.
:::

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

이름이 `football.tsv`인 다음 TSV 파일을 사용합니다:

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
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedWithNames;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

`TabSeparatedWithNames` 포맷으로 데이터를 읽습니다:

```sql
SELECT *
FROM football
FORMAT TabSeparatedWithNames
```

출력은 탭으로 구분된 포맷입니다:

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
