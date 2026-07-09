---
description: 'lagInFrame 윈도 함수 문서'
sidebar_label: 'lagInFrame'
sidebar_position: 9
slug: /sql-reference/window-functions/lagInFrame
title: 'lagInFrame'
doc_type: 'reference'
---

정렬된 프레임 내에서 현재 행보다 지정된 물리적 오프셋만큼 앞에 있는 행을 기준으로 계산된 값을 반환합니다.

:::warning
`lagInFrame`의 동작은 표준 SQL `lag` 윈도 함수와 다릅니다.
ClickHouse 윈도 함수 `lagInFrame`은 윈도 프레임을 따릅니다.
`lag`와 동일한 동작을 얻으려면 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`을 사용하십시오.
:::

**구문**

```sql
lagInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

윈도 함수 구문에 대한 자세한 내용은 [윈도우 함수 - 구문](./index.md/#syntax)을 참조하십시오.

**매개변수**

* `x` — 컬럼 이름.
* `offset` — 적용할 오프셋. [(U)Int*](../data-types/int-uint.md). (선택 사항 - 기본값은 `1`).
* `default` — 계산된 행이 윈도우 프레임 경계를 벗어날 경우 반환할 값. (선택 사항 - 생략하면 컬럼 타입의 기본값이 사용됨).

**반환 값**

* 정렬된 프레임 내에서 현재 행보다 지정한 물리적 오프셋만큼 앞선 행에서 평가된 값입니다.

**예시**

이 예시에서는 특정 주식의 과거 데이터를 살펴보고 `lagInFrame` 함수를 사용해 종가의 일별 delta와 백분율 변화를 계산합니다.

```sql title="Query"
CREATE TABLE stock_prices
(
    `date`   Date,
    `open`   Float32, -- opening price
    `high`   Float32, -- daily high
    `low`    Float32, -- daily low
    `close`  Float32, -- closing price
    `volume` UInt32   -- trade volume
)
Engine = Memory;

INSERT INTO stock_prices FORMAT Values
    ('2024-06-03', 113.62, 115.00, 112.00, 115.00, 438392000),
    ('2024-06-04', 115.72, 116.60, 114.04, 116.44, 403324000),
    ('2024-06-05', 118.37, 122.45, 117.47, 122.44, 528402000),
    ('2024-06-06', 124.05, 125.59, 118.32, 121.00, 664696000),
    ('2024-06-07', 119.77, 121.69, 118.02, 120.89, 412386000);
```

```sql title="Query"
SELECT
    date,
    close,
    lagInFrame(close, 1, close) OVER (ORDER BY date ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
     ) AS previous_day_close,
    COALESCE(ROUND(close - previous_day_close, 2)) AS delta,
    COALESCE(ROUND((delta / previous_day_close) * 100, 2)) AS percent_change
FROM stock_prices
ORDER BY date DESC
```

```response title="Response"
   ┌───────date─┬──close─┬─previous_day_close─┬─delta─┬─percent_change─┐
1. │ 2024-06-07 │ 120.89 │                121 │ -0.11 │          -0.09 │
2. │ 2024-06-06 │    121 │             122.44 │ -1.44 │          -1.18 │
3. │ 2024-06-05 │ 122.44 │             116.44 │     6 │           5.15 │
4. │ 2024-06-04 │ 116.44 │                115 │  1.44 │           1.25 │
5. │ 2024-06-03 │    115 │                115 │     0 │              0 │
   └────────────┴────────┴────────────────────┴───────┴────────────────┘
```