---
description: 'lag 윈도 함수에 대한 문서'
sidebar_label: 'lag'
sidebar_position: 9
slug: /sql-reference/window-functions/lag
title: 'lag'
doc_type: '참고'
---

정렬된 프레임 내에서 현재 행보다 지정된 물리적 OFFSET만큼 앞선 행에서 평가한 값을 반환합니다.
이 함수는 [`lagInFrame`](./lagInFrame.md)과 비슷하지만, 항상 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` 프레임을 사용합니다.

**구문**

```sql
lag(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

윈도 함수 구문에 관한 자세한 내용은 [Window Functions - Syntax](./index.md/#syntax)를 참조하십시오.

**매개변수**

* `x` — 컬럼 이름.
* `offset` — 적용할 오프셋. [(U)Int*](../data-types/int-uint.md). (선택 사항 - 기본값은 `1`)
* `default` — 계산된 행이 윈도우 프레임의 경계를 벗어날 경우 반환할 값. (선택 사항 - 생략하면 컬럼 타입의 기본값)

**반환 값**

* 정렬된 프레임에서 현재 행보다 지정된 물리적 오프셋만큼 앞선 행에서 평가된 값.

**예시**

이 예시는 특정 주식의 과거 데이터를 살펴보고 `lag` 함수를 사용하여 종가의 일별 변동분과 백분율 변화를 계산합니다.

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
    lag(close, 1, close) OVER (ORDER BY date ASC) AS previous_day_close,
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