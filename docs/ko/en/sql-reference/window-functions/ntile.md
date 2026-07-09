---
description: 'ntile 윈도 함수 문서'
sidebar_label: 'ntile'
sidebar_position: 13
slug: /sql-reference/window-functions/ntile
title: 'ntile'
doc_type: 'reference'
---

파티션 내에서 정렬된 행을 지정된 개수의 버킷(그룹)으로 가능한 한 균등하게 나누고, 현재 행이 속한 버킷 번호를 반환합니다. 버킷 번호는 1부터 시작합니다. 각 파티션에서 행은 순서대로 버킷에 할당됩니다. 행 수가 버킷 수로 나누어떨어지지 않으면 앞쪽 버킷에는 뒤쪽 버킷보다 행이 1개 더 할당됩니다.

**구문**

```sql
ntile (buckets)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

인수 `buckets`는 양의 정수 상수여야 합니다.

`ORDER BY` 절이 필요합니다. 윈도우 프레임은 전체 파티션(`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`)이어야 합니다. 또한 이는 명시적으로 지정하지 않을 때 기본으로 사용되는 프레임입니다.

윈도우 함수 구문에 대한 자세한 내용은 다음을 참조하십시오: [윈도우 함수 - 구문](./index.md/#syntax).

**반환 값**

* 파티션 내 현재 행의 버킷 번호입니다. [UInt64](../data-types/int-uint.md).

**예시**

다음 예시에서는 선수들을 급여 내림차순으로 정렬한 뒤 4개의 버킷으로 나눕니다.

```sql title="Query"
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       ntile(4) OVER (ORDER BY salary DESC, player ASC) AS bucket
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─bucket─┐
1. │ Gary Chen       │ 195000 │      1 │
2. │ Robert George   │ 195000 │      1 │
3. │ Charles Juarez  │ 190000 │      2 │
4. │ Douglas Benson  │ 150000 │      2 │
5. │ Michael Stanley │ 150000 │      3 │
6. │ Scott Harrison  │ 150000 │      3 │
7. │ James Henderson │ 140000 │      4 │
   └─────────────────┴────────┴────────┘
```

여기에는 7개의 행과 4개의 버킷이 있으므로 처음 3개의 버킷에는 각각 2개의 행이 들어가고, 마지막 버킷에는 1개의 행이 들어갑니다.