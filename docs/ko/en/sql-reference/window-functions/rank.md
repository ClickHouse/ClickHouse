---
description: 'rank 윈도 함수 문서'
sidebar_label: 'rank'
sidebar_position: 6
slug: /sql-reference/window-functions/rank
title: 'rank'
doc_type: 'reference'
---

현재 행이 속한 파티션 내에서 중간 순위를 건너뛰는 방식으로 순위를 매깁니다. 즉, 처리 중인 행의 값이 앞선 행의 값과 같으면 해당 행에는 앞선 행과 동일한 순위가 부여됩니다.
그다음 행의 순위는 이전 행의 순위에, 그 이전 순위가 부여된 횟수만큼 건너뛴 값을 더한 순위가 됩니다.

[dense&#95;rank](./dense_rank.md) 함수는 동일하게 동작하지만 순위 사이에 간격이 없습니다.

**구문**

```sql
rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

윈도 함수 구문에 대한 자세한 내용은 [윈도 함수 - 구문](./index.md/#syntax)을 참조하십시오.

**반환 값**

* 해당 파티션에서 현재 행의 번호로, 중간에 비는 순번을 포함합니다. [UInt64](../data-types/int-uint.md).

**예시**

다음 예시는 동영상 가이드 [ClickHouse에서 윈도 함수 순위 매기기](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA)에 나온 예시를 바탕으로 합니다.

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
       rank() OVER (ORDER BY salary DESC) AS rank
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─rank─┐
1. │ Gary Chen       │ 195000 │    1 │
2. │ Robert George   │ 195000 │    1 │
3. │ Charles Juarez  │ 190000 │    3 │
4. │ Douglas Benson  │ 150000 │    4 │
5. │ Michael Stanley │ 150000 │    4 │
6. │ Scott Harrison  │ 150000 │    4 │
7. │ James Henderson │ 140000 │    7 │
   └─────────────────┴────────┴──────┘
```