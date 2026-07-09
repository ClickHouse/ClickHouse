---
description: 'last_value 윈도 함수 문서'
sidebar_label: 'last_value'
sidebar_position: 4
slug: /sql-reference/window-functions/last_value
title: 'last_value'
doc_type: 'reference'
---

정렬된 프레임 내에서 평가된 마지막 값을 반환합니다. 기본적으로는 NULL 인수를 건너뛰지만, 이 동작은 `RESPECT NULLS` 수정자를 사용해 재정의할 수 있습니다.

**구문**

```sql
last_value (column_name) [[RESPECT NULLS] | [IGNORE NULLS]]
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

별칭: `anyLast`.

:::note
`first_value(column_name)` 뒤에 선택적 수정자 `RESPECT NULLS`를 사용하면 `NULL` 인수를 건너뛰지 않습니다.
자세한 내용은 [NULL 처리](../aggregate-functions/index.md/#null-processing)를 참조하십시오.

별칭: `lastValueRespectNulls`
:::

윈도우 함수 구문에 대한 자세한 내용은 [윈도우 함수 - 구문](./index.md/#syntax)을 참조하십시오.

**반환 값**

* 정렬된 프레임 내에서 평가된 마지막 값입니다.

**예시**

이 예시에서는 `last_value` 함수를 사용하여 프리미어리그 축구 선수 급여로 구성된 가상의 데이터셋에서 가장 낮은 급여를 받는 선수를 찾습니다.

```sql title="Query"
DROP TABLE IF EXISTS salaries;
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT VALUES
    ('Port Elizabeth Barbarians', 'Gary Chen', 196000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 100000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 180000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       last_value(player) OVER (ORDER BY salary DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_paid_player
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─lowest_paid_player─┐
1. │ Gary Chen       │ 196000 │ Michael Stanley    │
2. │ Robert George   │ 195000 │ Michael Stanley    │
3. │ Charles Juarez  │ 190000 │ Michael Stanley    │
4. │ Scott Harrison  │ 180000 │ Michael Stanley    │
5. │ Douglas Benson  │ 150000 │ Michael Stanley    │
6. │ James Henderson │ 140000 │ Michael Stanley    │
7. │ Michael Stanley │ 100000 │ Michael Stanley    │
   └─────────────────┴────────┴────────────────────┘
```