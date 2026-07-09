---
description: 'dense_rank 윈도 함수에 대한 문서'
sidebar_label: 'dense_rank'
sidebar_position: 7
slug: /sql-reference/window-functions/dense_rank
title: 'dense_rank'
doc_type: 'reference'
---

파티션 내에서 현재 행의 순위를 중간 순위의 건너뜀 없이 매깁니다. 즉, 새로 확인한 행의 값이 이전 행들 중 하나의 값과 같으면 순위 번호를 건너뛰지 않고 바로 다음 순위를 부여합니다.

[rank](./rank.md) 함수도 동일하게 동작하지만, 순위 사이에 건너뛴 번호가 생깁니다.

**구문**

별칭: `denseRank` (대소문자 구분)

```sql
dense_rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

윈도 함수 구문에 대한 자세한 내용은 [윈도우 함수 - 구문](./index.md/#syntax)을 참조하십시오.

**반환 값**

* 파티션 내 현재 행의 순위를 나타내는 숫자입니다. 순위에는 공백이 없습니다. [UInt64](../data-types/int-uint.md).

**예시**

다음 예시는 동영상 안내 자료 [ClickHouse에서 윈도 함수 순위 매기기](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA)에서 제공된 예시를 기반으로 합니다.

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
       dense_rank() OVER (ORDER BY salary DESC) AS dense_rank
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─dense_rank─┐
1. │ Gary Chen       │ 195000 │          1 │
2. │ Robert George   │ 195000 │          1 │
3. │ Charles Juarez  │ 190000 │          2 │
4. │ Michael Stanley │ 150000 │          3 │
5. │ Douglas Benson  │ 150000 │          3 │
6. │ Scott Harrison  │ 150000 │          3 │
7. │ James Henderson │ 140000 │          4 │
   └─────────────────┴────────┴────────────┘
```