---
description: 'ClickHouse의 중첩 데이터 구조 개요'
sidebar_label: 'Nested(Name1 Type1, Name2 Type2, ...)'
sidebar_position: 57
slug: /sql-reference/data-types/nested-data-structures/nested
title: 'Nested(name1 Type1, Name2 Type2, ...)'
doc_type: 'guide'
---

중첩 데이터 구조는 하나의 셀 안에 있는 테이블과 비슷합니다. 중첩 데이터 구조의 매개변수인 컬럼 이름과 유형은 [CREATE TABLE](../../../sql-reference/statements/create/table.md) 쿼리와 같은 방식으로 지정합니다. 각 테이블 행은 중첩 데이터 구조의 여러 행에 대응할 수 있습니다.

:::tip[컬럼 이름에 점을 사용하지 마십시오]
점이 포함된 컬럼 이름, 동일한 점 접두사를 공유하는 컬럼, 그리고 `Array` 유형 컬럼은 `flatten_nested = 1`(기본값)일 때 각각 평탄화된 Nested 구조의 일부로 해석될 수 있습니다. 이로 인해 삽입 시 예상하지 못한 배열 길이 검증이나 이름 변경 제한이 발생할 수 있습니다.

가능하면 컬럼 이름에 점을 사용하지 마십시오.
의도적으로 `Nested` 의미 체계가 필요한 경우가 아니라면 컬럼 이름에서는 점 대신 밑줄(`_`)이나 다른 구분자를 사용하십시오.
:::

예시:

```sql
CREATE TABLE test.visits(
  CounterID UInt32,
  StartDate Date,
  Sign Int8,
  IsNew UInt8,
  VisitID UInt64,
  UserID UInt64,
--highlight-start
  Goals Nested(
    ID UInt32,
    Serial UInt32,
    EventTime DateTime,
    Price Int64,
    OrderID String,
    CurrencyID UInt32
  )
--highlight-end
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY (StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID));

INSERT INTO test.visits
(CounterID, StartDate, Sign, IsNew, VisitID, UserID, Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-17', 1, 1, 1001, 100001, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 16:38:10', '2014-03-17 16:38:48', '2014-03-17 16:42:27'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1002, 100002, [1073752], [1], ['2014-03-17 00:28:25'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 0, 1003, 100003, [1073752], [1], ['2014-03-17 10:46:20'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 1, 1004, 100004, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:59:20', '2014-03-17 22:17:55', '2014-03-17 22:18:07', '2014-03-17 22:18:51'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1005, 100005, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1006, 100006, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 11:37:06', '2014-03-17 14:07:47', '2014-03-17 14:36:21'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1007, 100007, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1008, 100008, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 1, 1009, 100009, [591325, 1073752], [1, 2], ['2014-03-17 00:46:05', '2014-03-17 00:46:05'], [0, 0], ['', ''], [0, 0]),
    (101500, '2014-03-17', 1, 1, 1010, 100010, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:28:33', '2014-03-17 13:30:26', '2014-03-17 18:51:21', '2014-03-17 18:51:45'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]);
```

위의 `CREATE TABLE` DDL 문은 전환 또는 달성된 목표에 대한 데이터를 포함하는 `Goals` 중첩 데이터 구조를 선언합니다.
&#39;visits&#39; 테이블의 각 행은 0개 이상의 전환에 해당합니다.

[`flatten_nested`](/ko/operations/settings/settings#flatten_nested) 설정을 `0`으로 지정하면(`flatten_nested=1`이 기본값) 임의 깊이의 중첩을 지원합니다.

대부분의 경우 중첩 데이터 구조를 다룰 때는 점(.)으로 구분된 컬럼 이름으로 컬럼을 지정합니다.
이 컬럼들은 서로 대응하는 유형의 배열을 이룹니다.
하나의 중첩 데이터 구조에 속한 모든 컬럼 배열은 길이가 동일합니다.

예를 들면 다음과 같습니다:

```sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
ORDER BY VisitID
LIMIT 10
```

```text
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goals.ID                       ┃ Goals.EventTime                                                                           ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
 1. │ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 2. │ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 3. │ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 4. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 5. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 6. │ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 7. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 8. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 9. │ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
10. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
    └────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

:::tip
중첩 데이터 구조는 길이가 같은 여러 개의 컬럼 배열로 이루어진 것으로 생각하면 가장 이해하기 쉽습니다.
:::

<div id="filtering-nested-columns-in-where">
  ### WHERE 절에서 Nested 컬럼 필터링
</div>

`Nested` 구조의 각 컬럼은 `Array`로 저장되므로, `WHERE` 절에서 이를 참조하면 개별 요소가 아니라 각 행의 전체 배열을 얻게 됩니다. 따라서 Nested 컬럼을 스칼라 값과 직접 비교할 수 없으며, 대신 [배열 함수](/ko/sql-reference/functions/array-functions)를 사용해야 합니다.

예를 들어, 다음 쿼리는 **아무 행도 반환하지 않은 채 조용히 끝나는 것이 아니라** 예외를 발생시킵니다. `Goals.ID`의 유형이 `Array(UInt32)`이고 `equals(Array(UInt32), UInt32)`는 유효한 비교가 아니기 때문입니다:

```sql
-- WRONG: compares the entire Array to a scalar
SELECT * FROM test.visits
WHERE Goals.ID = 591325;
```

```text
Code: 43. DB::Exception: Illegal types of arguments (`Array(UInt32)`, `UInt32`)
of function `equals`. (ILLEGAL_TYPE_OF_ARGUMENT)
```

배열에 특정 값이 있는지 확인하려면 [`has`](/ko/sql-reference/functions/array-functions#has)를 사용합니다:

```sql
-- Find visits that have at least one goal with ID 591325
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE has(Goals.ID, 591325);
```

조건이 더 복잡할 때는 [`arrayExists`](/ko/sql-reference/functions/array-functions#arrayExists)를 사용하십시오:

```sql
-- Find visits that have at least one goal with ID greater than 1000000
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE arrayExists(id -> id > 1000000, Goals.ID);
```

`length`로 배열의 길이를 기준으로 필터링하거나 `notEmpty`로 빈 배열을 제외할 수 있습니다:

```sql
-- Visits with at least 3 goals
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE length(Goals.ID) >= 3;

-- Visits with at least one goal (non-empty array)
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE notEmpty(Goals.ID);
```

전체 행이 아니라 중첩 구조의 개별 요소를 기준으로 필터링하려면 먼저 `ARRAY JOIN`으로 배열을 펼치십시오.
`ARRAY JOIN`을 적용하면 각 요소가 별도의 행이 되므로 `WHERE` 절이 스칼라 값에 적용됩니다.
자세한 내용은 [`ARRAY JOIN` 절](/ko/sql-reference/statements/select/array-join)을 참조하십시오. 예시:

```sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
ORDER BY VisitID, Goal.Serial
LIMIT 10
```

```text
    ┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goal.ID ┃      Goal.EventTime ┃
    ┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
 1. │ 1073752 │ 2014-03-17 16:38:10 │
    ├─────────┼─────────────────────┤
 2. │  591325 │ 2014-03-17 16:38:48 │
    ├─────────┼─────────────────────┤
 3. │  591325 │ 2014-03-17 16:42:27 │
    ├─────────┼─────────────────────┤
 4. │ 1073752 │ 2014-03-17 00:28:25 │
    ├─────────┼─────────────────────┤
 5. │ 1073752 │ 2014-03-17 10:46:20 │
    ├─────────┼─────────────────────┤
 6. │ 1073752 │ 2014-03-17 13:59:20 │
    ├─────────┼─────────────────────┤
 7. │  591325 │ 2014-03-17 22:17:55 │
    ├─────────┼─────────────────────┤
 8. │  591325 │ 2014-03-17 22:18:07 │
    ├─────────┼─────────────────────┤
 9. │  591325 │ 2014-03-17 22:18:51 │
    ├─────────┼─────────────────────┤
10. │ 1073752 │ 2014-03-17 11:37:06 │
    └─────────┴─────────────────────┘
```

`SELECT`로 중첩 데이터 구조 전체를 조회할 수는 없습니다. 그 안에 포함된 개별 컬럼만 명시적으로 나열할 수 있습니다.

<div id="inserting-data">
  ### 데이터 삽입
</div>

`INSERT` 쿼리에서는 중첩 데이터 구조의 모든 구성 컬럼 배열을 각각 별도로 전달해야 합니다(마치 각각이 개별 컬럼 배열인 것처럼). 삽입 시 시스템은 이들의 길이가 모두 같은지 확인합니다.

각 중첩 하위 컬럼은 점 표기법(`Goals.ID`, `Goals.Serial`, ...)을 사용해 컬럼 목록에 나열되며, 해당 값은 배열입니다:

```sql
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    -- A visit with two goals: each nested sub-column gets an array of length 2
    (101500, '2014-03-18', 1, 1, 2001, 200001,
     [1073752, 591325], [1, 2],
     ['2014-03-18 10:00:00', '2014-03-18 10:05:00'],
     [100, 200], ['order_a', 'order_b'], [1, 2]),
    -- A visit with no goals: all nested sub-columns get empty arrays
    (101500, '2014-03-18', 1, 0, 2002, 200002,
     [], [], [], [], [], []);
```

하나의 행에 있는 모든 중첩 하위 컬럼 배열은 길이가 동일해야 합니다. 길이가 서로 다르면 오류가 발생합니다:

```sql
-- ERROR: Goals.ID has 2 elements, but Goals.Serial has 1
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-18', 1, 1, 2003, 200003,
     [1073752, 591325], [1],
     ['2014-03-18 12:00:00'], [0], [''], [0]);
```

`DESCRIBE` 쿼리에서는 중첩 데이터 구조의 컬럼이 동일한 방식으로 각각 나열됩니다.

<div id="alter-limitations">
  ### ALTER 제한 사항
</div>

중첩 데이터 구조에 대한 `ALTER` 쿼리에는 다음과 같은 제한 사항이 있습니다.

**하위 컬럼 추가**는 문제없이 작동합니다. 기존 `Nested` 구조에 새 하위 컬럼을 추가할 수 있습니다:

```sql
ALTER TABLE test.visits ADD COLUMN Goals.Revenue Float64;
```

**하위 컬럼 삭제**는 개별 하위 컬럼에도 적용할 수 있습니다:

```sql
ALTER TABLE test.visits DROP COLUMN Goals.Revenue;
```

**하위 컬럼의 유형 변경**은 가능하며 mutation(데이터 재작성)을 트리거합니다:

```sql
ALTER TABLE test.visits MODIFY COLUMN Goals.Price Int32;
```

**이름 변경**에는 제약이 있습니다. 동일한 중첩 구조 내에서 하위 컬럼의 이름을 변경할 수 있습니다:

```sql
-- OK: stays within the Goals structure
ALTER TABLE test.visits RENAME COLUMN Goals.Price TO Goals.Amount;
```

하지만 다음 작업은 **수행할 수 없습니다**:

* 중첩 구조 전체의 이름을 변경할 수 없습니다(예: `Goals`를 `Conversions`로 변경).
* 하위 컬럼을 다른 중첩 구조로 옮길 수 없습니다(예: `Goals.ID`를 `OtherNested.ID`로 이동).
* 하위 컬럼을 중첩 구조 밖으로 꺼내거나 중첩 구조 안으로 넣을 수 없습니다(예: `Goals.ID`를 `GoalID`로 변경하거나 그 반대).