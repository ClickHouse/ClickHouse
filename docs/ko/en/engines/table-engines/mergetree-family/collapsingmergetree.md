---
description: 'MergeTree를 상속하지만, 머지 과정 중 행을 축약하는 로직을
  추가한 엔진입니다.'
keywords: ['업데이트', '축약']
sidebar_label: 'CollapsingMergeTree'
sidebar_position: 70
slug: /engines/table-engines/mergetree-family/collapsingmergetree
title: 'CollapsingMergeTree 테이블 엔진'
doc_type: 'guide'
---

<div id="description">
  ## 설명
</div>

`CollapsingMergeTree` 엔진은 [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)를 상속하고,
머지 과정에서 행을 축약하는 로직을 추가합니다.
`CollapsingMergeTree` 테이블 엔진은 비동기적으로
정렬 키(`ORDER BY`)의 모든 필드가 특수 필드 `Sign`만 제외하고 같을 경우 행의 쌍을 삭제(축약)합니다.
`Sign`은 `1` 또는 `-1` 값을 가질 수 있습니다.
반대 값을 가진 `Sign`의 짝이 없는 행은 유지됩니다.

자세한 내용은 문서의 [Collapsing](#table_engine-collapsingmergetree-collapsing) 섹션을 참조하십시오.

:::note
이 엔진은 저장소 사용량을 크게 줄일 수 있으며,
그 결과 `SELECT` 쿼리의 효율을 높일 수 있습니다.
:::

<div id="parameters">
  ## 매개변수
</div>

`Sign` 매개변수를 제외한 이 테이블 엔진의 모든 매개변수는
[`MergeTree`](/ko/engines/table-engines/mergetree-family/mergetree)와 동일한 의미를 가집니다.

* `Sign` — `1`이 &quot;상태&quot; 행이고 `-1`이 &quot;취소&quot; 행인 행 유형을 나타내는 컬럼에 부여하는 이름입니다. 유형: [Int8](/ko/sql-reference/data-types/int-uint).

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">
  <summary>테이블 생성의 Deprecated 메서드</summary>

  :::note
  아래 메서드는 새 프로젝트에서 사용하지 않는 것이 좋습니다.
  가능하면 기존 프로젝트를 업데이트하여 새 메서드를 사용하십시오.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) 
  ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
  ```

  `Sign` — `1`이 &quot;상태&quot; 행이고 `-1`이 &quot;취소&quot; 행인 행 유형의 컬럼 이름입니다. [Int8](/ko/sql-reference/data-types/int-uint).
</details>

* 쿼리 매개변수에 대한 설명은 [쿼리 설명](../../../sql-reference/statements/create/table.md)을 참조하십시오.
* `CollapsingMergeTree` 테이블을 생성할 때는 `MergeTree` 테이블을 생성할 때와 동일한 [쿼리 절](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)이 필요합니다.

<div id="table_engine-collapsingmergetree-collapsing">
  ## 축약
</div>

<div id="data">
  ### 데이터
</div>

주어진 객체에 대해 계속 변경되는 데이터를 저장해야 하는 상황을 생각해 보겠습니다.
객체마다 행 하나를 두고 변경이 있을 때마다 갱신하는 방식이 합리적으로 보일 수 있지만,
갱신 작업은 저장소의 데이터를 다시 써야 하므로 DBMS에서는 비용이 크고 느립니다.
데이터를 빠르게 써야 한다면 대량의 갱신을 수행하는 방식은 적절하지 않습니다.
하지만 객체의 변경 사항을 순차적으로 기록하는 것은 언제나 가능합니다.
이를 위해 특수한 컬럼 `Sign`을 사용합니다.

* `Sign` = `1`이면 해당 행은 &quot;상태&quot; 행을 뜻합니다: *현재 유효한 상태를 나타내는 필드들을 포함한 행*.
* `Sign` = `-1`이면 해당 행은 &quot;취소&quot; 행을 뜻합니다: *동일한 속성을 가진 객체의 상태를 취소하는 데 사용되는 행*.

예를 들어, 어떤 웹사이트에서 사용자가 몇 개의 페이지를 조회했고 각 페이지에 얼마나 오래 머물렀는지 계산하려고 합니다.
특정 시점에 다음과 같은 사용자 활동 상태 행을 기록합니다:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

이후 어느 시점에 사용자 활동의 변경을 반영하고, 이를 다음 2개의 행으로 기록합니다:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

첫 번째 행은 객체(이 경우 사용자)의 이전 상태를 취소합니다.
&quot;취소된&quot; 행의 모든 정렬 키 필드를 `Sign`을 제외하고 복사해야 합니다.
위의 두 번째 행에는 현재 상태가 들어 있습니다.

사용자 활동의 마지막 상태만 필요하므로, 아래와 같이 원래의 &quot;상태&quot; 행과 삽입한 &quot;취소&quot;
행을 삭제하여 객체의 유효하지 않은(오래된) 상태를 축약할 수 있습니다:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree`는 데이터 파트의 머지가 일어나는 동안 바로 이러한 *축약* 동작을 수행합니다.

:::note
각 변경에 왜 2개의 행이 필요한지는
[알고리즘](#table_engine-collapsingmergetree-collapsing-algorithm) 단락에서 더 자세히 설명합니다.
:::

**이러한 접근 방식의 특징**

1. 데이터를 쓰는 프로그램은 객체의 상태를 취소할 수 있도록 그 상태를 기억해야 합니다. &quot;취소&quot; 행에는 &quot;상태&quot;의 정렬 키 필드 복사본과 반대 부호의 `Sign`이 포함되어야 합니다. 이렇게 하면 초기 저장소 크기는 커지지만 데이터를 빠르게 쓸 수 있습니다.
2. 컬럼에서 길이가 계속 늘어나는 배열은 쓰기 부하를 증가시켜 엔진의 효율을 떨어뜨립니다. 데이터가 단순할수록 효율이 높아집니다.
3. `SELECT` 결과는 객체 변경 이력의 일관성에 크게 좌우됩니다. 데이터를 삽입할 수 있도록 준비할 때는 주의해야 합니다. 데이터가 일관되지 않으면 예측할 수 없는 결과가 나올 수 있습니다. 예를 들어 session depth와 같이 음수가 될 수 없는 메트릭에 음수 값이 들어갈 수 있습니다.

<div id="table_engine-collapsingmergetree-collapsing-algorithm">
  ### 알고리즘
</div>

ClickHouse가 데이터 [파트](/ko/concepts/glossary#parts)를 머지할 때,
같은 정렬 키(`ORDER BY`)를 가진 연속된 각 행 그룹은 최대 2개의 행으로 축약됩니다.
즉, `Sign` = `1`인 &quot;상태&quot; 행과 `Sign` = `-1`인 &quot;취소&quot; 행입니다.
다시 말해, ClickHouse에서는 항목이 축약됩니다.

각 결과 데이터 파트에 대해 ClickHouse는 다음을 저장합니다.

|    |                                                                                                                                             |
| -- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. | &quot;상태&quot; 행과 &quot;취소&quot; 행의 수가 같고 마지막 행이 &quot;상태&quot; 행인 경우, 첫 번째 &quot;취소&quot; 행과 마지막 &quot;상태&quot; 행을 저장합니다. |
| 2. | &quot;상태&quot; 행이 &quot;취소&quot; 행보다 많은 경우, 마지막 &quot;상태&quot; 행을 저장합니다.                                                          |
| 3. | &quot;취소&quot; 행이 &quot;상태&quot; 행보다 많은 경우, 첫 번째 &quot;취소&quot; 행을 저장합니다.                                                        |
| 4. | 그 밖의 모든 경우에는 어떤 행도 저장하지 않습니다.                                                                                                               |

또한 &quot;상태&quot; 행이 &quot;취소&quot; 행보다 2개 이상 많거나,
반대로 &quot;취소&quot; 행이 &quot;상태&quot; 행보다 2개 이상 많으면 머지는 계속 진행됩니다.
하지만 ClickHouse는 이 상황을 논리 오류로 간주하고 서버 로그에 기록합니다.
이 오류는 동일한 데이터가 두 번 이상 삽입되면 발생할 수 있습니다.
따라서 축약은 통계 계산 결과를 바꾸지 않아야 합니다.
변경 사항은 점진적으로 축약되므로, 최종적으로는 거의 모든 객체에 대해 마지막 상태만 남습니다.

머지 알고리즘은
같은 정렬 키를 가진 모든 행이 동일한 결과 데이터 파트에, 심지어 동일한 물리적 서버에 있게 된다는 것을 보장하지 않으므로 `Sign` 컬럼이 필요합니다.
ClickHouse는 여러 스레드로 `SELECT` 쿼리를 처리하므로 결과에서 행의 순서를 예측할 수 없습니다.

`CollapsingMergeTree` 테이블에서 완전히 &quot;축약된&quot; 데이터를 얻어야 한다면 집계가 필요합니다.
축약을 완료하려면 부호를 고려하는 `GROUP BY` 절과 집계 함수를 사용해 쿼리를 작성하십시오.
예를 들어 수량을 계산할 때는 `count()` 대신 `sum(Sign)`를 사용하십시오.
어떤 값의 합계를 계산할 때는 아래 [예시](#example-of-use)와 같이 `sum(x)` 대신 `sum(Sign * x)`와 `HAVING sum(Sign) > 0`을 함께 사용하십시오.

집계 `count`, `sum`, `avg`는 이 방식으로 계산할 수 있습니다.
객체에 축약되지 않은 상태가 하나 이상 있으면 집계 `uniq`도 계산할 수 있습니다.
하지만 집계 `min`과 `max`는 계산할 수 없습니다.
`CollapsingMergeTree`는 축약된 상태의 이력을 저장하지 않기 때문입니다.

:::note
집계 없이 데이터를 추출해야 하는 경우
(예를 들어, 최신 값이 특정 조건과 일치하는 행이 있는지 확인하는 경우),
`FROM` 절에 [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) 수정자를 사용할 수 있습니다. 결과를 반환하기 전에 데이터를 머지합니다.
CollapsingMergeTree의 경우 각 키에 대해 가장 최신 상태 행만 반환됩니다.
:::

<div id="examples">
  ## 예시
</div>

<div id="example-of-use">
  ### 사용 예시
</div>

다음 예시 데이터가 있다고 가정합니다:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree`를 사용해 테이블 `UAct`를 생성하겠습니다:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

다음으로 데이터를 삽입해 보겠습니다:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

서로 다른 2개의 데이터 파트를 만들기 위해 `INSERT` 쿼리 2개를 사용합니다.

:::note
단일 쿼리로 데이터를 삽입하면 ClickHouse는 데이터 파트 1개만 생성하며, 이후에는 머지를 전혀 수행하지 않습니다.
:::

다음과 같이 데이터를 조회할 수 있습니다:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

위에서 반환된 데이터를 살펴보며 축약이 발생했는지 확인해 보겠습니다...
두 개의 `INSERT` 쿼리로 두 개의 데이터 파트(data part)를 생성했습니다.
`SELECT` 쿼리는 2개의 스레드에서 수행되었으며, 행은 무작위 순서로 반환되었습니다.
하지만 아직 데이터 파트 머지가 일어나지 않았기 때문에 축약은 **발생하지 않았습니다**
그리고 ClickHouse는 예측할 수 없는 시점에 백그라운드에서 데이터 파트를 머지합니다.

따라서 집계가 필요합니다.
이는 [`sum`](/ko/sql-reference/aggregate-functions/reference/sum) 집계 함수와
[`HAVING`](/ko/sql-reference/statements/select/having) 절을 사용해 수행합니다:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

집계가 필요 없고 축약을 강제로 수행하려면 `FROM` 절에 `FINAL` 수정자를 사용할 수도 있습니다.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

:::note
이 방식으로 데이터를 선택하는 것은 효율이 떨어지며, 스캔하는 데이터가 많은 경우(수백만 행)에는 권장되지 않습니다.
:::

<div id="example-of-another-approach">
  ### 다른 접근 방식의 예시
</div>

이 접근 방식의 핵심은 머지 시 키 필드만 고려된다는 점입니다.
따라서 &quot;cancel&quot; 행에서는 음수 값을 지정할 수 있으며,
이렇게 하면 `Sign` 컬럼을 사용하지 않고 합계를 계산할 때 이전 버전의 행을 상쇄할 수 있습니다.

이 예시에서는 아래의 예시 데이터를 사용합니다:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

이 접근 방식에서는 음수 값을 저장할 수 있도록 `PageViews`와 `Duration`의 데이터 타입을 변경해야 합니다.
따라서 `collapsingMergeTree`를 사용해 테이블 `UAct`를 생성할 때
이 컬럼들의 데이터 타입을 `UInt8`에서 `Int16`으로 변경합니다:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

테이블에 데이터를 삽입해 이 접근 방식을 테스트해 보겠습니다.

다만 예시 수준이거나 테이블이 작은 경우에는 허용됩니다:

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```