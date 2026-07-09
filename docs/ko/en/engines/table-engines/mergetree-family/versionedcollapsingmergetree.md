---
description: '지속적으로 변경되는 객체 상태를 빠르게 저장하고,
  이전 객체 상태를 백그라운드에서 삭제할 수 있습니다.'
sidebar_label: 'VersionedCollapsingMergeTree'
sidebar_position: 80
slug: /engines/table-engines/mergetree-family/versionedcollapsingmergetree
title: 'VersionedCollapsingMergeTree 테이블 엔진'
doc_type: '참고'
---

이 엔진은 다음과 같은 기능을 제공합니다.

* 지속적으로 변경되는 객체 상태를 빠르게 저장할 수 있습니다.
* 이전 객체 상태를 백그라운드에서 삭제합니다. 이렇게 하면 저장 공간 사용량이 크게 줄어듭니다.

자세한 내용은 [축약](#table_engines_versionedcollapsingmergetree) 섹션을 참조하십시오.

이 엔진은 [MergeTree](/ko/engines/table-engines/mergetree-family/mergetree)를 상속하며, 데이터 파트를 머지하는 알고리즘에 행 축약 로직을 추가합니다. `VersionedCollapsingMergeTree`는 [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md)와 같은 목적을 갖지만, 여러 스레드를 사용해 데이터를 임의의 순서로 삽입할 수 있도록 하는 다른 축약 알고리즘을 사용합니다. 특히 `Version` 컬럼은 행이 잘못된 순서로 삽입되더라도 올바르게 축약되도록 도와줍니다. 반면 `CollapsingMergeTree`는 엄격하게 연속된 삽입만 허용합니다.

<div id="creating-a-table">
  ## 테이블 생성하기
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

쿼리 매개변수에 대한 설명은 [쿼리 설명](../../../sql-reference/statements/create/table.md)을 참고하십시오.

<div id="engine-parameters">
  ### 엔진 매개변수
</div>

```sql
VersionedCollapsingMergeTree(sign, version)
```

| 매개변수      | 설명                                                                                | 유형                                                                                                                                                                                                                                                                                             |
| --------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sign`    | 행의 유형을 나타내는 컬럼의 이름입니다. `1`은 &quot;state&quot; 행이고, `-1`은 &quot;cancel&quot; 행입니다. | [`Int8`](/ko/sql-reference/data-types/int-uint)                                                                                                                                                                                                                                                   |
| `version` | 객체 상태의 버전을 나타내는 컬럼의 이름입니다.                                                        | [`Int*`](/ko/sql-reference/data-types/int-uint), [`UInt*`](/ko/sql-reference/data-types/int-uint), [`Date`](/ko/sql-reference/data-types/date), [`Date32`](/ko/sql-reference/data-types/date32), [`DateTime`](/ko/sql-reference/data-types/datetime), 또는 [`DateTime64`](/ko/sql-reference/data-types/datetime64) |

<div id="query-clauses">
  ### 쿼리 절
</div>

`VersionedCollapsingMergeTree` 테이블을 생성할 때는 `MergeTree` 테이블을 생성할 때와 동일한 [절](../../../engines/table-engines/mergetree-family/mergetree.md)이 필요합니다.

<details markdown="1">
  <summary>더 이상 권장되지 않는 테이블 생성 방법</summary>

  :::note
  새 프로젝트에서는 이 방법을 사용하지 마십시오. 가능하면 기존 프로젝트도 위에서 설명한 방법으로 전환하십시오.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
  ```

  `sign` 및 `version`을 제외한 모든 매개변수의 의미는 `MergeTree`와 같습니다.

  * `sign` — 행 유형을 나타내는 컬럼 이름입니다. `1`은 &quot;state&quot; 행이고 `-1`은 &quot;cancel&quot; 행입니다.

    컬럼 데이터 타입 — `Int8`.

  * `version` — 객체 상태 버전을 나타내는 컬럼 이름입니다.

    컬럼 데이터 타입은 `UInt*`여야 합니다.
</details>

<div id="table_engines_versionedcollapsingmergetree">
  ## 축약
</div>

<div id="data">
  ### 데이터
</div>

어떤 객체에 대해 계속 변경되는 데이터를 저장해야 하는 상황을 생각해 보겠습니다. 보통은 객체마다 하나의 행을 두고 변경이 있을 때마다 그 행을 업데이트하는 방식이 합리적입니다. 하지만 업데이트 작업은 저장소의 데이터를 다시 써야 하므로 DBMS에서는 비용이 많이 들고 속도도 느립니다. 데이터를 빠르게 써야 한다면 업데이트는 적합하지 않지만, 다음과 같이 객체의 변경 사항을 순차적으로 기록할 수 있습니다.

행을 기록할 때는 `Sign` 컬럼을 사용합니다. `Sign = 1`이면 해당 행이 객체의 상태를 나타낸다는 의미입니다(이를 &quot;state&quot; 행이라고 하겠습니다). `Sign = -1`이면 동일한 속성을 가진 객체 상태의 취소를 나타냅니다(이를 &quot;cancel&quot; 행이라고 하겠습니다). 또한 `Version` 컬럼도 사용하는데, 이 컬럼은 객체의 각 상태를 서로 다른 번호로 식별해야 합니다.

예를 들어, 어떤 사이트에서 사용자가 몇 개의 페이지를 방문했고 얼마나 오래 머물렀는지 계산하려는 경우를 생각해 보겠습니다. 어느 시점에 다음과 같이 사용자 활동 상태를 나타내는 행을 기록합니다:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

이후 어느 시점에 사용자 활동의 변경 사항을 등록한 뒤, 이를 다음 2개의 행으로 기록합니다.

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

첫 번째 행(row)은 객체(user)의 이전 상태를 무효화합니다. `Sign`을 제외하고 무효화되는 상태의 모든 필드(field)를 복사해야 합니다.

두 번째 행에는 현재 상태가 들어 있습니다.

사용자 활동의 마지막 상태만 필요하므로, 이 행들은

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

삭제할 수 있으며, 이 과정에서 객체의 유효하지 않은(이전) 상태가 축약됩니다. `VersionedCollapsingMergeTree`는 데이터 파트를 머지하는 동안 이를 수행합니다.

각 변경마다 왜 2개의 행이 필요한지 알아보려면 [알고리즘](#table_engines-versionedcollapsingmergetree-algorithm)을 참조하십시오.

**사용 시 참고 사항**

1. 데이터를 쓰는 프로그램은 객체의 상태를 취소할 수 있도록 해당 상태를 기억해야 합니다. &quot;Cancel&quot; 문자열에는 기본 키 필드의 복사본, &quot;state&quot; 문자열의 버전, 그리고 반대 부호의 `Sign`이 포함되어야 합니다. 이렇게 하면 초기 저장소 크기는 늘어나지만 데이터를 빠르게 쓸 수 있습니다.
2. 컬럼에 길게 계속 증가하는 배열이 있으면 쓰기 부하로 인해 엔진 효율이 낮아집니다. 데이터는 단순할수록 효율이 높아집니다.
3. `SELECT` 결과는 객체 변경 이력의 일관성에 크게 좌우됩니다. 삽입할 데이터를 준비할 때는 특히 정확해야 합니다. 데이터에 일관성이 없으면 세션 깊이처럼 음수가 될 수 없는 메트릭에 음수 값이 나타나는 등 예측할 수 없는 결과가 발생할 수 있습니다.

<div id="table_engines-versionedcollapsingmergetree-algorithm">
  ### 알고리즘
</div>

ClickHouse가 데이터 파트(data parts)를 머지할 때, 기본 키(primary key)와 버전이 같고 `Sign` 값이 다른 각 행 쌍을 삭제합니다. 행의 순서는 중요하지 않습니다.

ClickHouse가 데이터를 삽입할 때는 기본 키를 기준으로 행을 정렬합니다. `Version` 컬럼이 기본 키에 포함되어 있지 않으면, ClickHouse는 이를 암묵적으로 기본 키의 마지막 필드에 추가한 뒤 정렬에 사용합니다.

<div id="selecting-data">
  ## 데이터 선택
</div>

ClickHouse는 동일한 기본 키(primary key)를 가진 모든 행이 같은 결과 데이터 파트에 있거나, 심지어 같은 물리 서버에 있을 것이라고 보장하지 않습니다. 이는 데이터를 쓸 때뿐 아니라 이후 데이터 파트를 머지할 때도 마찬가지입니다. 또한 ClickHouse는 여러 스레드로 `SELECT` 쿼리를 처리하므로 결과의 행 순서를 예측할 수 없습니다. 즉, `VersionedCollapsingMergeTree` 테이블에서 완전히 &quot;축약된&quot; 데이터를 얻으려면 집계가 필요합니다.

축약을 최종 반영하려면 부호를 고려하는 `GROUP BY` 절과 집계 함수를 사용하는 쿼리를 작성하십시오. 예를 들어 수량을 계산할 때는 `count()` 대신 `sum(Sign)`를 사용합니다. 어떤 값의 합계를 계산할 때는 `sum(x)` 대신 `sum(Sign * x)`를 사용하고, `HAVING sum(Sign) > 0`을 추가합니다.

집계 함수인 `count`, `sum`, `avg`는 이 방식으로 계산할 수 있습니다. 집계 함수 `uniq`는 객체에 축약되지 않은 상태가 하나 이상 남아 있으면 계산할 수 있습니다. 집계 함수 `min`과 `max`는 `VersionedCollapsingMergeTree`가 축약된 상태 값의 이력을 저장하지 않기 때문에 계산할 수 없습니다.

집계 없이도 &quot;축약&quot;이 반영된 데이터를 추출해야 하는 경우(예를 들어 최신 값이 특정 조건과 일치하는 행이 존재하는지 확인하는 경우), `FROM` 절에 `FINAL` 수정자를 사용할 수 있습니다. 이 방법은 비효율적이므로 큰 테이블에는 사용하지 않아야 합니다.

<div id="example-of-use">
  ## 사용 예시
</div>

예시 데이터:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

테이블 생성:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

데이터 삽입하기:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

서로 다른 2개의 데이터 파트를 만들기 위해 `INSERT` 쿼리 2개를 사용합니다. 데이터를 단일 쿼리로 삽입하면 ClickHouse는 데이터 파트 1개만 생성하며, 머지는 수행되지 않습니다.

데이터 가져오기:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

여기서는 무엇이 보이고, 축약된 파트는 어디에 있을까요?
두 개의 `INSERT` 쿼리를 사용해 두 개의 데이터 파트를 생성했습니다. `SELECT` 쿼리는 두 개의 스레드에서 수행되었으며, 그 결과 행이 무작위 순서로 반환됩니다.
데이터 파트가 아직 머지되지 않았기 때문에 축약은 발생하지 않았습니다. ClickHouse는 예측할 수 없는 시점에 데이터 파트를 머지합니다.

이 때문에 집계가 필요합니다:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

집계가 필요 없고 축약을 강제로 수행하려면 `FROM` 절에 `FINAL` 수정자를 사용할 수 있습니다.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

이는 데이터를 조회하는 매우 비효율적인 방법입니다. 대규모 테이블에는 사용하지 마십시오.