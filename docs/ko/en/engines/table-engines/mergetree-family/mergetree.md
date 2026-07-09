---
description: '`MergeTree` 계열 테이블 엔진은 높은 데이터 수집 속도와 대규모 데이터 양을 처리하도록 설계되었습니다.'
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: 'MergeTree 테이블 엔진'
doc_type: '참고'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mergetree-table-engine">
  # MergeTree 테이블 엔진
</div>

`MergeTree` 엔진과 `MergeTree` 계열의 다른 엔진(예: `ReplacingMergeTree`, `AggregatingMergeTree`)은 ClickHouse에서 가장 널리 사용되며 가장 안정적인 테이블 엔진입니다.

`MergeTree` 계열 테이블 엔진은 높은 데이터 수집 속도와 대규모 데이터 볼륨을 처리하도록 설계되었습니다.
삽입 작업을 수행하면 테이블 파트가 생성되며, 이 파트는 백그라운드 프로세스에 의해 다른 테이블 파트와 머지됩니다.

`MergeTree` 계열 테이블 엔진의 주요 기능은 다음과 같습니다.

* 테이블의 프라이머리 키(primary key)는 각 테이블 파트 내 정렬 순서(클러스터형 인덱스)를 결정합니다. 또한 프라이머리 키는 개별 행이 아니라 그래뉼이라고 하는 8192개 행의 블록을 참조합니다. 따라서 방대한 데이터 집합의 프라이머리 키도 메인 메모리에 유지할 수 있을 만큼 작게 유지하면서, 디스크상의 데이터에도 빠르게 접근할 수 있습니다.

* 테이블은 임의의 파티션 표현식(partition expression)을 사용해 파티션으로 나눌 수 있습니다. 파티션 프루닝을 통해 쿼리 조건에 따라 읽지 않아도 되는 파티션은 제외됩니다.

* 데이터는 고가용성, 장애 조치, 무중단 업그레이드를 위해 여러 클러스터 노드에 복제할 수 있습니다. [데이터 복제](/ko/engines/table-engines/mergetree-family/replication.md)를 참조하십시오.

* `MergeTree` 테이블 엔진은 쿼리 최적화에 도움이 되도록 다양한 종류의 통계와 샘플링 메서드를 지원합니다.

:::note
이름은 비슷하지만 [Merge](/ko/engines/table-engines/special/merge) 엔진은 `*MergeTree` 엔진과 다릅니다.
:::

<div id="table_engine-mergetree-creating-a-table">
  ## 테이블 만들기
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

매개변수에 대한 자세한 설명은 [CREATE TABLE](/ko/sql-reference/statements/create/table.md) SQL 문을 참고하십시오

<div id="mergetree-query-clauses">
  ### 쿼리 절
</div>

<div id="engine">
  #### ENGINE
</div>

`ENGINE` — 엔진의 이름과 매개변수를 지정합니다. `ENGINE = MergeTree()`. `MergeTree` 엔진에는 매개변수가 없습니다.

<div id="order_by">
  #### ORDER BY
</div>

`ORDER BY` — 정렬 키입니다.

컬럼 이름 또는 임의의 표현식으로 이루어진 튜플입니다. 예시: `ORDER BY (CounterID + 1, EventDate)`.

프라이머리 키(primary key)가 정의되지 않은 경우(즉, `PRIMARY KEY`가 지정되지 않은 경우) ClickHouse는 정렬 키를 프라이머리 키로 사용합니다.

정렬이 필요하지 않으면 `ORDER BY tuple()` 구문을 사용할 수 있습니다.
또는 `create_table_empty_primary_key_by_default` 설정이 활성화되어 있으면 `CREATE TABLE` SQL 문에 `ORDER BY ()`가 암묵적으로 추가됩니다. [프라이머리 키 선택하기](#selecting-a-primary-key)를 참조하십시오.

<div id="partition-by">
  #### PARTITION BY
</div>

`PARTITION BY` — [파티셔닝 키](/ko/engines/table-engines/mergetree-family/custom-partitioning-key.md)입니다. 선택 사항입니다. 대부분의 경우 파티션 키는 필요하지 않으며, 파티셔닝이 필요하더라도 일반적으로 월 단위보다 더 세분화된 파티션 키는 필요하지 않습니다. 파티셔닝은 쿼리 속도를 높이지 않습니다(`ORDER BY` 표현식과는 달리). 지나치게 세분화된 파티셔닝은 절대 사용하지 마십시오. 데이터를 클라이언트 식별자나 이름을 기준으로 파티셔닝하지 마십시오(대신 클라이언트 식별자 또는 이름을 `ORDER BY` 표현식의 첫 번째 컬럼으로 지정하십시오).

월 단위로 파티셔닝하려면 `toYYYYMM(date_column)` 표현식을 사용하십시오. 여기서 `date_column`은 [Date](/ko/sql-reference/data-types/date.md) 타입의 날짜가 들어 있는 컬럼입니다. 이 경우 파티션 이름의 포맷은 `"YYYYMM"`입니다.

<div id="primary-key">
  #### PRIMARY KEY
</div>

`PRIMARY KEY` — [정렬 키와 다른 경우](#choosing-a-primary-key-that-differs-from-the-sorting-key)의 프라이머리 키입니다. 선택 사항입니다.

정렬 키를 지정하면(`ORDER BY` 절 사용) 프라이머리 키도 암묵적으로 지정됩니다.
일반적으로 정렬 키와 별도로 프라이머리 키를 지정할 필요는 없습니다.

<div id="sample-by">
  #### SAMPLE BY
</div>

`SAMPLE BY` — 샘플링 표현식입니다. 선택 사항입니다.

지정한 경우 프라이머리 키에 포함되어야 합니다.
샘플링 표현식의 결과는 부호 없는 정수여야 합니다.

예시: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

<div id="ttl">
  #### TTL
</div>

`TTL` — 행의 저장 기간과 [디스크 및 볼륨 간](#table_engine-mergetree-multiple-volumes) 자동 파트 이동 로직을 지정하는 규칙 목록입니다. 선택 사항입니다.

표현식의 결과는 `Date` 또는 `DateTime`이어야 합니다. 예를 들어 `TTL date + INTERVAL 1 DAY`입니다.

규칙 유형 `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY`는 표현식 조건이 충족될 때(현재 시간에 도달할 때) 파트에 수행할 작업을 지정합니다. 즉, 만료된 행 삭제, 파트를 지정된 디스크(`TO DISK 'xxx'`) 또는 볼륨(`TO VOLUME 'xxx'`)으로 이동(파트의 모든 행에 대해 표현식 조건이 충족되는 경우), 또는 만료된 행의 값을 집계하는 작업입니다. 규칙의 기본 유형은 삭제(`DELETE`)입니다. 여러 규칙을 지정할 수 있지만, `DELETE` 규칙은 1개만 지정할 수 있습니다.

자세한 내용은 [컬럼 및 테이블의 TTL](#table_engine-mergetree-ttl)을 참조하십시오.

<div id="settings">
  #### 설정
</div>

[MergeTree 설정](../../../operations/settings/merge-tree-settings.md)을 참조하십시오.

**섹션 설정 예시**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

예시에서는 월별 파티셔닝을 설정합니다.

또한 사용자 ID를 기준으로 한 hash를 샘플링 표현식으로 설정합니다. 이렇게 하면 각 `CounterID` 및 `EventDate`별로 테이블 데이터를 의사 난수 방식으로 분산시킬 수 있습니다. 데이터를 조회할 때 [SAMPLE](/ko/sql-reference/statements/select/sample) 절을 지정하면 ClickHouse는 사용자 부분 집합에 대해 고르게 분포된 의사 무작위 데이터 샘플을 반환합니다.

8192가 기본값이므로 `index_granularity` 설정은 생략할 수 있습니다.

<details markdown="1">
  <summary>테이블 생성의 Deprecated 메서드</summary>

  :::note
  새 프로젝트에서는 이 메서드를 사용하지 마십시오. 가능하면 기존 프로젝트도 위에서 설명한 메서드로 전환하십시오.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  **MergeTree() 매개변수**

  * `date-column` — [Date](/ko/sql-reference/data-types/date.md) 타입 컬럼의 이름입니다. ClickHouse는 이 컬럼을 기준으로 월별 파티션을 자동으로 생성합니다. 파티션 이름의 포맷은 `"YYYYMM"`입니다.
  * `sampling_expression` — 샘플링 표현식입니다.
  * `(primary, key)` — 프라이머리 키입니다. 유형: [Tuple()](/ko/sql-reference/data-types/tuple.md)
  * `index_granularity` — 인덱스의 세분화 수준입니다. 인덱스의 &quot;marks&quot; 사이에 있는 데이터 행 수를 의미합니다. 대부분의 작업에는 8192 값이 적절합니다.

  **예시**

  ```sql
  MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
  ```

  `MergeTree` 엔진은 기본 엔진 구성 메서드의 위 예시와 동일한 방식으로 구성됩니다.
</details>

<div id="mergetree-data-storage">
  ## 데이터 저장
</div>

테이블은 프라이머리 키로 정렬된 데이터 파트로 구성됩니다.

테이블에 데이터를 삽입하면 별도의 데이터 파트가 생성되며, 각 파트는 프라이머리 키를 기준으로 사전식으로 정렬됩니다. 예를 들어 프라이머리 키가 `(CounterID, Date)`이면, 파트의 데이터는 `CounterID`로 정렬되고 각 `CounterID` 내에서는 `Date` 순으로 정렬됩니다.

서로 다른 파티션에 속하는 데이터는 서로 다른 파트로 분리됩니다. 백그라운드에서 ClickHouse는 더 효율적으로 저장하기 위해 데이터 파트를 머지합니다. 서로 다른 파티션에 속한 파트는 머지되지 않습니다. 머지 메커니즘은 동일한 프라이머리 키를 가진 모든 행이 같은 데이터 파트에 포함된다고 보장하지는 않습니다.

데이터 파트는 `Wide` 또는 `Compact` 포맷으로 저장할 수 있습니다. `Wide` 포맷에서는 각 컬럼이 파일 시스템의 별도 파일에 저장되고, `Compact` 포맷에서는 모든 컬럼이 하나의 파일에 저장됩니다. `Compact` 포맷은 작고 빈번한 삽입 성능을 높이는 데 사용할 수 있습니다.

데이터 저장 포맷은 테이블 엔진의 `min_bytes_for_wide_part` 및 `min_rows_for_wide_part` 설정으로 제어됩니다. 데이터 파트의 바이트 수 또는 행 수가 해당 설정값보다 작으면 그 파트는 `Compact` 포맷으로 저장됩니다. 그렇지 않으면 `Wide` 포맷으로 저장됩니다. 이 설정이 모두 지정되지 않으면 데이터 파트는 `Wide` 포맷으로 저장됩니다.

각 데이터 파트는 논리적으로 그래뉼로 나뉩니다. 그래뉼은 데이터를 조회할 때 ClickHouse가 읽는 가장 작은 불가분의 데이터 집합입니다. ClickHouse는 행이나 값을 분할하지 않으므로 각 그래뉼에는 항상 정수 개수의 행이 포함됩니다. 그래뉼의 첫 번째 행에는 해당 행의 프라이머리 키 값이 마크로 기록됩니다. 각 데이터 파트에 대해 ClickHouse는 마크를 저장하는 인덱스 파일을 생성합니다. 각 컬럼에 대해서도 프라이머리 키 포함 여부와 관계없이 ClickHouse는 동일한 마크를 저장합니다. 이러한 마크를 사용하면 컬럼 파일에서 데이터를 직접 찾을 수 있습니다.

그래뉼 크기는 테이블 엔진의 `index_granularity` 및 `index_granularity_bytes` 설정에 의해 제한됩니다. 그래뉼의 행 수는 행 크기에 따라 `[1, index_granularity]` 범위에 있습니다. 단일 행의 크기가 설정값보다 큰 경우 그래뉼 크기는 `index_granularity_bytes`를 초과할 수 있습니다. 이 경우 그래뉼의 크기는 해당 행의 크기와 같습니다.

<div id="primary-keys-and-indexes-in-queries">
  ## 쿼리에서의 프라이머리 키와 인덱스
</div>

`(CounterID, Date)` 프라이머리 키(primary key)를 예시로 들면, 정렬과 인덱스는 다음과 같이 나타낼 수 있습니다:

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

데이터 쿼리에서 다음과 같이 지정한 경우:

* `CounterID in ('a', 'h')`인 경우 서버는 마크 범위 `[0, 3)` 및 `[6, 8)`의 데이터를 읽습니다.
* `CounterID IN ('a', 'h') AND Date = 3`인 경우 서버는 마크 범위 `[1, 3)` 및 `[7, 8)`의 데이터를 읽습니다.
* `Date = 3`인 경우 서버는 마크 범위 `[1, 10]`의 데이터를 읽습니다.

위 예시에서 알 수 있듯이 전체 스캔(full scan)보다 인덱스(index)를 사용하는 편이 항상 더 효율적입니다.

희소 인덱스(sparse index)를 사용하면 추가 데이터가 함께 읽힐 수 있습니다. 프라이머리 키(primary key)의 단일 범위를 읽을 때는 각 데이터 블록(data block)에서 최대 `index_granularity * 2`개의 추가 행이 읽힐 수 있습니다.

희소 인덱스는 대부분의 경우 컴퓨터의 RAM에 들어갈 만큼 크기가 작기 때문에, 매우 많은 수의 테이블 행을 처리할 수 있게 해줍니다.

ClickHouse는 고유한 프라이머리 키를 요구하지 않습니다. 동일한 프라이머리 키를 가진 여러 행을 삽입할 수 있습니다.

`PRIMARY KEY` 및 `ORDER BY` 절에서 `Nullable` 유형의 표현식을 사용할 수는 있지만, 강력히 비권장됩니다. 이 기능을 허용하려면 [allow&#95;nullable&#95;key](/ko/operations/settings/merge-tree-settings/#allow_nullable_key) 설정을 활성화하십시오. `ORDER BY` 절의 `NULL` 값에는 [NULLS&#95;LAST](/ko/sql-reference/statements/select/order-by.md/#sorting-of-special-values) 원칙이 적용됩니다.

<div id="selecting-a-primary-key">
  ### 프라이머리 키 선택하기
</div>

프라이머리 키(primary key)의 컬럼 수에는 명시적인 제한이 없습니다. 데이터 구조에 따라 프라이머리 키에 더 많은 컬럼을 포함할 수도 있고 더 적은 컬럼을 포함할 수도 있습니다. 이에 따라 다음과 같은 효과를 얻을 수 있습니다.

* 인덱스(index) 성능이 향상될 수 있습니다.

  프라이머리 키가 `(a, b)`인 경우, 다음 조건을 만족하면 컬럼 `c`를 추가했을 때 성능이 향상됩니다.

  * 컬럼 `c`에 대한 조건이 있는 쿼리가 있습니다.
  * `(a, b)` 값이 동일한 긴 데이터 범위(`index_granularity`보다 몇 배 더 긴 범위)가 자주 나타납니다. 다시 말해, 다른 컬럼을 추가하면 상당히 긴 데이터 범위를 건너뛸 수 있습니다.

* 데이터 압축이 향상될 수 있습니다.

  ClickHouse는 프라이머리 키를 기준으로 데이터를 정렬하므로, 일관성이 높을수록 압축 효율이 좋아집니다.

* [CollapsingMergeTree](/ko/engines/table-engines/mergetree-family/collapsingmergetree) 및 [SummingMergeTree](/ko/engines/table-engines/mergetree-family/summingmergetree.md) 엔진에서 데이터 파트를 머지할 때 추가적인 로직을 제공할 수 있습니다.

  이 경우에는 프라이머리 키와 다른 *정렬 키(sorting key)*를 지정하는 것이 적절합니다.

프라이머리 키가 길면 삽입 성능과 메모리 사용량에 부정적인 영향을 미치지만, 프라이머리 키에 추가된 컬럼은 `SELECT` 쿼리 중 ClickHouse 성능에 영향을 주지 않습니다.

`ORDER BY tuple()` 구문을 사용하면 프라이머리 키 없이 테이블을 생성할 수 있습니다. 이 경우 ClickHouse는 데이터를 삽입된 순서대로 저장합니다. `INSERT ... SELECT` 쿼리로 데이터를 삽입할 때 데이터 순서를 유지하려면 [max&#95;insert&#95;threads = 1](/ko/operations/settings/settings#max_insert_threads)을 설정하십시오.

초기 순서대로 데이터를 조회하려면 [단일 스레드](/ko/operations/settings/settings.md/#max_threads) `SELECT` 쿼리를 사용하십시오.

<div id="choosing-a-primary-key-that-differs-from-the-sorting-key">
  ### 정렬 키와 다른 프라이머리 키 선택
</div>

정렬 키(sorting key)와 다른 프라이머리 키(primary key)도 지정할 수 있습니다. 프라이머리 키는 각 mark에 대해 인덱스 파일에 기록되는 값을 갖는 표현식입니다. 이 경우 프라이머리 키 표현식 튜플은 정렬 키 표현식 튜플의 prefix여야 합니다.

이 기능은 [SummingMergeTree](/ko/engines/table-engines/mergetree-family/summingmergetree.md) 및
[AggregatingMergeTree](/ko/engines/table-engines/mergetree-family/aggregatingmergetree.md) 테이블 엔진을 사용할 때 유용합니다. 이러한 엔진을 사용하는 일반적인 사례에서는 테이블에 *차원*과 *측정값*이라는 두 가지 타입의 컬럼이 있습니다. 일반적인 쿼리에서는 임의의 `GROUP BY`와 차원 기준 필터링을 사용해 측정값 컬럼의 값을 집계합니다. SummingMergeTree와 AggregatingMergeTree는 정렬 키 값이 같은 행을 집계하므로, 정렬 키에 모든 차원을 추가하는 것이 자연스럽습니다. 그 결과 키 표현식은 긴 컬럼 목록으로 이루어지며, 새 차원이 추가될 때마다 이 목록을 자주 업데이트해야 합니다.

이 경우 효율적인 범위 스캔을 제공하는 몇 개의 컬럼만 프라이머리 키에 남기고, 나머지 차원 컬럼은 정렬 키 튜플에 추가하는 것이 합리적입니다.

정렬 키의 [ALTER](/ko/sql-reference/statements/alter/index.md)는 lightweight 작업입니다. 새 컬럼을 테이블과 정렬 키에 동시에 추가하는 경우 기존 데이터 파트는 변경할 필요가 없기 때문입니다. 이전 정렬 키는 새 정렬 키의 prefix이고, 새로 추가된 컬럼에는 데이터가 없으므로 테이블 수정 시점에는 데이터가 이전 정렬 키와 새 정렬 키 모두에 따라 정렬된 상태입니다.

<div id="use-of-indexes-and-partitions-in-queries">
  ### 쿼리에서 인덱스와 파티션 사용
</div>

`SELECT` 쿼리의 경우 ClickHouse는 인덱스를 사용할 수 있는지 분석합니다. `WHERE/PREWHERE` 절에 동등 비교 또는 부등 비교 연산을 나타내는 표현식이 있거나(전체 또는 conjunction 요소 중 하나로 포함된 경우), 프라이머리 키 또는 파티셔닝 키에 포함된 컬럼이나 표현식, 이러한 컬럼의 특정 부분 반복 함수, 또는 이러한 표현식의 논리 관계에 대해 고정된 접두사를 사용하는 `IN` 또는 `LIKE` 조건이 있으면 인덱스를 사용할 수 있습니다.

따라서 프라이머리 키의 하나 이상의 범위에 대해 쿼리를 빠르게 실행할 수 있습니다. 이 예시에서는 특정 추적 태그에 대해서나, 특정 태그와 날짜 범위에 대해서나, 특정 태그와 날짜에 대해서나, 여러 태그와 날짜 범위에 대해서 쿼리를 실행할 때 빠르게 동작합니다.

다음과 같이 구성된 엔진을 살펴보겠습니다:

```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

이 경우 쿼리에서는:

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse는 프라이머리 키 인덱스를 사용해 조건에 맞지 않는 데이터를 걸러내고, 월별 파티셔닝 키를 사용해 날짜 범위에 맞지 않는 파티션을 제외합니다.

위의 쿼리는 복잡한 표현식에도 인덱스가 사용된다는 점을 보여줍니다. 테이블 읽기는 인덱스를 사용하는 방식이 전체 스캔보다 느려지지 않도록 구성됩니다.

아래 예시에서는 인덱스를 사용할 수 없습니다.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

쿼리를 실행할 때 ClickHouse가 인덱스를 사용할 수 있는지 확인하려면 [force&#95;index&#95;by&#95;date](/ko/operations/settings/settings.md/#force_index_by_date) 및 [force&#95;primary&#95;key](/ko/operations/settings/settings#force_primary_key) 설정을 사용하십시오.

월 단위 파티셔닝 키를 사용하면 해당 범위의 날짜가 포함된 데이터 블록만 읽을 수 있습니다. 이 경우 데이터 블록에는 여러 날짜의 데이터가 들어 있을 수 있으며(최대 한 달 전체), 블록 내 데이터는 프라이머리 키(primary key)로 정렬되지만 첫 번째 컬럼에 날짜가 포함되지 않을 수도 있습니다. 따라서 프라이머리 키 prefix를 지정하지 않고 날짜 조건만 사용하는 쿼리는 단일 날짜를 조회하는 경우보다 더 많은 데이터를 읽게 됩니다.

<div id="use-of-index-for-deterministic-expressions-in-primary-keys">
  ### 프라이머리 키의 결정적 표현식에 인덱스 사용
</div>

프라이머리 키에는 컬럼 이름뿐 아니라 표현식도 포함할 수 있습니다. 이러한 표현식은 단순한 함수 체인으로만 제한되지 않습니다. 결정적이기만 하면 임의의 표현식 트리(예: 중첩 함수와 복합 표현식)도 사용할 수 있습니다.

표현식은 동일한 입력값에 대해 항상 동일한 결과를 반환할 때 **결정적**이라고 합니다(예: `length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()`이며, `now()`나 `rand()`는 해당하지 않습니다). 프라이머리 키에 결정적 표현식이 포함되어 있으면 ClickHouse는 이를 쿼리의 상수값에 적용한 뒤, 그 결과를 바탕으로 프라이머리 키 인덱스에 대한 조건을 구성할 수 있습니다. 그러면 `=`, `IN`, `has`와 같은 프레디케이트에 대해 데이터 스키핑이 가능해집니다.

일반적인 사용 사례는 프라이머리 키를 간결하게 유지하는 것입니다(예: 긴 `String` 대신 해시를 저장). 그러면서도 원래 컬럼에 대한 프레디케이트가 계속 인덱스를 사용할 수 있습니다.

결정적이지만(단사 함수는 아닌) 프라이머리 키의 예시:

```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

인덱스를 사용할 수 있는 프레디케이트의 예시:

```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

이러한 경우 ClickHouse는 `length('alice')`(및 기타 상수)를 한 번만 계산하고, 그 길이 값을 사용해 프라이머리 키(primary key) 인덱스의 범위를 좁힙니다. 문자열의 길이는 **injective**가 아니므로 서로 다른 `user_id` 문자열이 같은 길이를 가질 수 있으며, 그 결과 인덱스가 추가 그래뉼(false positives)을 읽을 수 있습니다. 원래 프레디케이트(`user_id = ...`, `IN` 등)는 읽은 후에도 계속 적용되므로 결과의 정확성은 유지됩니다.

결정적 표현식이 **injective**이기도 한 경우(사용된 인수 타입에서 서로 다른 입력이 같은 출력을 만들 수 없는 경우), ClickHouse는 부정 형태인 `!=`, `NOT IN`, `NOT has(...)`에도 인덱스를 효과적으로 사용할 수 있습니다. 예를 들어 `reverse(p)`와 `hex(p)`는 `String`에 대해 injective입니다.

injective 프라이머리 키의 예시:

```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

더 복잡한 단사 표현식도 지원합니다. 예시는 다음과 같습니다:

```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

인덱스를 사용할 수 있는 프레디케이트 예시:

```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

<div id="use-of-index-for-partially-monotonic-primary-keys">
  ### 부분 단조 프라이머리 키에 대한 인덱스 사용
</div>

예를 들어 월의 날짜를 생각해 보겠습니다. 날짜는 한 달 범위에서는 [단조 시퀀스](https://en.wikipedia.org/wiki/Monotonic_function)를 이루지만, 더 긴 기간으로 보면 단조적이지 않습니다. 이런 시퀀스를 부분 단조 시퀀스라고 합니다. 사용자가 부분 단조 프라이머리 키(primary key)로 테이블을 생성하면 ClickHouse는 평소와 같이 희소 인덱스를 생성합니다. 사용자가 이러한 테이블에서 데이터를 조회하면 ClickHouse는 쿼리 조건을 분석합니다. 사용자가 인덱스의 두 마크 사이에 있는 데이터를 가져오려 하고, 그 두 마크가 모두 한 달 안에 있다면, 이 경우 ClickHouse는 인덱스를 사용할 수 있습니다. 쿼리 매개변수와 인덱스 마크 사이의 거리를 계산할 수 있기 때문입니다.

쿼리 매개변수 범위에 있는 프라이머리 키 값이 단조 시퀀스를 이루지 않으면 ClickHouse는 인덱스를 사용할 수 없습니다. 이 경우 ClickHouse는 전체 스캔 방식을 사용합니다.

ClickHouse는 이 로직을 월의 날짜 시퀀스에만 적용하는 것이 아니라, 부분 단조 시퀀스를 나타내는 모든 프라이머리 키에 적용합니다.

<div id="table_engine-mergetree-data_skipping-indexes">
  ### 데이터 스키핑 인덱스
</div>

인덱스 선언은 `CREATE` 쿼리의 컬럼 섹션에 포함됩니다.

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

`*MergeTree` 계열 테이블에서는 데이터 스킵 인덱스를 지정할 수 있습니다.

이 인덱스는 `granularity_value`개의 그래뉼로 구성된 블록에 대해 지정된 표현식의 일부 정보를 집계합니다(그래뉼의 크기는 테이블 엔진의 `index_granularity` 설정으로 지정합니다). 그런 다음 이러한 집계는 `SELECT` 쿼리에서 사용되며, `where` 조건을 만족할 수 없는 큰 데이터 블록을 스킵하여 디스크에서 읽어야 하는 데이터 양을 줄입니다.

`GRANULARITY` 절은 생략할 수 있으며, `granularity_value`의 기본값은 1입니다.

**예시**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

예시의 인덱스를 사용하면 ClickHouse는 다음 쿼리에서 디스크에서 읽어야 하는 데이터 양을 줄일 수 있습니다.

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

데이터 스키핑 인덱스는 복합 컬럼에 대해서도 생성할 수 있습니다:

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

<div id="skip-index-types">
  ### 스킵 인덱스 유형
</div>

`MergeTree` 테이블 엔진은 다음 스킵 인덱스 유형을 지원합니다.
스킵 인덱스를 성능 최적화에 활용하는 방법에 대한 자세한 내용은
[&quot;ClickHouse 데이터 스키핑 인덱스 이해하기&quot;](/ko/optimize/skipping-indexes)를 참조하십시오.

* [`MinMax`](#minmax) 인덱스
* [`Set`](#set) 인덱스
* [`bloom_filter`](#bloom-filter) 인덱스
* [`ngrambf_v1`](#n-gram-bloom-filter) 인덱스 *(지원 중단됨)*
* [`tokenbf_v1`](#token-bloom-filter) 인덱스 *(지원 중단됨)*
* [`text`](#text) 인덱스
* [`vector_similarity`](#vector-similarity) 인덱스

<div id="minmax">
  #### MinMax 스킵 인덱스
</div>

각 인덱스 그래뉼(index granule)에는 표현식의 최솟값과 최댓값이 저장됩니다.
(표현식의 유형이 `tuple`이면 각 tuple 요소의 최솟값과 최댓값이 저장됩니다.)

```text title="Syntax"
minmax
```

<div id="set">
  #### Set
</div>

각 인덱스 그래뉼마다 지정된 표현식의 고유 값이 최대 `max_rows`개 저장됩니다.
`max_rows = 0`은 &quot;모든 고유 값을 저장&quot;함을 의미합니다.

```text title="Syntax"
set(max_rows)
```

<div id="bloom-filter">
  #### 블룸 필터
</div>

각 인덱스 그래뉼에 대해 지정된 컬럼용 [블룸 필터](https://en.wikipedia.org/wiki/Bloom_filter)를 저장합니다.

```text title="Syntax"
bloom_filter([false_positive_rate])
```

`false_positive_rate` 매개변수는 0과 1 사이의 값을 가질 수 있으며(기본값: `0.025`), 양성 판정이 발생할 확률을 지정합니다(이 값이 높을수록 읽어야 하는 데이터 양이 증가합니다).

다음 데이터 타입을 지원합니다:

* `(U)Int*`
* `Float*`
* `Enum`
* `Date`
* `DateTime`
* `String`
* `FixedString`
* `Array`
* `LowCardinality`
* `Nullable`
* `UUID`
* `Map`

:::note Map 데이터 타입: 키 또는 값에 대한 인덱스 생성 지정
`Map` 데이터 타입의 경우, 클라이언트는 [`mapKeys`](/ko/sql-reference/functions/tuple-map-functions.md/#mapKeys) 또는 [`mapValues`](/ko/sql-reference/functions/tuple-map-functions.md/#mapValues) 함수를 사용해 인덱스를 키에 대해 생성할지, 값에 대해 생성할지 지정할 수 있습니다.
:::

:::note JSON 데이터 타입: JSON 경로 인덱싱
[`JSON`](/ko/sql-reference/data-types/newjson) 데이터 타입의 경우, [`JSONAllPaths`](/ko/sql-reference/functions/json-functions#JSONAllPaths) 함수를 사용해 경로 집합에 대해 블룸 필터 인덱스를 생성할 수 있습니다. 이를 통해 쿼리한 JSON 경로가 없는 그래뉼을 스키핑할 수 있습니다. 자세한 내용은 [JSON용 데이터 스키핑 인덱스](/ko/sql-reference/data-types/newjson#data-skipping-indexes-for-json)를 참조하십시오.
:::

<div id="n-gram-bloom-filter">
  #### N-gram 블룸 필터 *(지원 중단 예정)*
</div>

:::note
ClickHouse 버전 26.2부터 `text` 인덱스가 일반 제공(GA)되면서 `ngrambf_v1` 인덱스는 더 이상 전문 검색에 권장되지 않습니다.

자세한 내용은 [&quot;텍스트 인덱스를 사용한 전문 검색&quot;](./textindexes.md) 페이지를 참조하십시오.
:::

각 인덱스 그래뉼마다 지정된 [컬럼](https://en.wikipedia.org/wiki/Bloom_filter)의 [n-그램](https://en.wikipedia.org/wiki/N-gram)에 대한 [블룸 필터](https://en.wikipedia.org/wiki/Bloom_filter)를 저장합니다.

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| 매개변수                            | 설명                                                                                         |
| ------------------------------- | ------------------------------------------------------------------------------------------ |
| `n`                             | ngram 크기                                                                                   |
| `size_of_bloom_filter_in_bytes` | 바이트 단위의 블룸 필터 크기입니다. 이 값에는 큰 수를 사용할 수 있습니다. 예를 들어 `256` 또는 `512`를 사용할 수 있는데, 잘 압축되기 때문입니다. |
| `number_of_hash_functions`      | 블룸 필터에 사용되는 해시 함수의 개수입니다.                                                                  |
| `random_seed`                   | 블룸 필터 해시 함수의 시드입니다.                                                                        |

이 인덱스는 다음 데이터 타입에서만 작동합니다.

* [`String`](/ko/sql-reference/data-types/string.md)
* [`FixedString`](/ko/sql-reference/data-types/fixedstring.md)
* [`Map`](/ko/sql-reference/data-types/map.md)

`ngrambf_v1`의 매개변수를 추정하려면 다음 [사용자 정의 함수(UDF)](/ko/sql-reference/statements/create/function.md)를 사용할 수 있습니다.

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

이 함수들을 사용하려면 최소 2개의 매개변수를 지정해야 합니다:

* `total_number_of_all_grams`
* `probability_of_false_positives`

예를 들어 그래뉼에 `4300`개의 ngram이 있고, 오탐률이 `0.0001`보다 낮아야 한다고 가정합니다.
그러면 다음 쿼리를 실행해 나머지 매개변수를 추정할 수 있습니다:

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

물론 이러한 함수들을 사용해 다른 조건의 매개변수도 추정할 수 있습니다.
위 함수는 [여기](https://hur.st/bloomfilter)의 블룸 필터 계산기를 기반으로 합니다.

<div id="token-bloom-filter">
  #### 토큰 블룸 필터
</div>

:::note
ClickHouse 26.2 버전부터 `text` 인덱스가 일반 제공(GA)됨에 따라, `tokenbf_v1` 인덱스는 더 이상 전문 검색에 권장되지 않습니다.

자세한 내용은 [&quot;텍스트 인덱스를 사용한 전문 검색&quot;](./textindexes.md) 페이지를 참조하십시오.
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="sparse-grams-bloom-filter">
  #### 희소 grams 블룸 필터
</div>

희소 grams 블룸 필터는 `ngrambf_v1`와 비슷하지만, ngrams 대신 [희소 grams 토큰](/ko/sql-reference/functions/string-functions.md/#sparseGrams)을 사용합니다.

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="text">
  ### 텍스트 인덱스
</div>

토큰화된 문자열 데이터에 역색인(inverted index)을 구축하여 효율적이고 결정적인 전문 검색을 수행할 수 있게 합니다. 자세한 내용은 [여기](textindexes.md)를 참조하십시오.

<div id="vector-similarity">
  #### 벡터 유사도
</div>

근사 최근접 이웃 검색을 지원합니다. 자세한 내용은 [여기](annindexes.md)에서 확인할 수 있습니다.

<div id="functions-support">
  ### 함수 지원
</div>

`WHERE` 절의 조건에는 컬럼에 대해 동작하는 함수 호출이 포함될 수 있습니다. 컬럼이 인덱스의 일부이면, ClickHouse는 함수를 수행할 때 해당 인덱스를 사용하려고 합니다. ClickHouse는 인덱스 사용 시 함수별로 서로 다른 부분 집합을 지원합니다.

`set` 유형의 인덱스는 모든 함수에서 사용할 수 있습니다. 다른 인덱스 유형은 다음과 같이 지원됩니다:

| 함수(연산자) / 인덱스                                                                                                             | 기본 키 | minmax | ngrambf&#95;v1 | tokenbf&#95;v1 | bloom&#95;filter | sparse&#95;grams | text |
| ------------------------------------------------------------------------------------------------------------------------- | ---- | ------ | -------------- | -------------- | ---------------- | ---------------- | ---- |
| [equals (=, ==)](/ko/sql-reference/functions/comparison-functions.md/#equals)                                                | ✔    | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notEquals(!=, &lt;&gt;)](/ko/sql-reference/functions/comparison-functions.md/#notEquals)                                    | ✔    | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [like](/ko/sql-reference/functions/string-search-functions.md/#like)                                                         | ✔    | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [notLike](/ko/sql-reference/functions/string-search-functions.md/#notLike)                                                   | ✔    | ✔      | ✔              | ✔              | ✗                | ✔                | ✗    |
| [match](/ko/sql-reference/functions/string-search-functions.md/#match)                                                       | ✗    | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [startsWith](/ko/sql-reference/functions/string-functions.md/#startsWith)                                                    | ✔    | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [endsWith](/ko/sql-reference/functions/string-functions.md/#endsWith)                                                        | ✗    | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [multiSearchAny](/ko/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                     | ✗    | ✗      | ✔              | ✗              | ✗                | ✗                | ✔    |
| [multiSearchAnyUTF8](/ko/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)                             | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [multiMatchAny](/ko/sql-reference/functions/string-search-functions.md/#multiMatchAny)                                       | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [in](/ko/sql-reference/functions/in-functions)                                                                               | ✔    | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notIn](/ko/sql-reference/functions/in-functions)                                                                            | ✔    | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [less (`<`)](/ko/sql-reference/functions/comparison-functions.md/#less)                                                      | ✔    | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greater (`>`)](/ko/sql-reference/functions/comparison-functions.md/#greater)                                                | ✔    | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [lessOrEquals (`<=`)](/ko/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                     | ✔    | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greaterOrEquals (`>=`)](/ko/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                               | ✔    | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [empty](/ko/sql-reference/functions/array-functions/#empty)                                                                  | ✔    | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [notEmpty](/ko/sql-reference/functions/array-functions/#notEmpty)                                                            | ✗    | ✔      | ✗              | ✗              | ✗                | ✔                | ✗    |
| [has](/ko/sql-reference/functions/array-functions#has)                                                                       | ✔    | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [hasAny](/ko/sql-reference/functions/array-functions#hasAny)                                                                 | ✗    | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasAll](/ko/sql-reference/functions/array-functions#hasAll)                                                                 | ✗    | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasToken](/ko/sql-reference/functions/string-search-functions.md/#hasToken)                                                 | ✗    | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenOrNull](/ko/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                     | ✗    | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenCaseInsensitive (`*`)](/ko/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)             | ✗    | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasTokenCaseInsensitiveOrNull (`*`)](/ko/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull) | ✗    | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasAnyTokens](/ko/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                         | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [hasAllTokens](/ko/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                         | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [pointInPolygon](/ko/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                              | ✔    | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [mapContains (mapContainsKey)](/ko/sql-reference/functions/tuple-map-functions#mapContainsKey)                               | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsKeyLike](/ko/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                     | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValue](/ko/sql-reference/functions/tuple-map-functions#mapContainsValue)                                         | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValueLike](/ko/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                 | ✗    | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |

상수 인수가 ngram 크기보다 작은 함수는 `ngrambf_v1`에서 쿼리 최적화에 사용할 수 없습니다.

(*) `hasTokenCaseInsensitive` 및 `hasTokenCaseInsensitiveOrNull`이 효과를 발휘하려면 `tokenbf_v1` 인덱스를 소문자 변환된 데이터에 생성해야 합니다. 예: `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`.

:::note
블룸 필터는 오탐(false positive)이 발생할 수 있으므로, `ngrambf_v1`, `tokenbf_v1`, `sparse_grams`, `bloom_filter` 인덱스는 함수 결과가 false로 예상되는 쿼리 최적화에는 사용할 수 없습니다.

예를 들면 다음과 같습니다:

* 최적화 가능:
  * `s LIKE '%test%'`
  * `NOT s NOT LIKE '%test%'`
  * `s = 1`
  * `NOT s != 1`
  * `startsWith(s, 'test')`
* 최적화 불가:
  * `NOT s LIKE '%test%'`
  * `s NOT LIKE '%test%'`
  * `NOT s = 1`
  * `s != 1`
  * `NOT startsWith(s, 'test')`
    :::

<div id="projections">
  ## 프로젝션
</div>

프로젝션은 [materialized view](/ko/sql-reference/statements/create/view)와 비슷하지만, 파트 수준에서 정의됩니다. 또한 쿼리에서 자동으로 사용되며 일관성을 보장합니다.

:::note
프로젝션을 구현할 때는 [force&#95;optimize&#95;projection](/ko/operations/settings/settings#force_optimize_projection) 설정도 함께 고려해야 합니다.
:::

프로젝션은 [FINAL](/ko/sql-reference/statements/select/from#final-modifier) 수정자를 사용하는 `SELECT` 문에서는 지원되지 않습니다.

<div id="projection-query">
  ### 프로젝션 쿼리
</div>

프로젝션 쿼리는 프로젝션을 정의하는 쿼리입니다. 상위 테이블(table)에서 데이터를 암묵적으로 선택합니다.
**구문**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

프로젝션은 [ALTER](/ko/sql-reference/statements/alter/projection.md) SQL 문을 사용해 수정하거나 삭제할 수 있습니다.

<div id="projection-index">
  ### Projection 인덱스
</div>

Projection 인덱스는 프로젝션 하위 시스템을 확장하여 프로젝션 수준 인덱스를 가볍고 명시적으로 정의할 수 있게 합니다.
외부적으로 Projection 인덱스는 여전히 프로젝션이지만, 구문이 더 단순하고 의도도 더 분명합니다. 즉, 구체화된 데이터를 제공하는 것이 아니라 필터링에 특화된 표현식을 정의합니다.
내부적으로 Projection 인덱스는 일반 프로젝션처럼 원본 테이블을 순열된 행 순서로 구체화하지 않습니다.
대신 순열은 숫자 순열 컬럼 `_part_offset` 형태로 저장됩니다. 즉, `SELECT _part_offset ORDER BY <index_expr>`입니다.

<div id="projection-index-syntax">
  #### 구문
</div>

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

예시:

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="projection-index-types">
  #### 인덱스 유형
</div>

현재 지원되는 항목은 다음과 같습니다.

* **basic**: 표현식에 적용되는 일반적인 MergeTree 인덱스와 동일합니다.

이 프레임워크는 향후 더 많은 인덱스 유형을 추가할 수 있도록 설계되었습니다.

<div id="projection-storage">
  ### 프로젝션 저장소
</div>

프로젝션은 파트 디렉터리 내부에 저장됩니다. 인덱스와 비슷하지만, 익명 `MergeTree` 테이블의 파트를 저장하는 하위 디렉터리를 포함한다는 점이 다릅니다. 이 테이블은 프로젝션의 정의 쿼리에 따라 생성됩니다. `GROUP BY` 절이 있으면 기본 저장 엔진은 [AggregatingMergeTree](aggregatingmergetree.md)가 되며, 모든 집계 함수는 `AggregateFunction`으로 변환됩니다. `ORDER BY` 절이 있으면 `MergeTree` 테이블은 이를 기본 키 표현식으로 사용합니다. 머지 프로세스 중에는 프로젝션 파트가 해당 저장소의 머지 루틴을 통해 머지됩니다. 부모 테이블 파트의 체크섬은 프로젝션 파트의 체크섬과 결합됩니다. 그 밖의 유지 관리 작업은 스킵 인덱스와 유사합니다.

<div id="projection-query-analysis">
  ### 쿼리 분석
</div>

1. 프로젝션을 사용해 주어진 쿼리에 응답할 수 있는지, 즉 base table을 쿼리했을 때와 동일한 결과를 생성하는지 확인합니다.
2. 읽어야 하는 그래뉼 수가 가장 적은 최적의 실행 가능 대상을 선택합니다.
3. 프로젝션을 사용하는 쿼리 파이프라인은 원본 파트를 사용하는 경우와 다릅니다. 일부 파트에 프로젝션이 없으면, 이를 즉석에서 &quot;project&quot;할 수 있도록 파이프라인을 추가할 수 있습니다.

<div id="concurrent-data-access">
  ## 동시 데이터 액세스
</div>

테이블에 동시에 접근할 때는 다중 버전 방식을 사용합니다. 다시 말해, 테이블이 동시에 읽히고 갱신되더라도 데이터는 쿼리 시점에 유효한 파트 집합에서 읽습니다. 장시간 유지되는 잠금은 없습니다. 삽입은 읽기 작업을 방해하지 않습니다.

테이블 읽기는 자동으로 병렬화됩니다.

<div id="table_engine-mergetree-ttl">
  ## 컬럼 및 테이블의 TTL
</div>

값의 수명을 결정합니다.

`TTL` 절은 전체 테이블과 각 개별 컬럼에 설정할 수 있습니다. 테이블 수준의 `TTL`에서는 디스크와 볼륨 간 데이터 자동 이동 로직이나, 모든 데이터가 만료된 파트의 재압축도 지정할 수 있습니다.

표현식은 [Date](/ko/sql-reference/data-types/date.md), [Date32](/ko/sql-reference/data-types/date32.md), [DateTime](/ko/sql-reference/data-types/datetime.md) 또는 [DateTime64](/ko/sql-reference/data-types/datetime64.md) 데이터 타입으로 평가되어야 합니다.

:::tip[TTL 표현식에서는 비결정적 함수를 피하세요]
TTL은 삽입 시점이 아니라 백그라운드 머지 중에 평가됩니다.
`rand()`, `now()`, `now64()` 같은 함수는 머지할 때마다 다시 평가되므로 삭제 동작을 예측할 수 없게 됩니다.
ClickHouse는 컬럼 의존성이 전혀 없는 표현식은 차단하지만, 현재는 컬럼 참조와 함께 사용된 비결정적 함수(예: `ts + rand()`)는 거부하지 않습니다. 예측 가능한 결과를 얻으려면 TTL 표현식은 결정적이고 컬럼에서 파생된 값만을 기반으로 해야 합니다.
:::

**구문**

컬럼에 time-to-live를 설정하는 방법:

```sql
TTL time_column
TTL time_column + interval
```

`interval`을 정의하려면 [시간 인터벌](/ko/sql-reference/operators#operators-for-working-with-dates-and-times) 연산자를 사용하십시오. 예를 들면 다음과 같습니다:

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

<div id="mergetree-column-ttl">
  ### 컬럼 TTL
</div>

컬럼의 값이 만료되면 ClickHouse는 해당 값을 그 컬럼의 데이터 타입(data type)에 대한 기본값으로 대체합니다. 데이터 파트에서 해당 컬럼의 모든 값이 만료되면 ClickHouse는 파일 시스템(filesystem)에 있는 해당 데이터 파트에서 이 컬럼을 삭제합니다.

`TTL` 절은 키 컬럼에는 사용할 수 없습니다.

**예시**

<div id="creating-a-table-with-ttl">
  #### `TTL`이 설정된 테이블 생성:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

<div id="adding-ttl-to-a-column-of-an-existing-table">
  #### 기존 테이블의 컬럼에 TTL 추가하기
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

<div id="altering-ttl-of-the-column">
  #### 컬럼 TTL 변경
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

<div id="mergetree-table-ttl">
  ### 테이블 TTL
</div>

테이블에는 만료된 행을 삭제하기 위한 표현식과 [디스크 또는 볼륨](#table_engine-mergetree-multiple-volumes) 간에 파트를 자동으로 이동하기 위한 여러 표현식을 지정할 수 있습니다. 테이블의 행이 만료되면 ClickHouse는 해당 행을 모두 삭제합니다. 파트 이동 또는 재압축의 경우에는 파트의 모든 행이 `TTL` 표현식 조건을 충족해야 합니다.

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

각 `TTL` 표현식 뒤에는 TTL 규칙 유형을 지정할 수 있습니다. 이 유형은 표현식 조건이 충족되었을 때(현재 시점에 도달했을 때) 수행할 동작에 영향을 줍니다:

* `DELETE` - 만료된 행을 삭제합니다(기본 동작).
* `RECOMPRESS codec_name` - 데이터 파트를 `codec_name`으로 다시 압축합니다.
* `TO DISK 'aaa'` - 파트를 디스크 `aaa`로 이동합니다.
* `TO VOLUME 'bbb'` - 파트를 볼륨 `bbb`로 이동합니다.
* `GROUP BY` - 만료된 행을 집계합니다.

`DELETE` 동작은 필터링 조건에 따라 만료된 행 중 일부만 삭제하도록 `WHERE` 절과 함께 사용할 수 있습니다:

```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

`GROUP BY` 표현식은 테이블 기본 키의 접두사여야 합니다.

컬럼이 `GROUP BY` 표현식에 포함되지 않고 `SET` 절에서도 명시적으로 설정되지 않은 경우, 결과 행에는 그룹화된 행들 중 하나에서 가져온 임의의 값이 들어갑니다(마치 해당 컬럼에 집계 함수 `any`가 적용된 것과 같습니다).

**예시**

<div id="creating-a-table-with-ttl">
  #### `TTL`이 설정된 테이블 생성:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

<div id="altering-ttl-of-the-table">
  #### 테이블 `TTL` 변경:
</div>

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

행이 한 달 후 만료되는 테이블을 생성합니다. 만료된 행 중 날짜가 월요일인 경우 해당 행은 삭제됩니다:

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

<div id="creating-a-table-where-expired-rows-are-recompressed">
  #### 만료된 행을 재압축하는 테이블 만들기:
</div>

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

만료된 행이 집계되도록 테이블을 생성합니다. 결과 행에서 `x`에는 그룹화된 행 전체의 최댓값이, `y`에는 최솟값이, `d`에는 그룹화된 행 중 임의의 값이 포함됩니다.

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

<div id="mergetree-removing-expired-data">
  ### 만료된 데이터 삭제
</div>

`TTL`이 만료된 데이터는 ClickHouse가 데이터 파트를 머지할 때 삭제됩니다.

ClickHouse가 데이터 만료를 감지하면 예정되지 않은 머지를 수행합니다. 이러한 머지의 빈도를 제어하려면 `merge_with_ttl_timeout`을 설정할 수 있습니다. 이 값이 너무 낮으면 예정되지 않은 머지가 자주 수행되어 많은 리소스를 사용할 수 있습니다.

머지 사이에 `SELECT` 쿼리를 수행하면 만료된 데이터가 반환될 수 있습니다. 이를 방지하려면 `SELECT` 전에 [OPTIMIZE](/ko/sql-reference/statements/optimize.md) 쿼리를 사용하십시오.

**관련 항목**

* [ttl&#95;only&#95;drop&#95;parts](/ko/operations/settings/merge-tree-settings#ttl_only_drop_parts) 설정

<div id="disk-types">
  ## 디스크 유형
</div>

로컬 블록 디바이스 외에도 ClickHouse는 다음과 같은 스토리지 유형을 지원합니다:

* [S3 및 MinIO용 `s3`](#table_engine-mergetree-s3)
* [GCS용 `gcs`](/ko/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
* [Azure Blob Storage용 `blob_storage_disk`](/ko/operations/storing-data#azure-blob-storage)
* [HDFS용 `hdfs`](/ko/engines/table-engines/integrations/hdfs)
* [웹에서 읽기 전용으로 사용하는 `web`](/ko/operations/storing-data#web-storage)
* [로컬 캐싱용 `cache`](/ko/operations/storing-data#using-local-cache)
* [S3 백업용 `s3_plain`](/ko/operations/backup/disk)
* [S3의 불변 비복제 테이블용 `s3_plain_rewritable`](/ko/operations/storing-data.md#s3-plain-rewritable-storage)

<div id="table_engine-mergetree-multiple-volumes">
  ## 데이터 저장에 여러 블록 디바이스 사용
</div>

<div id="introduction">
  ### 소개
</div>

`MergeTree` 계열 테이블 엔진은 여러 블록 디바이스에 데이터를 저장할 수 있습니다. 예를 들어, 특정 테이블의 데이터가 자연스럽게 &quot;hot&quot; 데이터와 &quot;cold&quot; 데이터로 나뉘는 경우 유용합니다. 최신 데이터는 자주 조회되지만 필요한 저장 공간은 많지 않습니다. 반면, 긴 꼬리 분포를 보이는 과거 데이터는 드물게 조회됩니다. 여러 디스크를 사용할 수 있다면 &quot;hot&quot; 데이터는 빠른 디스크(예: NVMe SSD 또는 메모리)에 두고, &quot;cold&quot; 데이터는 상대적으로 느린 디스크(예: HDD)에 둘 수 있습니다.

이는 S3 및 기타 객체 스토리지 디스크를 포함한 모든 디스크 유형에 적용됩니다. 예를 들어, 하나의 볼륨 안에서 여러 S3 버킷에 데이터를 분산할 수 있고, 로컬 디스크의 데이터를 S3로 이동하는 계층형 정책을 만들 수도 있습니다. 자세한 내용은 [여러 볼륨에서 S3 디스크 사용하기](#s3-multiple-volumes)를 참조하십시오.

데이터 파트는 `MergeTree` 엔진 테이블에서 이동 가능한 최소 단위입니다. 하나의 파트에 속한 데이터는 하나의 디스크에 저장됩니다. 데이터 파트는 백그라운드에서(사용자 설정에 따라) 디스크 간에 이동할 수 있으며, [ALTER](/ko/sql-reference/statements/alter/partition) 쿼리를 사용해 이동할 수도 있습니다.

<div id="terms">
  ### 용어
</div>

* 디스크 — 파일 시스템에 마운트된 블록 디바이스입니다.
* 기본 디스크 — [path](/ko/operations/server-configuration-parameters/settings.md/#path) 서버 설정에 지정된 경로가 위치한 디스크입니다.
* 볼륨 — 동일한 디스크를 순서대로 묶은 집합입니다([JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)와 유사).
* 스토리지 정책 — 볼륨 집합과 그 사이에서 데이터를 이동하는 규칙입니다.

위에서 설명한 엔터티의 이름은 시스템 테이블 [system.storage&#95;policies](/ko/operations/system-tables/storage_policies) 및 [system.disks](/ko/operations/system-tables/disks)에서 확인할 수 있습니다. 테이블에 구성된 스토리지 정책 중 하나를 적용하려면 `MergeTree` 엔진 계열 테이블의 `storage_policy` 설정을 사용하십시오.

<div id="table_engine-mergetree-multiple-volumes_configure">
  ### 구성
</div>

디스크, 볼륨 및 스토리지 정책은 `config.d` 디렉터리의 파일에서 `<storage_configuration>` 태그 안에 선언해야 합니다.

:::tip
디스크는 쿼리의 `SETTINGS` 섹션에서도 선언할 수 있습니다. 이는 예를 들어 URL에 호스팅된 디스크를 일시적으로 연결해야 하는 애드혹 분석에 유용합니다.
자세한 내용은 [동적 스토리지](/ko/operations/storing-data#dynamic-configuration)를 참조하십시오.
:::

구성 구조:

```xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

태그:

* `<disk_name_N>` — 디스크 이름입니다. 모든 디스크의 이름은 서로 달라야 합니다.
* `path` — 서버가 데이터(`data` 및 `shadow` 폴더)를 저장하는 경로이며, 끝은 &#39;/&#39;로 끝나야 합니다.
* `keep_free_space_bytes` — 예약해 둘 디스크의 여유 공간입니다.

디스크 정의의 순서는 중요하지 않습니다.

스토리지 정책 구성 마크업:

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

태그:

* `policy_name_N` — 정책 이름입니다. 정책 이름은 고유해야 합니다.
* `volume_name_N` — 볼륨 이름입니다. 볼륨 이름은 고유해야 합니다.
* `disk` — 볼륨 내의 디스크입니다.
* `max_data_part_size_bytes` — 볼륨의 어떤 디스크에든 저장할 수 있는 파트의 최대 크기입니다. 병합된 파트의 예상 크기가 `max_data_part_size_bytes`보다 크면 이 파트는 다음 볼륨에 기록됩니다. 이 기능을 사용하면 기본적으로 새롭거나 작은 파트는 핫(SSD) 볼륨에 유지하고, 크기가 커지면 콜드(HDD) 볼륨으로 이동할 수 있습니다. 정책에 볼륨이 하나만 있는 경우에는 이 설정을 사용하지 마십시오.
* `move_factor` — 사용 가능한 공간이 이 값보다 적어지면, 다음 볼륨이 있을 경우 데이터가 자동으로 그 볼륨으로 이동하기 시작합니다(기본값: 0.1). ClickHouse는 기존 파트를 크기 기준으로 큰 것부터 작은 것까지(내림차순) 정렬한 뒤, `move_factor` 조건을 충족하기에 충분한 총 크기를 갖는 파트를 선택합니다. 모든 파트의 총크기가 충분하지 않으면 모든 파트가 이동됩니다.
* `perform_ttl_move_on_insert` — 데이터 파트 INSERT 시 TTL 이동을 비활성화합니다. 기본적으로(활성화된 경우) TTL 이동 규칙에 따라 이미 만료된 데이터 파트를 삽입하면 즉시 이동 규칙에 지정된 볼륨/디스크로 이동합니다. 대상 볼륨/디스크가 느린 경우(예: S3) 삽입 성능이 크게 저하될 수 있습니다. 비활성화하면 이미 만료된 데이터 파트는 기본 볼륨에 기록된 뒤 곧바로 TTL 볼륨으로 이동합니다.
* `load_balancing` - 디스크 균형 조정 정책입니다. `round_robin` 또는 `least_used`를 사용할 수 있습니다.
* `least_used_ttl_ms` - 모든 디스크의 사용 가능 공간을 갱신하는 시간 제한(밀리초 단위)을 구성합니다(`0` - 항상 갱신, `-1` - 갱신 안 함, 기본값은 `60000`). 참고로, 디스크를 ClickHouse만 사용하고 온라인 파일 시스템 크기 확장/축소의 대상이 아닌 경우에는 `-1`을 사용할 수 있습니다. 그 밖의 경우에는 결국 잘못된 공간 분배로 이어질 수 있으므로 권장하지 않습니다.
* `prefer_not_to_merge` — 이 설정은 사용하지 마십시오. 이 볼륨에서 데이터 파트의 머지를 비활성화합니다(이는 해로우며 성능 저하를 초래합니다). 이 설정을 활성화하면(그렇게 하지 마십시오) 이 볼륨에서는 데이터 머지가 허용되지 않습니다(바람직하지 않습니다). 이 설정은 ClickHouse가 느린 디스크를 다루는 방식을 제어할 수 있게 하지만(필요하지 않습니다), ClickHouse가 더 잘 처리하므로 이 설정은 사용하지 마십시오.
* `volume_priority` — 볼륨이 채워지는 우선순위(순서)를 정의합니다. 값이 낮을수록 우선순위가 높습니다. 매개변수 값은 자연수여야 하며, 숫자를 건너뛰지 않고 1부터 N까지의 범위를 모두 포함해야 합니다(가장 낮은 우선순위까지 포함).
  * *모든* 볼륨에 태그가 지정된 경우, 지정된 순서대로 우선순위가 부여됩니다.
  * *일부* 볼륨에만 태그가 지정된 경우, 태그가 없는 볼륨이 가장 낮은 우선순위를 가지며, 구성에서 정의된 순서대로 우선순위가 부여됩니다.
  * *어떤* 볼륨에도 태그가 지정되지 않은 경우, 우선순위는 구성에서 선언된 순서에 따라 설정됩니다.
  * 두 볼륨은 같은 우선순위 값을 가질 수 없습니다.

구성 예시:

```xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

주어진 예시에서 `hdd_in_order` 정책은 [라운드 로빈](https://en.wikipedia.org/wiki/Round-robin_scheduling) 방식을 구현합니다. 따라서 이 정책은 하나의 볼륨(`single`)만 정의하며, 데이터 파트는 해당 볼륨의 모든 디스크에 순환하는 순서로 저장됩니다. 이러한 정책은 시스템에 성능이 비슷한 디스크 여러 개가 마운트되어 있지만 RAID가 구성되지 않은 경우 특히 유용합니다. 각 개별 디스크는 신뢰성이 높지 않을 수 있으므로, 이를 보완하기 위해 복제 계수를 3 이상으로 설정하는 것이 좋습니다.

시스템에서 서로 다른 종류의 디스크를 사용할 수 있다면 `moving_from_ssd_to_hdd` 정책을 대신 사용할 수 있습니다. `hot` 볼륨은 SSD 디스크(`fast_ssd`)로 구성되며, 이 볼륨에 저장할 수 있는 파트의 최대 크기는 1GB입니다. 크기가 1GB를 초과하는 모든 파트는 HDD 디스크 `disk1`이 포함된 `cold` 볼륨에 직접 저장됩니다.
또한 `fast_ssd` 디스크 사용량이 80%를 초과하면 백그라운드 프로세스에 의해 데이터가 `disk1`으로 이동됩니다.

스토리지 정책 내에서 볼륨이 나열되는 순서는, 나열된 볼륨 중 하나 이상에 명시적인 `volume_priority` 매개변수가 없는 경우 중요합니다.
볼륨이 가득 차면 데이터는 다음 볼륨으로 이동됩니다. 또한 데이터는 디스크에 번갈아 저장되므로 디스크가 나열되는 순서 역시 중요합니다.

테이블을 생성할 때는 구성된 스토리지 정책 중 하나를 적용할 수 있습니다:

```sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

`default` 스토리지 정책은 `<path>`에 지정된 단일 디스크로만 구성된 단일 볼륨만 사용함을 의미합니다.
테이블 생성 후에는 [ALTER TABLE ... MODIFY SETTING] 쿼리를 사용해 스토리지 정책을 변경할 수 있으며, 새 정책에는 기존의 모든 디스크와 볼륨이 동일한 이름으로 포함되어야 합니다.

데이터 파트의 백그라운드 이동 작업을 수행하는 스레드 수는 [background&#95;move&#95;pool&#95;size](/ko/operations/server-configuration-parameters/settings.md/#background_move_pool_size) 설정으로 변경할 수 있습니다.

<div id="details">
  ### 세부 정보
</div>

`MergeTree` 테이블에서는 데이터가 여러 방식으로 디스크에 저장됩니다.

* 삽입(`INSERT` 쿼리)의 결과로 저장됩니다.
* 백그라운드 머지와 [뮤테이션](/ko/sql-reference/statements/alter#mutations) 중에 저장됩니다.
* 다른 레플리카에서 다운로드할 때 저장됩니다.
* 파티션 프리징 [ALTER TABLE ... FREEZE PARTITION](/ko/sql-reference/statements/alter/partition#freeze-partition)의 결과로 저장됩니다.

뮤테이션과 파티션 프리징을 제외한 모든 경우, 지정된 스토리지 정책에 따라 파트가 볼륨과 디스크에 저장됩니다.

1. 파트를 저장하기에 충분한 디스크 공간이 있고(`unreserved_space > current_part_size`), 지정된 크기의 파트 저장을 허용하는(`max_data_part_size_bytes > current_part_size`) 첫 번째 볼륨(정의된 순서 기준)을 선택합니다.
2. 이 볼륨 내에서는 이전 데이터 청크를 저장하는 데 사용한 디스크의 다음 디스크이면서, 파트 크기보다 많은 여유 공간이 있는(`unreserved_space - keep_free_space_bytes > current_part_size`) 디스크를 선택합니다.

내부적으로 뮤테이션과 파티션 프리징은 [하드 링크](https://en.wikipedia.org/wiki/Hard_link)를 사용합니다. 서로 다른 디스크 간 하드 링크는 지원되지 않으므로, 이런 경우 결과 파트는 원래 파트와 동일한 디스크에 저장됩니다.

백그라운드에서는 설정 파일에 선언된 볼륨 순서에 따라 여유 공간의 양(`move_factor` 매개변수)을 기준으로 파트가 볼륨 간에 이동됩니다.
데이터는 마지막 볼륨에서 다른 볼륨으로 이동되지 않으며, 첫 번째 볼륨으로도 이동되지 않습니다. 백그라운드 이동 작업은 시스템 테이블 [system.part&#95;log](/ko/operations/system-tables/part_log) (필드 `type = MOVE_PART`)와 [system.parts](/ko/operations/system-tables/parts.md) (필드 `path` 및 `disk`)를 사용해 모니터링할 수 있습니다. 또한 자세한 정보는 서버 로그에서 확인할 수 있습니다.

사용자는 [ALTER TABLE ... MOVE PART|PARTITION ... TO VOLUME|DISK ...](/ko/sql-reference/statements/alter/partition) 쿼리를 사용해 파트 또는 파티션을 한 볼륨에서 다른 볼륨으로 강제로 이동할 수 있으며, 이때 백그라운드 작업에 대한 모든 제약 사항이 적용됩니다. 이 쿼리는 자체적으로 이동 작업을 시작하며 백그라운드 작업이 완료될 때까지 기다리지 않습니다. 사용 가능한 여유 공간이 충분하지 않거나 필요한 조건을 충족하지 않으면 오류 메시지가 반환됩니다.

데이터 이동은 데이터 복제에 영향을 주지 않습니다. 따라서 동일한 테이블에 대해 각 레플리카마다 서로 다른 스토리지 정책을 지정할 수 있습니다.

백그라운드 머지와 뮤테이션이 완료된 후에도 오래된 파트는 일정 시간(`old_parts_lifetime`)이 지난 뒤에만 제거됩니다.
이 시간 동안에는 다른 볼륨이나 디스크로 이동되지 않습니다. 따라서 파트가 최종적으로 제거될 때까지는 점유된 디스크 공간을 계산할 때 계속 반영됩니다.

사용자는 [min&#95;bytes&#95;to&#95;rebalance&#95;partition&#95;over&#95;jbod](/ko/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod) 설정을 사용해 [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) 볼륨의 여러 디스크에 새로운 큰 파트를 균형 있게 할당할 수 있습니다.

<div id="table_engine-mergetree-s3">
  ## 데이터 저장에 외부 스토리지 사용하기
</div>

[MergeTree](/ko/engines/table-engines/mergetree-family/mergetree.md) 계열 테이블 엔진은 각각 `s3`, `azure_blob_storage`, `hdfs` 유형의 디스크를 사용해 데이터를 `S3`, `AzureBlobStorage`, `HDFS`에 저장할 수 있습니다. 자세한 내용은 [외부 스토리지 옵션 구성](/ko/operations/storing-data.md/#configuring-external-storage)을 참조하십시오.

유형이 `s3`인 디스크를 사용해 [S3](https://aws.amazon.com/s3/)를 외부 스토리지로 사용하는 예시입니다.

구성 마크업:

```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

「[외부 스토리지 옵션 구성하기](/ko/operations/storing-data.md/#configuring-external-storage)」도 참조하십시오.

<div id="s3-multiple-volumes">
  ### 여러 볼륨에서 S3 디스크 사용하기
</div>

S3(및 기타 객체 스토리지) 디스크는 로컬 디스크와 동일하게 다중 디스크 및 다중 볼륨 스토리지 정책에 사용할 수 있습니다. 이를 통해 단일 볼륨 내 여러 S3 버킷에 데이터를 분산(JBOD 스타일)하거나, S3 볼륨을 사용한 계층형 스토리지 정책을 구성할 수 있습니다.

예를 들어, 두 개의 S3 버킷에 데이터를 라운드 로빈 방식으로 분산하려면:

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

로컬 볼륨과 S3 볼륨을 계층형 정책으로 조합해 사용할 수도 있습니다. 예를 들어, 데이터가 오래될수록 로컬 SSD의 데이터를 S3로 이동하도록 할 수 있습니다:

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
S3 인증에 `use_environment_credentials`를 사용하는 경우, 환경 자격 증명(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)은 모든 S3 디스크에서 공유됩니다. 디스크마다 서로 다른 환경 자격 증명을 사용할 수는 없습니다. 각 S3 디스크에 서로 다른 자격 증명이 필요하다면, 대신 디스크별로 `access_key_id` 및 `secret_access_key`를 명시적으로 설정하십시오.
:::

공유 스토리지에서 단일 writer와 다수의 reader 시나리오로 복제되지 않은 MergeTree 테이블을 구성할 수 있습니다. 이는 reader에 설정할 수 있는 파트 목록 자동 갱신 기능으로 지원됩니다. 단, 이를 위해서는 레플리카 간에 파일 시스템 메타데이터를 공유해야 합니다(또는 테이블 로컬 디스크와 함께 `table_disk = true`를 사용해야 합니다). [refresh&#95;parts&#95;interval and table&#95;disk](/ko/operations/storing-data.md/#refresh-parts-interval-and-table-disk)를 참조하십시오.

:::note cache 구성
ClickHouse 22.3~22.7 버전은 다른 cache 구성을 사용합니다. 해당 버전 중 하나를 사용 중이라면 [using local cache](/ko/operations/storing-data.md/#using-local-cache)를 참조하십시오.
:::

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_part` — 파트 이름입니다.
* `_part_index` — 쿼리 결과에서 파트의 순차 인덱스입니다.
* `_part_starting_offset` — 쿼리 결과에서 파트가 시작하는 누적 행 위치입니다.
* `_part_offset` — 파트 내 행 번호입니다.
* `_part_granule_offset` — 파트 내 granule 번호입니다.
* `_partition_id` — 파티션 이름입니다.
* `_part_uuid` — 고유한 파트 식별자입니다(MergeTree 설정 `assign_part_uuids`가 활성화된 경우).
* `_part_data_version` — 파트의 데이터 버전입니다(최소 block 번호 또는 mutation 버전).
* `_partition_value` — `partition by` 표현식의 값(튜플)입니다.
* `_sample_factor` — 샘플 계수입니다(쿼리에서 가져옴).
* `_block_number` — 삽입 시 해당 행에 할당된 원래 block 번호이며, 설정 `enable_block_number_column`이 활성화되면 머지 시에도 유지됩니다.
* `_block_offset` — 삽입 시 해당 행에 할당된 block 내 원래 행 번호이며, 설정 `enable_block_offset_column`이 활성화되면 머지 시에도 유지됩니다.
* `_disk_name` — 스토리지에 사용되는 디스크 이름입니다.

<div id="column-statistics">
  ## 컬럼 통계
</div>

<CloudNotSupportedBadge />

통계 선언은 `*MergeTree*` 계열 테이블의 `CREATE` 쿼리에서 컬럼 섹션에 있습니다:

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

통계는 `ALTER` SQL 문으로도 변경할 수 있습니다:

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

이 경량 통계는 컬럼 값의 분포 정보를 집계합니다. 통계는 각 파트에 저장되며, 데이터가 삽입될 때마다 갱신됩니다.
`set use_statistics = 1`을 설정한 경우에만 PREWHERE 최적화에 사용할 수 있습니다.

<div id="part-pruning-with-statistics">
  #### 통계를 활용한 파트 프루닝
</div>

`use_statistics_for_part_pruning`이 활성화되면 통계를 파트 프루닝에 사용할 수 있습니다.
현재는 `MinMax` 및 `Basic` 통계만 파트 프루닝을 지원합니다. 이러한 통계가 컬럼에 정의되면 ClickHouse는 각 파트에 대해 해당 컬럼의 최솟값과 최댓값을 기록합니다.
파트 프루닝을 사용하면 쿼리 필터 조건과 일치하는 행이 해당 파트에 전혀 없을 때 전체 데이터 파트 읽기를 건너뛸 수 있습니다.

**예시:**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

<div id="available-types-of-column-statistics">
  ### 사용 가능한 컬럼 통계 유형
</div>

* `Basic`

  컬럼에서 파생된 단일 값 요약을 간결하게 묶은 통계입니다. 컬럼 타입에 따라 다음 정보가 채워집니다.

  * 값이 숫자(정수, 부동소수점, `Decimal*`, `Date*`, `DateTime*`, `Enum*`, `IPv4`, ...)로 표현되는 모든 컬럼: 최솟값과 최댓값입니다. 이를 통해 범위 필터의 선택도를 추정하고 파트 프루닝을 수행할 수 있습니다.
  * `String` 및 `FixedString` 컬럼: `NULL`이 아닌 값의 전체 바이트 길이입니다(이로부터 평균 문자열 길이를 계산할 수 있습니다).
  * `Nullable` 및 `LowCardinality(Nullable)` 컬럼: `NULL` 값의 개수이며, 최적화기는 이를 사용해 선택도 추정에서 `NULL` 행을 제외합니다.

    하나의 `Basic` 통계로 이들 중 여러 정보를 동시에 채울 수 있습니다. 예를 들어 `Nullable(UInt32)` 컬럼에서는 숫자 최솟값/최댓값과 `NULL` 개수를 모두 추적합니다. `MinMax`와 비교하면 `Basic`은 `String` / `FixedString` 컬럼에서도 동작하며, `UUID` 또는 `IPv6` 같은 타입의 `Nullable` 래퍼에 선언해 `NULL` 개수만 추적하는 용도로도 사용할 수 있습니다.

    구문: `basic`

* `MinMax`

  숫자 컬럼에서 범위 필터의 선택도를 추정할 수 있게 해 주는 컬럼의 최솟값과 최댓값입니다.

  구문: `minmax`

* `TDigest`

:::warning
`tdigest` 타입의 통계는 생성 비용이 높아 데이터 수집이 느려질 수 있습니다.
:::

숫자 컬럼의 근사 백분위수(예: 90번째 백분위수)를 계산할 수 있는 [TDigest](https://github.com/tdunning/t-digest) 스케치입니다.

구문: `tdigest`

* `Uniq`

  컬럼에 포함된 고유 값의 개수를 추정하는 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) 스케치입니다.

  구문: `uniq`

* `CountMin`

:::warning
`countmin` 타입의 통계는 생성 비용이 높아 데이터 수집이 느려질 수 있습니다.
:::

컬럼의 각 값이 나타나는 빈도를 근사적으로 계산하는 [CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) 스케치입니다.

구문 `countmin`

<div id="supported-data-types">
  ### 지원되는 데이터 타입
</div>

|          | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | IPv4 | String 또는 FixedString |
| -------- | -------------------------------------------------- | ---- | --------------------- |
| Basic    | ✔                                                  | ✔    | ✔                     |
| CountMin | ✔                                                  | ✔    | ✔                     |
| MinMax   | ✔                                                  | ✔    | ✗                     |
| TDigest  | ✔                                                  | ✗    | ✗                     |
| Uniq     | ✔                                                  | ✔    | ✔                     |

위에 나열된 타입에 대한 `Nullable` 및 `LowCardinality(Nullable)` 래퍼도 모두 지원합니다. 또한 `Basic`은 null 개수만 추적하는 용도로 `UUID` 또는 `IPv6`와 같은 타입의 `Nullable` 래퍼에 대해서도 추가로 선언할 수 있습니다.

<div id="supported-operations">
  ### 지원되는 작업
</div>

|          | 동등 필터 (==) | 범위 필터 (`>, >=, <, <=`) |
| -------- | ---------- | ---------------------- |
| Basic    | ✗          | ✔ (숫자 컬럼만)             |
| CountMin | ✔          | ✗                      |
| MinMax   | ✗          | ✔ (숫자 컬럼만)             |
| TDigest  | ✗          | ✔ (숫자 컬럼만)             |
| Uniq     | ✔          | ✗                      |

`String` / `FixedString` 컬럼에서 `Basic`은 전체 non-NULL 바이트 길이(평균 문자열 길이 추정에 사용됨)와 NULL 개수만 기록합니다.
범위 필터와 파트 프루닝에는 사용되지 않습니다.

<div id="column-level-settings">
  ## 컬럼 수준 설정
</div>

일부 MergeTree 설정은 컬럼 수준에서 재정의할 수 있습니다.

* `max_compress_block_size` — 테이블에 쓰기 위해 압축하기 전에, 압축되지 않은 데이터 블록의 최대 크기입니다.
* `min_compress_block_size` — 다음 마크를 쓸 때 압축을 적용하기 위해 필요한, 압축되지 않은 데이터 블록의 최소 크기입니다.

예시:

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

컬럼 단위 설정은 [ALTER MODIFY COLUMN](/ko/sql-reference/statements/alter/column.md)을 사용해 수정하거나 제거할 수 있습니다. 예를 들어:

* 컬럼 선언에서 `SETTINGS` 제거:

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

* 설정을 변경합니다:

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

* 하나 이상의 설정을 재설정하고, 테이블의 CREATE 쿼리에 있는 컬럼 표현식에서 설정 선언도 제거합니다.

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```