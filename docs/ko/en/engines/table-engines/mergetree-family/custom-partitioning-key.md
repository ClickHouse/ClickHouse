---
description: 'MergeTree 테이블에 사용자 지정 파티셔닝 키를 추가하는 방법을 알아봅니다.'
sidebar_label: '사용자 지정 파티셔닝 키'
sidebar_position: 30
slug: /engines/table-engines/mergetree-family/custom-partitioning-key
title: '사용자 지정 파티셔닝 키'
doc_type: 'guide'
---

:::note
대부분의 경우 파티션 키는 필요하지 않으며, 그 외 대부분의 경우에도 월 단위보다 더 세분화된 파티션 키는 필요하지 않습니다. 다만 일 단위 파티셔닝이 일반적인 관측성 사용 사례는 예외입니다.

지나치게 세분화된 파티셔닝은 절대 사용하지 마십시오. 클라이언트 식별자나 이름으로 데이터를 파티셔닝하지 마십시오. 대신 클라이언트 식별자나 이름을 ORDER BY 표현식의 첫 번째 컬럼으로 지정하십시오.
:::

파티셔닝은 [복제된 테이블](../../../engines/table-engines/mergetree-family/replication.md) 및 [구체화된 뷰(Materialized View)](/ko/sql-reference/statements/create/view#materialized-view)를 포함한 [MergeTree 엔진 계열 테이블](../../../engines/table-engines/mergetree-family/mergetree.md)에서 사용할 수 있습니다.

파티션은 지정된 기준에 따라 테이블의 레코드를 논리적으로 묶은 단위입니다. 월별, 일별, 이벤트 유형별 등 임의의 기준으로 파티션을 설정할 수 있습니다. 각 파티션은 이러한 데이터를 더 쉽게 다룰 수 있도록 별도로 저장됩니다. 데이터에 접근할 때 ClickHouse는 가능한 한 가장 작은 범위의 파티션만 사용합니다. 파티셔닝 키가 포함된 쿼리에서는 파티션이 성능 향상에 도움이 됩니다. ClickHouse가 해당 파티션 내부의 파트와 그래뉼을 선택하기 전에 먼저 해당 파티션으로 필터링하기 때문입니다.

파티션은 [테이블을 생성할 때](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) `PARTITION BY expr` 절에서 지정합니다. 파티션 키는 테이블 컬럼을 기반으로 한 어떤 표현식이든 될 수 있습니다. 예를 들어 월별 파티셔닝을 지정하려면 `toYYYYMM(date_column)` 표현식을 사용합니다:

```sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

파티션 키도 [기본 키](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)와 마찬가지로 표현식의 튜플이 될 수 있습니다. 예시:

```sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

이 예시에서는 이번 주에 발생한 이벤트 유형을 기준으로 파티셔닝을 설정합니다.

기본적으로 부동소수점 파티션 키는 지원되지 않습니다. 사용하려면 [allow&#95;floating&#95;point&#95;partition&#95;key](../../../operations/settings/merge-tree-settings.md#allow_floating_point_partition_key) 설정을 활성화하십시오.

테이블에 새 데이터를 삽입하면 해당 데이터는 기본 키를 기준으로 정렬된 별도의 파트(청크)로 저장됩니다. 삽입 후 10~15분이 지나면 동일한 파티션에 속한 파트들이 하나의 전체 파트로 머지됩니다.

:::info
머지는 파티셔닝 표현식의 값이 같은 데이터 파트에 대해서만 작동합니다. 즉, **파티션을 지나치게 세분화해서는 안 됩니다**(약 1,000개를 초과하는 파티션). 그렇지 않으면 파일 시스템에 파일이 지나치게 많아지고 열린 파일 디스크립터 수도 늘어나 `SELECT` 쿼리 성능이 저하됩니다.
:::

테이블 파트와 파티션을 확인하려면 [system.parts](../../../operations/system-tables/parts.md) 테이블을 사용하십시오. 예를 들어, 월 단위로 파티셔닝된 `visits` 테이블이 있다고 가정하겠습니다. `system.parts` 테이블에 대해 `SELECT` 쿼리를 실행해 보겠습니다:

```sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

```text
┌─partition─┬─name──────────────┬─active─┐
│ 201901    │ 201901_1_3_1      │      0 │
│ 201901    │ 201901_1_9_2_11   │      1 │
│ 201901    │ 201901_8_8_0      │      0 │
│ 201901    │ 201901_9_9_0      │      0 │
│ 201902    │ 201902_4_6_1_11   │      1 │
│ 201902    │ 201902_10_10_0_11 │      1 │
│ 201902    │ 201902_11_11_0_11 │      1 │
└───────────┴───────────────────┴────────┘
```

`partition` 컬럼에는 파티션 이름이 들어 있습니다. 이 예시에는 `201901`과 `201902`라는 2개의 파티션이 있습니다. 이 컬럼 값을 사용하면 [ALTER ... PARTITION](../../../sql-reference/statements/alter/partition.md) 쿼리에서 파티션 이름을 지정할 수 있습니다.

`name` 컬럼에는 파티션 데이터 파트의 이름이 들어 있습니다. 이 컬럼을 사용하면 [ALTER ATTACH PART](/ko/sql-reference/statements/alter/partition#attach-partitionpart) 쿼리에서 파트 이름을 지정할 수 있습니다.

파트 이름 `201901_1_9_2_11`을 항목별로 살펴보겠습니다.

* `201901`은 파티션 이름입니다.
* `1`은 데이터 블록의 최소 번호입니다.
* `9`는 데이터 블록의 최대 번호입니다.
* `2`는 청크 수준입니다(이 파트가 형성된 머지 트리의 깊이).
* `11`은 mutation 버전입니다(파트에 mutation이 적용된 경우).

:::info
구형 테이블의 파트 이름은 `20190117_20190123_2_2_0` 형식입니다(최소 날짜 - 최대 날짜 - 최소 블록 번호 - 최대 블록 번호 - 수준).
:::

`active` 컬럼은 파트의 상태를 보여줍니다. `1`은 활성, `0`은 비활성을 의미합니다. 비활성 파트는 예를 들어 더 큰 파트로 머지된 후 남아 있는 원본 파트입니다. 손상된 데이터 파트도 비활성으로 표시됩니다.

예시에서 볼 수 있듯이 동일한 파티션에 여러 개의 분리된 파트가 있습니다(예: `201901_1_3_1` 및 `201901_1_9_2`). 이는 이러한 파트가 아직 머지되지 않았다는 뜻입니다. ClickHouse는 삽입된 데이터 파트를 주기적으로 머지하며, 일반적으로 삽입 후 약 15분 뒤에 수행됩니다. 또한 [OPTIMIZE](../../../sql-reference/statements/optimize.md) 쿼리를 사용해 예약되지 않은 머지를 수행할 수도 있습니다. 예시:

```sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```text
┌─partition─┬─name─────────────┬─active─┐
│ 201901    │ 201901_1_3_1     │      0 │
│ 201901    │ 201901_1_9_2_11  │      1 │
│ 201901    │ 201901_8_8_0     │      0 │
│ 201901    │ 201901_9_9_0     │      0 │
│ 201902    │ 201902_4_6_1     │      0 │
│ 201902    │ 201902_4_11_2_11 │      1 │
│ 201902    │ 201902_10_10_0   │      0 │
│ 201902    │ 201902_11_11_0   │      0 │
└───────────┴──────────────────┴────────┘
```

비활성 파트는 병합된 후 약 10분이 지나면 삭제됩니다.

파트와 파티션을 확인하는 또 다른 방법은 테이블 디렉터리인 `/var/lib/clickhouse/data/<database>/<table>/`로 이동하는 것입니다. 예시는 다음과 같습니다:

```bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

&#39;201901&#95;1&#95;1&#95;0&#39;, &#39;201901&#95;1&#95;7&#95;1&#39; 등의 폴더는 파트의 디렉터리입니다. 각 파트는 해당 파티션에 대응하며, 특정 월의 데이터만 포함합니다(이 예시의 테이블은 월 단위 파티셔닝을 사용합니다).

`detached` 디렉터리에는 [DETACH](/ko/sql-reference/statements/detach) 쿼리를 사용해 테이블에서 분리된 파트가 들어 있습니다. 손상된 파트도 삭제되지 않고 이 디렉터리로 이동합니다. 서버는 `detached` 디렉터리에 있는 파트를 사용하지 않습니다. 이 디렉터리의 데이터는 언제든지 추가, 삭제 또는 수정할 수 있지만, [ATTACH](/ko/sql-reference/statements/alter/partition#attach-partitionpart) 쿼리를 실행하기 전까지는 서버가 이를 인지하지 못합니다.

서버가 실행 중일 때는 파일 시스템에서 파트 집합이나 그 데이터를 수동으로 변경할 수 없다는 점에 유의하십시오. 서버가 이를 인지하지 못하기 때문입니다. 복제되지 않은 테이블은 서버가 중지된 상태일 때는 이렇게 할 수 있지만, 권장되지 않습니다. 복제된 테이블(Replicated Table)은 어떤 상황에서도 파트 집합을 변경할 수 없습니다.

ClickHouse에서는 파티션에 대해 삭제, 한 테이블에서 다른 테이블로 복사, Backup 생성과 같은 작업을 수행할 수 있습니다. 모든 작업 목록은 [Manipulations With Partitions and Parts](/ko/sql-reference/statements/alter/partition) 섹션을 참조하십시오.

<div id="group-by-optimisation-using-partition-key">
  ## 파티션 키를 사용한 Group By 최적화
</div>

테이블의 파티션 키와 쿼리의 Group By 키 조합에 따라서는 각 파티션별로 집계를 독립적으로 실행할 수 있습니다.
그러면 마지막에 모든 실행 스레드의 부분 집계 데이터를 머지할 필요가 없습니다.
이는 각 Group By 키 값이 서로 다른 두 스레드의 작업 Set에 동시에 포함될 수 없도록 보장되기 때문입니다.

대표적인 예시는 다음과 같습니다:

```sql
CREATE TABLE session_log
(
    UserID UInt64,
    SessionID UUID
)
ENGINE = MergeTree
PARTITION BY sipHash64(UserID) % 16
ORDER BY tuple();

SELECT
    UserID,
    COUNT()
FROM session_log
GROUP BY UserID;
```

:::note
이러한 쿼리의 성능은 테이블 레이아웃에 크게 좌우됩니다. 따라서 이 최적화는 기본적으로 활성화되지 않습니다.
:::

좋은 성능을 얻으려면 다음 요소가 중요합니다.

* 쿼리에 포함되는 파티션 수는 충분히 많아야 합니다(`max_threads / 2`보다 커야 함). 그렇지 않으면 쿼리가 시스템 자원을 충분히 활용하지 못합니다.
* 파티션이 너무 작아서는 안 됩니다. 그렇지 않으면 배치 처리(batch processing)가 행 단위 처리로 퇴화할 수 있습니다.
* 파티션 크기는 서로 비슷해야 하며, 그래야 모든 스레드가 대략 비슷한 양의 작업을 수행합니다.

:::info
데이터가 파티션 간에 고르게 분산되도록 `partition by` 절의 컬럼에 hash function을 적용하는 것이 좋습니다.
:::

관련 설정은 다음과 같습니다.

* `allow_aggregate_partitions_independently` - 이 최적화 사용 여부를 제어합니다.
* `force_aggregate_partitions_independently` - 정확성 측면에서는 적용 가능하지만, 효율성을 추정하는 내부 로직에 의해 비활성화되는 경우에도 사용을 강제합니다.
* `max_number_of_partitions_for_independent_aggregation` - 테이블이 가질 수 있는 최대 파티션 수에 대한 하드 제한입니다.