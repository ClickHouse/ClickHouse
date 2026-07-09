---
description: '경량 업데이트는 패치 파트를 사용해 데이터베이스의 데이터를 업데이트하는 과정을 간소화합니다.'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: '경량 UPDATE 문'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
경량 업데이트는 현재 베타입니다.
문제가 발생하면 [ClickHouse 리포지토리](https://github.com/clickhouse/clickhouse/issues)에 이슈를 등록해 주십시오.
:::

경량 `UPDATE` 문은 표현식 `filter_expr`와 일치하는 `[db.]table` 테이블의 행을 업데이트합니다.
이는 데이터 파트의 전체 컬럼을 다시 쓰는 무거운 방식의 [`ALTER TABLE ... UPDATE`](/ko/sql-reference/statements/alter/update) 쿼리와 구분하기 위해 &quot;경량 업데이트&quot;라고 부릅니다.
이는 [`MergeTree`](/ko/engines/table-engines/mergetree-family/mergetree) 테이블 엔진 계열에서만 사용할 수 있습니다.

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

`filter_expr`는 `UInt8` 타입이어야 합니다. 이 쿼리는 `filter_expr`가 0이 아닌 값을 갖는 행에서 지정된 컬럼의 값을 해당 표현식의 값으로 업데이트합니다.
값은 `CAST` 연산자를 사용해 컬럼 타입으로 변환됩니다. 프라이머리 키 또는 파티션 키 계산에 사용되는 컬럼은 업데이트할 수 없습니다.

<div id="examples">
  ## 예시
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## Lightweight updates는 데이터를 즉시 업데이트하지 않습니다
</div>

Lightweight `UPDATE`는 업데이트된 컬럼과 행만 포함하는 특수한 종류의 데이터 파트인 **패치 파트**를 사용해 구현됩니다.
Lightweight `UPDATE`는 패치 파트를 생성하지만, 스토리지에 저장된 원본 데이터를 즉시 물리적으로 수정하지는 않습니다.
업데이트 과정은 `INSERT ... SELECT ...` 쿼리와 유사하지만, `UPDATE` 쿼리는 패치 파트 생성이 완료될 때까지 기다린 후 결과를 반환합니다.

업데이트된 값은 다음과 같습니다.

* 패치 적용을 통해 `SELECT` 쿼리에서 **즉시 확인할 수 있습니다**
* 후속 머지 및 뮤테이션 과정에서만 **물리적으로 구체화됩니다**
* 모든 활성 파트에 패치가 구체화되면 **자동으로 정리됩니다**

<div id="lightweight-update-requirements">
  ## 경량 업데이트 요구 사항
</div>

경량 업데이트는 [`MergeTree`](/ko/engines/table-engines/mergetree-family/mergetree), [`ReplacingMergeTree`](/ko/engines/table-engines/mergetree-family/replacingmergetree), [`CollapsingMergeTree`](/ko/engines/table-engines/mergetree-family/collapsingmergetree), [`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree) 엔진과 해당 [`Replicated`](/ko/engines/table-engines/mergetree-family/replication.md) 및 [`Shared`](/ko/cloud/reference/shared-merge-tree) 버전에서 지원됩니다.

경량 업데이트를 사용하려면 테이블 설정 [`enable_block_number_column`](/ko/operations/settings/merge-tree-settings#enable_block_number_column) 및 [`enable_block_offset_column`](/ko/operations/settings/merge-tree-settings#enable_block_offset_column)을 사용하여 `_block_number` 및 `_block_offset` 컬럼의 머티리얼라이즈를 활성화해야 합니다.

<div id="lightweight-delete">
  ## 경량 DELETE
</div>

[경량 `DELETE`](/ko/sql-reference/statements/delete) 쿼리는 `ALTER UPDATE` mutation 대신 경량 `UPDATE`로 실행할 수 있습니다. 경량 `DELETE`의 동작은 [`lightweight_delete_mode`](/ko/operations/settings/settings#lightweight_delete_mode) 설정으로 제어됩니다.

<div id="performance-considerations">
  ## 성능 고려 사항
</div>

**경량 업데이트의 장점:**

* 업데이트의 지연 시간은 `INSERT ... SELECT ...` 쿼리의 지연 시간과 비슷합니다
* 전체 컬럼이 아니라 업데이트된 컬럼과 값만 데이터 파트에 기록됩니다
* 현재 실행 중인 머지/뮤테이션이 완료될 때까지 기다릴 필요가 없으므로 업데이트 지연 시간을 예측할 수 있습니다
* 경량 업데이트를 병렬로 실행할 수 있습니다

**잠재적인 성능 영향:**

* 패치를 적용해야 하는 `SELECT` 쿼리에는 오버헤드가 추가됩니다
* 패치를 적용해야 하는 데이터 파트의 컬럼에는 [스키핑 인덱스](/ko/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes)가 사용되지 않습니다. 또한 테이블에 패치 파트가 있으면, 패치를 적용할 필요가 없는 데이터 파트를 포함해 [프로젝션](/ko/engines/table-engines/mergetree-family/mergetree.md/#projections)도 사용되지 않습니다.
* 빈도가 너무 높은 소규모 업데이트는 &quot;too many parts&quot; 오류를 유발할 수 있습니다. 여러 업데이트를 하나의 쿼리로 묶는 것이 좋습니다. 예를 들어 업데이트할 ID를 `WHERE` 절의 단일 `IN` 절에 넣을 수 있습니다
* 경량 업데이트는 소량의 행(테이블의 약 10%까지)을 업데이트하도록 설계되었습니다. 더 많은 양을 업데이트해야 한다면 [`ALTER TABLE ... UPDATE`](/ko/sql-reference/statements/alter/update) 뮤테이션을 사용하는 것이 좋습니다

<div id="concurrent-operations">
  ## 동시 작업
</div>

경량 업데이트는 무거운 뮤테이션과 달리 현재 실행 중인 머지/뮤테이션이 완료될 때까지 기다리지 않습니다.
동시 경량 업데이트의 일관성은 설정 [`update_sequential_consistency`](/ko/operations/settings/settings#update_sequential_consistency) 및 [`update_parallel_mode`](/ko/operations/settings/settings#update_parallel_mode)로 제어됩니다.

<div id="update-permissions">
  ## UPDATE 권한
</div>

`UPDATE`를 사용하려면 `ALTER UPDATE` 권한이 필요합니다. 특정 사용자에게 특정 테이블(table)에서 `UPDATE` SQL 문을 허용하려면 다음을 실행하세요:

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## 구현 세부 사항
</div>

패치 파트는 일반 파트와 같지만, 업데이트된 컬럼과 몇 가지 시스템 컬럼만 포함합니다:

* `_part` - 원본 파트의 이름
* `_part_offset` - 원본 파트에서의 행 번호
* `_block_number` - 원본 파트에서 해당 행의 블록 번호
* `_block_offset` - 원본 파트에서 해당 행의 블록 오프셋
* `_data_version` - 업데이트된 데이터의 데이터 버전(`UPDATE` 쿼리에 할당된 블록 번호)

평균적으로 패치 파트에서는 업데이트된 각 행마다 약 40바이트의 오버헤드(uncompressed data)가 발생합니다.
시스템 컬럼은 업데이트해야 할 원본 파트의 행을 찾는 데 사용됩니다.
시스템 컬럼은 원본 파트의 [가상 컬럼(virtual columns)](/ko/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns)과 연관되어 있으며, 패치 파트를 적용해야 할 때 읽기 시 추가됩니다.
패치 파트는 `_part` 및 `_part_offset` 기준으로 정렬됩니다.

패치 파트는 원본 파트와는 다른 파티션에 속합니다.
패치 파트의 partition ID는 `patch-<hash of column names in patch part>-<original_partition_id>`입니다.
따라서 컬럼 구성이 다른 패치 파트는 서로 다른 파티션에 저장됩니다.
예를 들어 `SET x = 1 WHERE <cond>`, `SET y = 1 WHERE <cond>`, `SET x = 1, y = 1 WHERE <cond>`라는 3개의 업데이트는 각각 서로 다른 3개의 파티션에 3개의 패치 파트를 생성합니다.

패치 파트는 `SELECT` 쿼리에서 적용해야 하는 패치 수를 줄이고 오버헤드를 낮추기 위해 서로 머지될 수 있습니다. 패치 파트 머지에는 `_data_version`을 버전 컬럼로 사용하는 [replacing](/ko/engines/table-engines/mergetree-family/replacingmergetree) 머지 알고리즘이 사용됩니다.
따라서 패치 파트에는 각 업데이트된 행에 대해 항상 최신 버전만 저장됩니다.

경량 업데이트는 현재 실행 중인 머지와 뮤테이션이 끝날 때까지 기다리지 않으며, 항상 데이터 파트의 현재 스냅샷을 사용해 업데이트를 실행하고 패치 파트를 생성합니다.
이 때문에 패치 파트를 적용하는 방식은 두 가지 경우로 나뉠 수 있습니다.

예를 들어 파트 `A`를 읽을 때 패치 파트 `X`를 적용해야 한다고 가정하겠습니다:

* `X`에 파트 `A` 자체가 포함된 경우입니다. 이는 `UPDATE`가 실행될 때 `A`가 머지에 참여하고 있지 않았을 때 발생합니다.
* `X`에 파트 `A`에 포함된 `B`와 `C`가 포함된 경우입니다. 이는 `UPDATE`가 실행될 당시 (`B`, `C`) -&gt; `A` 머지가 진행 중이었을 때 발생합니다.

이 두 경우에 따라 패치 파트를 적용하는 방법도 각각 두 가지가 있습니다:

* 정렬된 컬럼 `_part`, `_part_offset`을 사용한 머지
* `_block_number`, `_block_offset` 컬럼을 사용한 join

join 모드는 머지 모드보다 느리고 더 많은 메모리를 필요로 하지만, 사용 빈도는 낮습니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* [`ALTER UPDATE`](/ko/sql-reference/statements/alter/update) - 대규모 `UPDATE` 작업
* [경량 DELETE](/ko/sql-reference/statements/delete) - 경량 `DELETE` 작업
* [`APPLY PATCHES`](/ko/sql-reference/statements/alter/apply-patches) - 패치를 데이터 파트에 강제로 물리적으로 머티리얼라이즈하는 mutation 작업