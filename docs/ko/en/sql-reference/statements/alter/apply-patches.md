---
description: '경량 업데이트의 패치 적용(APPLY PATCHES) 문서'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: '경량 업데이트의 패치 적용(APPLY PATCHES)'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

이 명령은 [lightweight `UPDATE`](/ko/sql-reference/statements/update) SQL 문으로 생성된 패치 파트의 물리적 머티리얼라이즈를 수동으로 시작합니다. 영향을 받은 컬럼만 다시 써서 대기 중인 패치를 데이터 파트에 강제로 적용합니다.

:::note

* 이 기능은 [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) 계열([복제된](../../../engines/table-engines/mergetree-family/replication.md) 테이블 포함)의 테이블에서만 작동합니다.
* 이는 mutation 작업이며 백그라운드에서 비동기로 실행됩니다.
  :::

<div id="when-to-use">
  ## `APPLY PATCHES`를 사용해야 하는 경우
</div>

:::tip
일반적으로 `APPLY PATCHES`를 사용할 필요는 없습니다.
:::

패치 파트는 [`apply_patches_on_merge`](/ko/operations/settings/merge-tree-settings#apply_patches_on_merge) 설정이 활성화되어 있으면(기본값) 일반적으로 머지 중에 자동으로 적용됩니다. 하지만 다음과 같은 경우에는 패치 적용을 수동으로 실행하는 것이 좋을 수 있습니다:

* `SELECT` 쿼리 중 패치 적용으로 인한 처리 오버헤드를 줄이기 위해
* 여러 패치 파트가 쌓이기 전에 미리 하나로 합치기 위해
* 패치가 이미 구체화된 상태로 데이터를 백업하거나 내보내기 위해 준비할 때
* `apply_patches_on_merge`가 비활성화되어 있고 패치를 적용할 시점을 직접 제어하려는 경우

<div id="examples">
  ## 예시
</div>

테이블에 대기 중인 모든 패치를 적용합니다:

```sql
ALTER TABLE my_table APPLY PATCHES;
```

특정 파티션에만 패치를 적용합니다:

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

다른 작업과 조합합니다:

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## 패치 적용 진행 상황 모니터링
</div>

[`system.mutations`](/ko/operations/system-tables/mutations) 테이블을 사용하면 패치 적용 진행 상황을 모니터링할 수 있습니다:

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## 관련 항목
</div>

* [경량 `UPDATE`](/ko/sql-reference/statements/update) - 경량 업데이트를 사용해 패치 파트를 생성합니다
* [`apply_patches_on_merge` 설정](/ko/operations/settings/merge-tree-settings#apply_patches_on_merge) - 머지 중 패치가 자동으로 적용되는 방식을 제어합니다