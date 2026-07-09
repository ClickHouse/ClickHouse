---
description: '항상 RAM에 있는 데이터 집합입니다. `IN` operator의 오른쪽에서 사용하도록 설계되었습니다.'
sidebar_label: 'Set'
sidebar_position: 60
slug: /engines/table-engines/special/set
title: 'Set 테이블 엔진'
doc_type: '참고'
---

:::note
ClickHouse Cloud에서 서비스가 25.4 이전 버전으로 생성된 경우, `SET compatibility=25.4`를 사용해 호환성을 최소 25.4로 설정해야 합니다.
:::

항상 RAM에 있는 데이터 집합입니다. `IN` operator의 오른쪽에서 사용하도록 설계되었습니다(&quot;IN operators&quot; 섹션 참조).

`INSERT`를 사용해 테이블에 데이터를 삽입할 수 있습니다. 새 요소는 데이터 집합에 추가되며, 중복 요소는 무시됩니다.
하지만 테이블에서 `SELECT`를 수행할 수는 없습니다. 데이터를 가져오는 유일한 방법은 `IN` operator의 오른쪽 부분에서 사용하는 것입니다.

데이터는 항상 RAM에 저장됩니다. `INSERT` 시에는 삽입된 데이터의 블록도 디스크에 있는 테이블 디렉터리에 기록됩니다. 서버가 시작되면 이 데이터가 RAM으로 로드됩니다. 즉, 재시작 후에도 데이터는 유지됩니다.

서버가 비정상적으로 재시작되면 디스크에 있는 데이터 블록이 손실되거나 손상될 수 있습니다. 이 경우 손상된 데이터가 들어 있는 파일을 수동으로 삭제해야 할 수 있습니다.

<div id="join-limitations-and-settings">
  ### 제한 사항 및 설정
</div>

테이블을 생성하면 다음 설정이 적용됩니다:

<div id="persistent">
  #### 영속성
</div>

Set 및 [Join](/ko/engines/table-engines/special/join) 테이블 엔진의 영속성을 비활성화합니다.

I/O 오버헤드를 줄여 줍니다. 성능을 우선하며 영속성이 필요하지 않은 시나리오에 적합합니다.

가능한 값:

* 1 — 활성화됨.
* 0 — 비활성화됨.

기본값: `1`.