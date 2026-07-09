---
description: 'ClickHouse Cloud에서 사용할 수 있는 `Shared` 데이터베이스 엔진을 소개하는 페이지'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="shared-database-engine">
  # 공유 데이터베이스 엔진
</div>

`Shared` 데이터베이스 엔진은 Shared Catalog와 함께 사용되어, [`SharedMergeTree`](/ko/cloud/reference/shared-merge-tree)와 같은 stateless 테이블 엔진을 사용하는 테이블이 있는 데이터베이스를 관리합니다.
이러한 테이블 엔진은 영구 상태를 디스크에 기록하지 않으며, 동적 컴퓨트 환경과 호환됩니다.

Cloud의 `Shared` 데이터베이스 엔진은 로컬 디스크 의존성을 없앱니다.
이 엔진은 전적으로 메모리 내에서 동작하며, CPU와 메모리만 필요합니다.

<div id="how-it-works">
  ## 어떻게 작동합니까?
</div>

`Shared` 데이터베이스 엔진은 Keeper를 기반으로 하는 중앙 Shared Catalog에 모든 데이터베이스와 테이블 정의를 저장합니다. 로컬 디스크에 기록하는 대신, 모든 컴퓨트 노드가 공유하는 단일한 버전 관리 전역 상태를 유지합니다.

각 노드는 마지막으로 적용된 버전만 추적하며, 시작 시 로컬 파일이나 수동 설정 없이 최신 상태를 가져옵니다.

<div id="syntax">
  ## 구문
</div>

최종 사용자는 Shared Catalog와 공유 데이터베이스 엔진을 사용할 때 추가 구성이 필요하지 않습니다. 데이터베이스 생성 방식도 기존과 동일합니다:

```sql
CREATE DATABASE my_database;
```

ClickHouse Cloud는 데이터베이스에 공유 데이터베이스 엔진을 자동으로 할당합니다. 이러한 데이터베이스에서 상태 비저장 엔진을 사용해 생성된 모든 테이블은 Shared Catalog의 복제 및 조정 기능을 자동으로 활용할 수 있습니다.

:::tip
Shared Catalog와 그 이점에 관한 자세한 내용은 Cloud 참고 섹션의 [&quot;Shared Catalog 및 공유 데이터베이스 엔진&quot;](/ko/cloud/reference/shared-catalog)를 참조하십시오.
:::