---
description: '`remote_servers` 섹션에 구성된 클러스터의 모든 세그먼트에 Distributed 테이블을 생성하지 않고 액세스할 수 있습니다.'
sidebar_label: 'cluster'
sidebar_position: 30
slug: /sql-reference/table-functions/cluster
title: 'clusterAllReplicas'
doc_type: '참고'
---

[Distributed](../../engines/table-engines/special/distributed.md) 테이블을 생성하지 않고 클러스터의 모든 세그먼트(`remote_servers` 섹션에 구성됨)에 액세스할 수 있습니다. 각 세그먼트에서는 하나의 레플리카만 쿼리됩니다.

`clusterAllReplicas` 함수는 `cluster`와 같지만, 모든 레플리카를 쿼리합니다. 클러스터의 각 레플리카는 별도의 세그먼트/연결로 사용됩니다.

:::note
사용 가능한 모든 클러스터는 [system.clusters](../../operations/system-tables/clusters.md) 테이블에 나열되어 있습니다.
:::

<div id="syntax">
  ## 구문
</div>

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```

<div id="arguments">
  ## 인수
</div>

| 인수                          | 유형                                                                             |
| --------------------------- | ------------------------------------------------------------------------------ |
| `cluster_name`              | 원격 및 로컬 서버의 주소 집합과 연결 매개변수를 구성하는 데 사용되는 클러스터 이름입니다. 지정하지 않으면 `default`를 사용합니다. |
| `db.table` or `db`, `table` | 데이터베이스와 테이블의 이름입니다.                                                            |
| `sharding_key`              | 세그먼트 분할 키입니다. 선택 사항입니다. 클러스터에 세그먼트가 2개 이상 있으면 지정해야 합니다.                        |

<div id="returned_value">
  ## 반환 값
</div>

클러스터에서 가져온 데이터셋입니다.

<div id="using_macros">
  ## 매크로 사용
</div>

`cluster_name`에는 `{}`로 지정하는 매크로 치환을 사용할 수 있습니다. 치환되는 값은 서버 구성 파일의 [macros](../../operations/server-configuration-parameters/settings.md#macros) 섹션에서 가져옵니다.

예시:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

<div id="usage_recommendations">
  ## 사용 및 권장 사항
</div>

`cluster` 및 `clusterAllReplicas` 테이블 함수를 사용하는 것은 `Distributed` 테이블을 생성하는 것보다 효율성이 떨어집니다. 이 경우 각 요청마다 서버 연결이 다시 설정되기 때문입니다. 많은 수의 쿼리를 처리할 때는 항상 `Distributed` 테이블을 미리 생성하고, `cluster` 및 `clusterAllReplicas` 테이블 함수는 사용하지 마십시오.

`cluster` 및 `clusterAllReplicas` 테이블 함수는 다음과 같은 경우에 유용할 수 있습니다:

* 데이터 비교, 디버깅 및 테스트를 위해 특정 클러스터에 접근하는 경우
* 조사 목적으로 다양한 ClickHouse 클러스터와 레플리카에 쿼리하는 경우
* 수동으로 수행하는 드문 분산 요청

`host`, `port`, `user`, `password`, `compression`, `secure`와 같은 연결 설정은 `<remote_servers>` 구성 섹션에서 가져옵니다. 자세한 내용은 [Distributed engine](../../engines/table-engines/special/distributed.md)을 참조하십시오.

<div id="related">
  ## 관련 항목
</div>

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [load&#95;balancing](../../operations/settings/settings.md#load_balancing)