---
description: 'ALTER에 대한 문서'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'reference'
---

대부분의 `ALTER TABLE` 쿼리는 테이블 설정이나 데이터를 수정합니다.

| 수정자                                                                         |
| --------------------------------------------------------------------------- |
| [COLUMN](/ko/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/ko/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/ko/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/ko/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/ko/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/ko/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/ko/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/ko/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/ko/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/ko/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/ko/sql-reference/statements/alter/apply-patches.md)           |

:::note
대부분의 `ALTER TABLE` 쿼리는 [*MergeTree](/ko/engines/table-engines/mergetree-family/index.md), [Merge](/ko/engines/table-engines/special/merge.md), [Distributed](/ko/engines/table-engines/special/distributed.md) 테이블에서만 지원됩니다.
:::

다음 `ALTER` SQL 문은 뷰를 조작합니다.

| SQL 문                                                                   | 설명                                                                             |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| [ALTER TABLE ... MODIFY QUERY](/ko/sql-reference/statements/alter/view.md) | [구체화된 뷰(Materialized View)](/ko/sql-reference/statements/create/view)의 구조를 수정합니다. |

다음 `ALTER` SQL 문은 역할 기반 접근 제어와 관련된 엔터티를 수정합니다.

| SQL 문                                                                   |
| ----------------------------------------------------------------------- |
| [USER](/ko/sql-reference/statements/alter/user.md)                         |
| [ROLE](/ko/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/ko/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/ko/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/ko/sql-reference/statements/alter/settings-profile.md) |

| SQL 문                                                                         | 설명                                                      |
| ----------------------------------------------------------------------------- | ------------------------------------------------------- |
| [ALTER TABLE ... MODIFY COMMENT](/ko/sql-reference/statements/alter/comment.md)  | 이전 설정 여부와 관계없이 테이블에 comment를 추가, 수정 또는 제거합니다.           |
| [ALTER NAMED COLLECTION](/ko/sql-reference/statements/alter/named-collection.md) | [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 수정합니다. |

<div id="mutations">
  ## 뮤테이션
</div>

테이블 데이터를 조작하는 `ALTER` 쿼리는 &quot;뮤테이션&quot;이라는 메커니즘으로 구현되며, 대표적인 예로 [ALTER TABLE ... DELETE](/ko/sql-reference/statements/alter/delete.md)와 [ALTER TABLE ... UPDATE](/ko/sql-reference/statements/alter/update.md)가 있습니다. 이는 [MergeTree](/ko/engines/table-engines/mergetree-family/index.md) 테이블의 머지와 유사한 비동기 백그라운드 프로세스이며, 파트의 새로운 &quot;mutated&quot; 버전을 생성합니다.

`*MergeTree` 테이블에서 뮤테이션은 **전체 데이터 파트를 다시 기록하는 방식으로** 실행됩니다.
원자성(atomicity)은 보장되지 않습니다. 준비가 완료되는 즉시 기존 파트가 뮤테이션된 파트로 교체되므로, 뮤테이션이 진행되는 동안 실행을 시작한 `SELECT` 쿼리는 이미 뮤테이션된 파트의 데이터와 아직 뮤테이션되지 않은 파트의 데이터를 함께 볼 수 있습니다.

뮤테이션은 생성된 순서대로 완전히 순서가 정해지며, 각 파트에도 그 순서대로 적용됩니다. 또한 뮤테이션은 `INSERT INTO` 쿼리와는 부분적으로만 순서가 정해집니다. 즉, 뮤테이션이 제출되기 전에 테이블에 삽입된 데이터는 뮤테이션되지만, 그 이후에 삽입된 데이터는 뮤테이션되지 않습니다. 뮤테이션이 어떤 방식으로도 삽입을 차단하지 않는다는 점에 유의하십시오.

뮤테이션 쿼리는 뮤테이션 항목이 추가된 직후 즉시 반환됩니다(복제된 테이블의 경우 ZooKeeper에, 비복제 테이블의 경우 파일 시스템에 추가됨). 뮤테이션 자체는 시스템 profile 설정을 사용해 비동기적으로 실행됩니다. 뮤테이션의 진행 상황을 추적하려면 [`system.mutations`](/ko/operations/system-tables/mutations) 테이블을 사용할 수 있습니다. 성공적으로 제출된 뮤테이션은 ClickHouse 서버가 재시작되더라도 계속 실행됩니다. 제출된 이후에는 뮤테이션을 롤백할 방법이 없지만, 어떤 이유로 뮤테이션이 멈춘 경우 [`KILL MUTATION`](/ko/sql-reference/statements/kill.md/#kill-mutation) 쿼리로 취소할 수 있습니다.

완료된 뮤테이션 항목은 즉시 삭제되지 않습니다(보존되는 항목 수는 `finished_mutations_to_keep` 스토리지 엔진 매개변수로 결정됩니다). 더 오래된 뮤테이션 항목은 삭제됩니다.

<div id="synchronicity-of-alter-queries">
  ## ALTER 쿼리의 동기 처리
</div>

복제되지 않은 테이블에서는 모든 `ALTER` 쿼리가 동기적으로 수행됩니다. 복제된 테이블(Replicated Table)에서는 쿼리가 적절한 작업에 대한 지시만 `ZooKeeper`에 추가하고, 실제 작업은 가능한 한 빨리 수행됩니다. 다만 쿼리가 이러한 작업이 모든 레플리카에서 완료될 때까지 기다리도록 할 수 있습니다.

뮤테이션을 생성하는 `ALTER` 쿼리(`UPDATE`, `DELETE`, `MATERIALIZE INDEX`, `MATERIALIZE PROJECTION`, `MATERIALIZE COLUMN`, `APPLY DELETED MASK`, `APPLY PATCHES`, `CLEAR STATISTIC`, `MATERIALIZE STATISTIC` 등이 이에 해당함)의 동기 처리 방식은 [mutations&#95;sync](/ko/operations/settings/settings.md/#mutations_sync) 설정으로 정의됩니다.

메타데이터만 수정하는 다른 `ALTER` 쿼리의 경우 [alter&#95;sync](/ko/operations/settings/settings#alter_sync) 설정을 사용해 대기 방식을 지정할 수 있습니다.

비활성 레플리카가 모든 `ALTER` 쿼리를 실행할 때까지 얼마나 오래 기다릴지(초 단위)는 [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ko/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 설정으로 지정할 수 있습니다.

:::note
모든 `ALTER` 쿼리에서 `alter_sync = 2`이고 일부 레플리카가 `replication_wait_for_inactive_replica_timeout` 설정에 지정된 시간보다 오래 비활성 상태로 남아 있으면 `UNFINISHED` 예외가 발생합니다.
:::

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 업데이트와 삭제 처리](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)