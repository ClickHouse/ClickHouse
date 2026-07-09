---
description: '이 엔진은 Atomic 엔진을 기반으로 합니다. 특정 데이터베이스에 대해
  ZooKeeper에 기록되고 해당 데이터베이스의 모든 레플리카에서 실행되는 DDL 로그를
  통해 메타데이터 복제를 지원합니다.'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

이 엔진은 [Atomic](../../engines/database-engines/atomic.md) 엔진을 기반으로 합니다. 특정 데이터베이스에 대해 ZooKeeper에 기록되고 해당 데이터베이스의 모든 레플리카에서 실행되는 DDL 로그를 통해 메타데이터 복제를 지원합니다.

하나의 ClickHouse 서버에서 여러 개의 복제된 데이터베이스를 동시에 실행하고 업데이트할 수 있습니다. 하지만 동일한 복제된 데이터베이스의 레플리카를 여러 개 둘 수는 없습니다.

<div id="creating-a-database">
  ## 데이터베이스 생성
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**엔진 매개변수**

* `zoo_path` — ZooKeeper 경로입니다. 동일한 ZooKeeper 경로는 동일한 데이터베이스를 가리킵니다.
* `shard_name` — 세그먼트 이름입니다. 데이터베이스 레플리카는 `shard_name`을 기준으로 세그먼트로 그룹화됩니다.
* `replica_name` — 레플리카 이름입니다. 동일한 세그먼트에 속한 모든 레플리카의 이름은 서로 달라야 합니다.

매개변수는 생략할 수 있으며, 이 경우 누락된 매개변수에는 기본값이 적용됩니다.

`zoo_path`에 매크로 `{uuid}`가 포함된 경우, 이 데이터베이스의 모든 레플리카가 동일한 UUID를 사용하도록 명시적으로 UUID를 지정하거나 CREATE 문에 [ON CLUSTER](../../sql-reference/distributed-ddl.md)를 추가해야 합니다.

[ReplicatedMergeTree](/ko/engines/table-engines/mergetree-family/replication) 테이블에서는 인수가 제공되지 않으면 기본 인수인 `/clickhouse/tables/{uuid}/{shard}` 및 `{replica}`가 사용됩니다. 이는 서버 설정 [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) 및 [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name)에서 변경할 수 있습니다. 매크로 `{uuid}`는 테이블의 UUID로 치환되며, `{shard}`와 `{replica}`는 데이터베이스 엔진 인수가 아니라 서버 구성의 값으로 치환됩니다. 다만 앞으로는 Replicated 데이터베이스의 `shard_name`과 `replica_name`도 사용할 수 있게 될 예정입니다.

기본 ZooKeeper 클러스터 대신 복제된 데이터베이스의 메타데이터를 저장하기 위해 보조 ZooKeeper 클러스터를 사용하는 것도 지원됩니다. 다음과 같이 SQL을 사용하여 보조 ZooKeeper 클러스터로 복제된 데이터베이스를 생성할 수 있습니다:

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## 세부 사항 및 권장 사항
</div>

`Replicated` 데이터베이스에서의 DDL 쿼리는 [ON CLUSTER](../../sql-reference/distributed-ddl.md) 쿼리와 비슷하게 동작하지만, 몇 가지 차이점이 있습니다.

먼저 DDL 요청은 initiator(사용자로부터 요청을 처음 받은 호스트)에서 실행을 시도합니다. 요청이 수행되지 않으면 사용자는 즉시 오류를 받으며, 다른 호스트는 이를 수행하려고 시도하지 않습니다. 요청이 initiator에서 성공적으로 완료되면, 다른 모든 호스트는 완료될 때까지 자동으로 재시도합니다. initiator는 다른 호스트에서 쿼리가 완료되기를 기다리며([distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)보다 오래 기다리지는 않음), 각 호스트의 쿼리 실행 상태가 담긴 테이블을 반환합니다.

오류 발생 시 동작은 [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode) 설정으로 제어되며, `Replicated` 데이터베이스에서는 이를 `null_status_on_timeout`으로 설정하는 것이 좋습니다. 즉, 일부 호스트가 [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout) 내에 요청을 실행하지 못한 경우 예외를 발생시키지 않고, 대신 테이블에서 해당 호스트의 상태를 `NULL`로 표시합니다.

[system.clusters](../../operations/system-tables/clusters.md) 시스템 테이블(system table)에는 복제된 데이터베이스와 같은 이름의 클러스터가 있으며, 이 클러스터는 해당 데이터베이스의 모든 레플리카로 구성됩니다. 이 클러스터는 레플리카 생성/삭제 시 자동으로 갱신되며, [Distributed](/ko/engines/table-engines/special/distributed) 테이블에 사용할 수 있습니다.

데이터베이스의 새 레플리카를 생성하면, 해당 레플리카가 직접 테이블을 생성합니다. 레플리카를 오랫동안 사용할 수 없어 복제 로그보다 뒤처진 경우에는 로컬 메타데이터를 ZooKeeper의 현재 메타데이터와 비교하고, 추가로 존재하는 데이터 포함 테이블은 별도의 non-replicated 데이터베이스로 이동하며(불필요한 항목이 실수로 삭제되지 않도록), 누락된 테이블을 생성하고, 이름이 변경된 테이블이 있으면 그 이름도 갱신합니다. 데이터는 `ReplicatedMergeTree` 수준에서 복제됩니다. 즉, 테이블이 복제되지 않으면 데이터도 복제되지 않습니다(데이터베이스는 메타데이터만 담당함).

[`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) 쿼리는 허용되지만 복제되지는 않습니다. 데이터베이스 엔진은 현재 레플리카에 대해서만 파티션/파트를 추가, 가져오기 또는 제거합니다. 하지만 테이블 자체가 Replicated 테이블 엔진을 사용하는 경우에는 `ATTACH` 사용 후 데이터가 복제됩니다.

테이블 복제는 유지하지 않고 클러스터만 구성해야 하는 경우에는 [Cluster Discovery](../../operations/cluster-discovery.md) 기능을 참조하십시오.

<div id="usage-example">
  ## 사용 예시
</div>

호스트 3개로 클러스터를 생성합니다:

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

암시적 매개변수를 사용하여 클러스터에 데이터베이스 생성:

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

DDL 쿼리 실행:

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

시스템 테이블 표시:

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

분산 테이블을 생성하고 데이터를 삽입합니다:

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

호스트를 하나 더 추가해 레플리카를 구성합니다:

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

매크로 `{uuid}`를 `zoo_path`에 사용하는 경우 다른 호스트에 레플리카 추가:

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

클러스터 구성은 다음과 같습니다.

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

분산 테이블도 새 호스트로부터 데이터를 가져옵니다:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## 설정
</div>

다음 설정을 지원합니다:

| 설정                                                                           | 기본값                            | 설명                                                                                                                                                       |
| ---------------------------------------------------------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_broken_tables_ratio`                                                    | 1                              | 오래된 테이블 수가 전체 테이블 수에서 차지하는 비율이 이 값보다 크면 레플리카를 자동으로 복구하지 않습니다                                                                                             |
| `max_replication_lag_to_enqueue`                                             | 50                             | 복제 지연이 이 값보다 크면 해당 레플리카에서 쿼리 실행을 시도할 때 예외를 발생시킵니다                                                                                                        |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | 제한 시간을 초과했지만 initiator host가 아직 이를 실행하지 않은 경우, 레플리카는 쿼리 취소를 시도합니다                                                                                        |
| `collection_name`                                                            |                                | 클러스터 authentication에 필요한 모든 정보가 정의된 서버 구성의 collection 이름입니다                                                                                              |
| `check_consistency`                                                          | true                           | 로컬 metadata와 Keeper의 metadata 일관성을 검사하고, 불일치가 있으면 레플리카 복구를 수행합니다                                                                                         |
| `max_retries_before_automatic_recovery`                                      | 10                             | 큐 항목 실행의 최대 시도 횟수입니다. 이 횟수를 초과하면 레플리카를 손실된 것으로 표시하고 snapshot에서 복구합니다(0은 무한대를 의미합니다)                                                                      |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | 활성화하면 복제된 데이터베이스에서 DDLs를 처리할 때, 가능하면 갱신 가능 구체화 뷰의 임시 테이블에 대한 DDL 생성 및 교환을 건너뜁니다                                                                          |
| `logs_to_keep`                                                               | 1000                           | 복제된 데이터베이스에 대해 ZooKeeper에 유지할 기본 로그 수입니다.                                                                                                                |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | 데이터베이스의 ZooKeeper 경로입니다. 데이터베이스 생성 시 인수가 생략되면 사용됩니다.                                                                                                     |
| `default_replica_shard_name`                                                 | `{shard}`                      | 데이터베이스에서 레플리카의 세그먼트 이름입니다. 데이터베이스 생성 시 인수가 생략되면 사용됩니다.                                                                                                   |
| `default_replica_name`                                                       | `{replica}`                    | 데이터베이스에서 레플리카의 이름입니다. 데이터베이스 생성 시 인수가 생략되면 사용됩니다.                                                                                                        |
| `internal_replication`                                                       | false                          | 이 복제된 데이터베이스의 클러스터로 생성된 분산 테이블이 데이터를 레플리카 중 하나로 보낼지(내부 복제는 클러스터의 레플리카가 자체적으로 복제를 수행함), 아니면 모든 레플리카로 보낼지를 지정합니다(내부 복제가 없으면 분산 테이블이 삽입된 데이터를 모든 레플리카로 전송함) |

기본값은 설정 파일에서 재정의할 수 있습니다

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```