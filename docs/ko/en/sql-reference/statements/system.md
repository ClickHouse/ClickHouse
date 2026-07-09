---
description: 'SYSTEM SQL 문에 관한 문서'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'SYSTEM SQL 문'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # SYSTEM SQL 문
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

모든 [내부 딕셔너리](./create/dictionary/overview.md)를 다시 로드합니다.
기본적으로는 내부 딕셔너리가 비활성화되어 있습니다.
내부 딕셔너리 업데이트 결과와 관계없이 항상 `Ok.`를 반환합니다.

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

`SYSTEM RELOAD DICTIONARIES` 쿼리는 상태가 `LOADED`인 딕셔너리([`system.dictionaries`](/ko/operations/system-tables/dictionaries)의 `status` 컬럼 참조), 즉 이전에 성공적으로 로드된 딕셔너리를 다시 로드합니다.
기본적으로 딕셔너리는 지연 로드되므로([dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load) 참조), 시작 시 자동으로 로드되지 않으며 [`dictGet`](/ko/sql-reference/functions/ext-dict-functions#dictGet) 함수를 통해 처음 액세스하거나 `ENGINE = Dictionary`인 테이블에서 `SELECT`를 사용할 때 초기화됩니다.

**구문**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

딕셔너리 상태(LOADED / NOT&#95;LOADED / FAILED)와 관계없이 딕셔너리 `dictionary_name`를 완전히 다시 로드합니다.
딕셔너리 업데이트 결과와 무관하게 항상 `Ok.`를 반환합니다.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

`system.dictionaries` 테이블을 쿼리하면 딕셔너리의 상태를 확인할 수 있습니다.

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
이 명령문과 `SYSTEM RELOAD MODEL`은 clickhouse-library-bridge에서 catboost 모델을 언로드할 뿐입니다. `catboostEvaluate()` 함수는 아직 로드되지 않은 모델에 처음 접근하면 해당 모델을 로드합니다.
:::

모든 CatBoost 모델을 언로드합니다.

**구문**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

`model_path`의 CatBoost 모델을 언로드합니다.

**구문**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

설정 파일로부터 등록된 모든 [실행형 사용자 정의 함수](/ko/sql-reference/functions/udf#executable-user-defined-functions) 또는 그중 하나를 다시 로드합니다.

**구문**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

모든 [비동기 메트릭](../../operations/system-tables/asynchronous_metrics.md)을 다시 계산합니다. 비동기 메트릭은 설정 [asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md)에 따라 주기적으로 갱신되므로, 일반적으로는 이 구문을 사용해 수동으로 갱신할 필요가 없습니다.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP DNS CACHE
</div>

ClickHouse의 내부 DNS 캐시를 지웁니다. 경우에 따라(구버전 ClickHouse에서는) 인프라를 변경할 때(다른 ClickHouse 서버의 IP 주소를 변경하거나 딕셔너리에서 사용하는 서버를 변경할 때) 이 명령을 사용해야 합니다.

캐시를 더 편리하게(자동으로) 관리하려면 `disable_internal_dns_cache`, `dns_cache_max_entries`, `dns_cache_update_period` 매개변수를 참조하십시오.

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

마크 캐시를 삭제합니다.

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

메모리에 [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 테이블의 프라이머리 키를 보관하는 프라이머리 인덱스 캐시를 비웁니다.
이 캐시의 크기는 서버 수준 설정 [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size)로 구성됩니다.

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

Iceberg 메타데이터 캐시를 삭제합니다.

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

`AvroConfluent` 포맷에서 사용하는 URL별 Confluent 스키마 레지스트리 캐시를 비웁니다. 이 명령은 스키마 조회 캐시(id → schema)와 스키마 등록 캐시(subject + schema → id)를 모두 삭제하므로, 이후 읽기 및 쓰기는 레지스트리 서버로 폴백됩니다. 레지스트리 쪽에서 스키마가 삭제되었거나 재작성된 경우, 또는 테스트에서 레지스트리의 멱등성을 검증할 때 유용합니다.

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

Parquet 메타데이터 캐시를 비웁니다.

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

[`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon) 함수에서 사용하는 전처리된 상수 Polygon의 캐시를 비웁니다. 설정된 크기 제한(`point_in_polygon_cache_size` 서버 설정)은 그대로 유지되므로, 이후에도 캐시는 계속 항목을 저장합니다. 캐시를 비활성화하려면 `point_in_polygon_cache_size`를 `0`으로 설정하십시오.

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

텍스트 인덱스의 토큰, 헤더 및 postings 캐시를 지웁니다.

이 캐시 중 하나만 개별적으로 삭제하려면 다음 명령을 실행할 수 있습니다.

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE`, 또는
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

보조(데이터 스키핑) 인덱스용 마크 캐시를 비웁니다.

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

보조(데이터 스키핑) 인덱스용 압축 해제 블록 캐시를 지웁니다.

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

메모리 매핑 파일 캐시를 비웁니다.

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

사용자 공간 페이지 캐시, 즉 기반 스토리지에서 읽은 데이터를 저장하는 ClickHouse 자체 인메모리 캐시를 비웁니다.

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE
</div>

벡터 유사도 인덱스 캐시를 비웁니다.

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

아웃바운드 연결에 사용되는 HTTP 연결 풀의 캐시를 삭제합니다.

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

S3 클라이언트 캐시를 삭제합니다.

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

테이블의 마크를 [마크 캐시](#drop-mark-cache)에 로드합니다. 보조 인덱스 마크도 [인덱스 마크 캐시](#drop-index-mark-cache)에 함께 로드됩니다.

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

`MergeTree` 테이블의 프라이머리 인덱스를 [프라이머리 인덱스 캐시](#drop-primary-index-cache)로 불러옵니다.

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK METADATA CACHE
</div>

지정된 디스크의 메타데이터 캐시를 비웁니다.

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

ClickHouse의 파일 시스템 캐시 인메모리 상태를 디스크에 실제로 존재하는 캐시 파일과 동기화하고, 캐시된 각 파일 세그먼트의 `cache_name`, `path`, 다운로드된 `size`를 반환합니다. 선택적으로 캐시 이름을 지정하면 작업이 단일 캐시로 제한됩니다.

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE`는 ClickHouse Cloud에서만 사용할 수 있습니다.
:::

Distributed Cache를 삭제합니다. Distributed Cache 서버로의 캐시된 연결만 삭제하려면 `CONNECTIONS`를 사용하고, 단일 서버만 대상으로 하려면 서버 식별자를 지정하십시오.

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

`ReplicatedMergeTree` 테이블의 더 이상 동작하지 않는 레플리카는 다음 구문을 사용하여 삭제할 수 있습니다:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

이 쿼리는 ZooKeeper에서 `ReplicatedMergeTree` 레플리카 경로를 제거합니다. 레플리카가 더 이상 동작하지 않고 해당 테이블도 이미 존재하지 않아 `DROP TABLE`로 ZooKeeper에서 메타데이터를 제거할 수 없을 때 유용합니다. 비활성 상태이거나 오래된 레플리카만 삭제할 수 있으며, 로컬 레플리카는 삭제할 수 없으므로 이 경우에는 `DROP TABLE`을 사용하십시오. `DROP REPLICA`는 어떤 테이블도 삭제하지 않으며, 디스크에서 데이터나 메타데이터도 제거하지 않습니다.

첫 번째 항목은 `database.table` 테이블의 `'replica_name'` 레플리카 메타데이터를 제거합니다.
두 번째 항목은 데이터베이스의 모든 복제된 테이블에 대해 동일한 작업을 수행합니다.
세 번째 항목은 로컬 server의 모든 복제된 테이블에 대해 동일한 작업을 수행합니다.
네 번째 항목은 테이블의 다른 모든 레플리카가 삭제된 경우, 더 이상 동작하지 않는 레플리카의 메타데이터를 제거할 때 유용합니다. 이 경우 테이블 경로를 명시적으로 지정해야 합니다. 이 경로는 테이블 생성 시 `ReplicatedMergeTree` engine의 첫 번째 인수로 전달한 경로와 동일해야 합니다.

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

더 이상 동작하지 않는 `Replicated` 데이터베이스의 레플리카는 다음 구문을 사용해 삭제할 수 있습니다:

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

`SYSTEM DROP REPLICA`와 비슷하지만, `DROP DATABASE`를 실행할 데이터베이스가 없을 때 ZooKeeper에서 `Replicated` 데이터베이스의 레플리카 경로를 삭제합니다. 다만 `ReplicatedMergeTree` 레플리카는 삭제하지 않으므로 `SYSTEM DROP REPLICA`도 필요할 수 있습니다. 세그먼트 및 레플리카 이름은 데이터베이스를 생성할 때 `Replicated` 엔진 인수에 지정한 이름입니다. 또한 이러한 이름은 `system.clusters`의 `database_shard_name` 및 `database_replica_name` 컬럼에서 확인할 수 있습니다. `FROM SHARD` 절이 없으면 `replica_name`은 `shard_name|replica_name` 포맷의 전체 레플리카 이름이어야 합니다.

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE
</div>

압축 해제된 데이터 캐시를 지웁니다.
압축 해제된 데이터 캐시는 쿼리/사용자/프로필 수준 설정인 [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache)로 활성화하거나 비활성화할 수 있습니다.
캐시 크기는 서버 수준 설정인 [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size)로 구성할 수 있습니다.

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

컴파일된 표현식 캐시를 지웁니다.
컴파일된 표현식 캐시는 쿼리/사용자/프로필 수준의 설정 [`compile_expressions`](../../operations/settings/settings.md#compile_expressions)으로 활성화하거나 비활성화할 수 있습니다.

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

쿼리 조건 캐시를 지웁니다.

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP QUERY CACHE
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

[쿼리 캐시](../../operations/query-cache.md)를 비웁니다.
태그를 지정한 경우, 해당 태그가 있는 쿼리 캐시 엔트리만 삭제됩니다.

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

[`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path)에서 로드된 스키마 캐시를 지웁니다.

지원되는 대상:

* Protobuf: 메모리에 로드된 Protobuf 메시지 정의를 제거합니다.
* Files: `format_schema_source`가 `query`로 설정된 경우 생성되어 [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path)에 로컬로 저장된 캐시 스키마 파일을 삭제합니다.
  참고: 대상을 지정하지 않으면 두 캐시가 모두 지워집니다.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

버퍼링된 로그 메시지를 시스템 테이블(system tables)로 플러시합니다. 예를 들어 `system.query_log`가 있습니다. 대부분의 시스템 테이블은 기본 플러시 인터벌이 7.5초이므로, 주로 디버깅에 유용합니다.
메시지 큐가 비어 있어도 시스템 테이블을 생성합니다.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

전체를 플러시하고 싶지 않다면, 이름이나 대상 테이블을 지정해 개별 로그를 하나 이상 플러시할 수 있습니다:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

ClickHouse 구성을 다시 로드합니다. 구성 정보가 ZooKeeper에 저장되어 있을 때 사용합니다. `SYSTEM RELOAD CONFIG`는 ZooKeeper에 저장된 `USER` 구성은 다시 로드하지 않으며, `users.xml`에 저장된 `USER` 구성만 다시 로드합니다. 모든 `USER` 구성을 다시 로드하려면 `SYSTEM RELOAD USERS`를 사용하십시오.

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

users.xml, 로컬 디스크 액세스 스토리지, 복제된(ZooKeeper의) 액세스 스토리지를 포함한 모든 액세스 스토리지를 다시 로드합니다.

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

일반적으로 ClickHouse를 종료합니다(`service clickhouse-server stop` / `kill {$pid_clickhouse-server}`와 같은 방식).

<div id="kill">
  ## SYSTEM KILL
</div>

ClickHouse 프로세스를 강제 종료합니다(예: `kill -9 {$ pid_clickhouse-server}`)

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

`ENABLE_XRAY=1`로 ClickHouse를 빌드한 경우 사용할 수 있는 LLVM의 XRay 기능을 사용하여 계측 지점을 관리합니다.
이를 통해 소스 코드를 수정하지 않고도 운영 환경에서 최소한의 오버헤드로 디버깅 및 프로파일링을 수행할 수 있습니다.
계측 지점을 추가하지 않은 경우에는 200개 이상의 명령어로 이루어진 함수의 프롤로그와 에필로그에 인접한 주소로 점프하는 명령이 하나 더 추가될 뿐이므로
성능 저하는 무시할 수 있을 정도로 미미합니다.

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

새 계측 지점을 추가합니다. 계측이 적용된 함수는 [`system.instrumentation`](../../operations/system-tables/instrumentation.md) 시스템 테이블(system table)에서 확인할 수 있습니다. 동일한 함수에 둘 이상의 핸들러를 추가할 수 있으며, 계측이 추가된 순서대로 실행됩니다.
계측할 함수는 [`system.symbols`](../../operations/system-tables/symbols.md) 시스템 테이블(system table)에서 확인할 수 있습니다.

함수에 추가할 수 있는 핸들러는 세 가지 종류가 있습니다.

**구문**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

여기서 `FUNCTION`은 `QueryMetricLog::startQuery`와 같은 함수 또는 함수명의 부분 문자열일 수 있으며, handler는 다음 항목 중 하나입니다

<div id="instrument-add-log">
  #### LOG
</div>

인수로 전달된 텍스트와 스택 트레이스를 함수의 `ENTRY` 또는 `EXIT`에서 출력합니다.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

`ENTRY` 또는 `EXIT`에서 정해진 초 수만큼 대기합니다:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

또는 공백으로 구분된 최소값과 최대값을 지정해 균등 분포를 따르는 임의의 초 단위 값을 사용할 수 있습니다:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

함수의 `ENTRY`와 `EXIT` 사이에 걸린 시간을 측정합니다.
프로파일링 결과는 [`system.trace_log`](../../operations/system-tables/trace_log.md)에 저장되며,
[Chrome Event Trace Format](../../operations/system-tables/trace_log.md#chrome-event-trace-format)으로 변환할 수 있습니다.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

다음 구문으로 단일 계측 지점을 제거할 수 있습니다:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

`ALL` 키워드를 사용해 모두:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

서브쿼리에서 가져온 ID 집합:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

또는 지정한 function&#95;name과 일치하는 모든 계측 지점:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

계측 지점 정보는 [`system.instrumentation`](../../operations/system-tables/instrumentation.md) 시스템 테이블에서 조회할 수 있습니다.

<div id="managing-distributed-tables">
  ## 분산 테이블 관리
</div>

ClickHouse는 [분산](../../engines/table-engines/special/distributed.md) 테이블을 관리할 수 있습니다. 사용자가 이러한 테이블에 데이터를 삽입하면, ClickHouse는 먼저 클러스터 노드로 전송할 데이터의 큐를 만든 다음 이를 비동기적으로 전송합니다. [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed), [`START DISTRIBUTED SENDS`](#start-distributed-sends) 쿼리를 사용해 큐 처리를 관리할 수 있습니다. 또한 [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert) 설정을 사용해 분산 테이블에 데이터를 동기식으로 삽입할 수도 있습니다.

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

분산 테이블에 데이터를 삽입할 때 수행되는 백그라운드 데이터 전송을 비활성화합니다.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
[`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica)이 활성화되어 있으면(기본값) 로컬 세그먼트로의 데이터는 어쨌든 삽입됩니다.
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

ClickHouse가 데이터를 클러스터 노드로 동기적으로 전송하도록 강제합니다. 사용할 수 없는 노드가 하나라도 있으면 ClickHouse는 예외를 발생시키고 쿼리 실행을 중단합니다. 모든 노드가 다시 온라인 상태가 되면 성공하므로, 성공할 때까지 쿼리를 재시도할 수 있습니다.

`SETTINGS` 절을 통해 일부 설정을 재정의할 수도 있습니다. 이는 `max_concurrent_queries_for_all_users` 또는 `max_memory_usage`와 같은 일시적인 제한을 피하는 데 유용할 수 있습니다.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
각 대기 중인 블록은 초기 INSERT 쿼리의 설정을 사용해 디스크에 저장됩니다. 따라서 경우에 따라 설정을 재정의해야 할 수 있습니다.
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

분산 테이블에 데이터를 삽입할 때 백그라운드 데이터 분산 기능을 활성화합니다.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

지정된 포트와 프로토콜에서 서버 소켓을 닫고, 해당 연결로 서버에 접속한 기존 연결을 정상적으로 종료합니다.

다만 해당 프로토콜 설정이 clickhouse-server 구성에 지정되어 있지 않으면, 이 명령은 아무런 효과가 없습니다.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* `CUSTOM 'protocol'` 수정자를 지정하면 서버 구성의 protocols 섹션에 정의된 해당 이름의 사용자 지정 프로토콜이 중지됩니다.
* `QUERIES ALL [EXCEPT .. [,..]]` 수정자를 지정하면 `EXCEPT` 절에서 지정한 경우를 제외한 모든 프로토콜이 중지됩니다.
* `QUERIES DEFAULT [EXCEPT .. [,..]]` 수정자를 지정하면 `EXCEPT` 절에서 지정한 경우를 제외한 모든 기본 프로토콜이 중지됩니다.
* `QUERIES CUSTOM [EXCEPT .. [,..]]` 수정자를 지정하면 `EXCEPT` 절에서 지정한 경우를 제외한 모든 사용자 지정 프로토콜이 중지됩니다.

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

지정된 프로토콜로 새로운 연결을 받을 수 있게 합니다.

하지만 지정된 포트와 프로토콜의 서버가 SYSTEM STOP LISTEN 명령으로 중지된 상태가 아니라면, 이 명령은 아무런 효과가 없습니다.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## MergeTree 테이블 관리
</div>

ClickHouse는 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 테이블의 백그라운드 작업을 관리할 수 있습니다.

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

MergeTree 엔진 계열의 테이블에서 백그라운드 머지를 중지할 수 있습니다:

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
테이블을 `DETACH / ATTACH`하면 이전에 모든 MergeTree 테이블의 머지를 중지했더라도 해당 테이블의 백그라운드 머지가 시작됩니다.
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

MergeTree 엔진 계열 테이블의 백그라운드 머지를 시작할 수 있는 기능을 제공합니다:

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

MergeTree 엔진 계열 테이블에서 [TTL 표현식](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)에 따라 오래된 데이터를 백그라운드에서 삭제하는 작업을 중지할 수 있습니다:
테이블이 존재하지 않거나 MergeTree 엔진 테이블이 아닌 경우에도 `Ok.`를 반환합니다. 데이터베이스가 존재하지 않으면 오류를 반환합니다:

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

MergeTree 엔진 계열 테이블에서 [TTL 표현식](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)에 따라 오래된 데이터를 백그라운드에서 삭제하는 작업을 시작할 수 있습니다:
테이블이 존재하지 않아도 `Ok.`를 반환합니다. 데이터베이스가 존재하지 않으면 오류를 반환합니다:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

MergeTree 엔진 계열의 테이블에 대해 [TO VOLUME 또는 TO DISK 절이 포함된 TTL 테이블 표현식](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)에 따른 백그라운드 데이터 이동 작업을 중지할 수 있습니다:
테이블이 존재하지 않아도 `Ok.`를 반환합니다. 데이터베이스가 존재하지 않으면 오류를 반환합니다:

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

MergeTree 엔진 계열 테이블에 대해 [TO VOLUME 및 TO DISK 절이 포함된 TTL 테이블 표현식](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)에 따라 백그라운드 데이터 이동 작업을 시작할 수 있습니다:
테이블이 존재하지 않아도 `Ok.`를 반환합니다. 데이터베이스가 존재하지 않으면 오류를 반환합니다:

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

지정한 이름의 동결된 백업을 모든 디스크에서 제거합니다. 개별 파트의 동결 해제 방법에 대한 자세한 내용은 [ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/ko/sql-reference/statements/alter/partition#unfreeze-partition)을 참조하십시오.

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

테이블의 모든 비동기 로딩 데이터 파트(오래된 데이터 파트)가 로드될 때까지 기다립니다.

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## ReplicatedMergeTree 테이블 관리
</div>

ClickHouse는 [ReplicatedMergeTree](/ko/engines/table-engines/mergetree-family/replication) 테이블의 백그라운드 복제 관련 프로세스를 관리할 수 있습니다.

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

`ReplicatedMergeTree` 계열 테이블에서 삽입으로 생성된 파트의 백그라운드 페치를 중지할 수 있습니다.
테이블 엔진과 무관하게, 테이블이나 데이터베이스가 존재하지 않아도 항상 `Ok.`를 반환합니다.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

`ReplicatedMergeTree` 계열 테이블의 삽입된 파트에 대해 백그라운드 페치를 시작할 수 있습니다:
테이블 엔진과 관계없이, 테이블이나 데이터베이스가 존재하지 않더라도 항상 `Ok.`를 반환합니다.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

`ReplicatedMergeTree` 계열의 테이블에서 새로 삽입된 파트를 클러스터의 다른 레플리카로 전송하는 백그라운드 작업을 중지할 수 있습니다:

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

`ReplicatedMergeTree` 계열 테이블에서 새로 삽입된 파트를 클러스터 내 다른 레플리카로 백그라운드에서 전송하는 작업을 시작할 수 있습니다:

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

`ReplicatedMergeTree` 계열 테이블의 ZooKeeper에 저장된 복제 큐에서 백그라운드 페치 작업을 중지할 수 있습니다. 가능한 백그라운드 작업 유형으로는 머지, 페치, mutation, ON CLUSTER 절이 포함된 DDL SQL 문이 있습니다:

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

`ReplicatedMergeTree` 계열 테이블의 경우, ZooKeeper에 저장된 복제 큐에서 백그라운드 페치 작업을 시작할 수 있습니다. 가능한 백그라운드 작업 타입은 다음과 같습니다. merges, 페치, mutation, ON CLUSTER 절이 포함된 DDL SQL 문:

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

`ReplicatedMergeTree` 테이블에서 복제 로그의 새 항목을 복제 큐로 가져오는 작업을 중지합니다.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

`SYSTEM STOP PULLING REPLICATION LOG`을 해제합니다.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

`ReplicatedMergeTree` 테이블이 클러스터의 다른 레플리카와 동기화될 때까지 기다리되, 대기 시간은 `receive_timeout`초를 넘지 않습니다.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

이 구문을 실행하면 `[db.]replicated_merge_tree_family_table_name`은 공통 복제 로그의 명령을 자체 복제 큐로 가져온 다음, 레플리카가 가져온 모든 명령을 처리할 때까지 쿼리가 대기합니다. 다음 수정자를 지원합니다.

* `IF EXISTS`와 함께 사용하면(25.6부터 지원) 테이블이 존재하지 않아도 쿼리에서 오류가 발생하지 않습니다. 이는 클러스터에 새 레플리카를 추가할 때 유용합니다. 해당 레플리카가 이미 클러스터 구성에 포함되어 있지만, 아직 테이블을 생성하고 동기화하는 중일 수 있기 때문입니다.
* `STRICT` 수정자가 지정되면 쿼리는 복제 큐가 빌 때까지 대기합니다. 복제 큐에 새 항목이 계속 추가되면 `STRICT` 형식은 끝내 성공하지 못할 수도 있습니다.
* `LIGHTWEIGHT` 수정자가 지정되면 쿼리는 `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE`, `DROP_PART` 항목만 처리될 때까지 대기합니다.
  또한 `LIGHTWEIGHT` 수정자는 선택적 `FROM 'srcReplicas'` 절을 지원하며, 여기서 `'srcReplicas'`는 쉼표로 구분된 원본 레플리카 이름 목록입니다. 이 확장 기능을 사용하면 지정된 원본 레플리카에서 시작된 복제 작업에만 집중하여 더 정밀하게 동기화할 수 있습니다.
* `PULL` 수정자가 지정되면 쿼리는 ZooKeeper에서 새 복제 큐 항목을 가져오지만, 어떤 항목이 처리될 때까지도 대기하지 않습니다.

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

지정된 [복제된 데이터베이스](/ko/engines/database-engines/replicated)가 해당 데이터베이스의 DDL 큐에 있는 모든 스키마 변경 사항을 적용할 때까지 대기합니다.

**구문**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

`ReplicatedMergeTree` 테이블의 ZooKeeper 세션 상태를 다시 초기화합니다. 현재 상태를 기준 정보인 ZooKeeper와 비교한 뒤, 필요하면 ZooKeeper 큐에 작업을 추가합니다.
ZooKeeper 데이터를 기반으로 한 복제 큐 초기화는 `ATTACH TABLE` 문과 동일한 방식으로 수행됩니다. 이 과정에서 잠시 동안 테이블을 사용할 수 없습니다.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

데이터는 [있을 수 있지만] ZooKeeper 메타데이터(metadata)가 손실된 경우 레플리카를 복원합니다.

읽기 전용 `ReplicatedMergeTree` 테이블에서만 작동합니다.

다음과 같은 손실이 발생한 후에 이 쿼리를 실행할 수 있습니다:

* ZooKeeper 루트 `/` 손실
* 레플리카 경로 `/replicas` 손실
* 개별 레플리카 경로 `/replicas/replica_name/` 손실

레플리카는 로컬에서 발견한 파트를 ATTACH하고 해당 정보를 ZooKeeper에 전송합니다.
메타데이터 손실 전에 레플리카에 있던 파트는 `outdated` 상태가 아닌 한 다른 레플리카에서 다시 가져오지 않습니다(따라서 레플리카 복원은 네트워크를 통해 모든 데이터를 다시 다운로드한다는 의미가 아닙니다).

:::note
모든 상태의 파트는 `detached/` 폴더로 이동됩니다. 데이터 손실 전에 활성 상태였던 파트(`committed`)는 ATTACH됩니다.
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

데이터는 [있을 수 있지만] ZooKeeper 메타데이터가 손실된 경우 레플리카를 복원합니다.

**구문**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**예시**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**구문**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

다른 구문:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**예시**

여러 서버에서 테이블을 생성합니다. ZooKeeper에 저장된 레플리카 메타데이터가 손실되면 메타데이터가 없으므로 테이블이 읽기 전용으로 ATTACH됩니다. 마지막 쿼리는 모든 레플리카에서 실행해야 합니다.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

또 다른 방법:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

모든 `ReplicatedMergeTree` 테이블의 ZooKeeper 세션 상태를 다시 초기화할 수 있습니다. 현재 상태를 실제 기준 정보인 ZooKeeper와 비교하고, 필요하면 ZooKeeper 큐에 작업을 추가합니다.

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

파일 시스템 캐시를 삭제합니다.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
부하가 크며 오용될 소지가 있습니다.
:::

sync syscall을 실행합니다.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

지정한 테이블 또는 모든 테이블의 프라이머리 키를 로드합니다.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

지정한 테이블 또는 모든 테이블의 프라이머리 키(primary key)를 언로드합니다.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## 갱신 가능 구체화 뷰 관리
</div>

[갱신 가능 구체화 뷰](../../sql-reference/statements/create/view.md#refreshable-materialized-view)가 수행하는 백그라운드 작업을 제어하는 명령입니다.

사용 중에는 [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md)를 확인하십시오.

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

지정한 뷰 또는 모든 갱신 가능한 뷰의 주기적 갱신을 중지합니다. 갱신이 진행 중인 경우 해당 작업도 취소합니다.

뷰가 Replicated 또는 Shared 데이터베이스에 있는 경우, `STOP VIEW`는 현재 레플리카에만 영향을 미치며 `STOP REPLICATED VIEW`는 모든 레플리카에 영향을 미칩니다.

:::note
중지 상태는 서버를 다시 시작해도 유지되지 않습니다. 재시작 후에는 뷰가 구성된 갱신 일정에 따라 다시 갱신을 재개합니다.
Replicated 또는 Shared 데이터베이스에서 `SYSTEM STOP VIEW`는 현재 레플리카에만 영향을 미칩니다. 모든 레플리카의 갱신을 중지하려면 `SYSTEM STOP REPLICATED VIEW`를 사용하십시오.
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

지정된 뷰 또는 모든 갱신 가능한 뷰의 주기적 갱신을 활성화합니다. 즉시 갱신이 실행되지는 않습니다.

뷰가 Replicated 또는 Shared 데이터베이스에 있는 경우, `START VIEW`는 `STOP VIEW`로 중지된 상태를 해제하고, `START REPLICATED VIEW`는 `STOP REPLICATED VIEW`로 중지된 상태를 해제합니다. `START VIEW`는 `PAUSE VIEW`로 일시 중지된 상태도 해제합니다.

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

지정한 뷰 또는 모든 갱신 가능한 뷰의 주기적 갱신을 비활성화합니다.
`SYSTEM STOP VIEW`와 달리 `SYSTEM PAUSE VIEW`는 이미 진행 중인 갱신을 중단하지 않습니다. 현재 실행 중인 갱신은 완료되며, 이후의 갱신만 방지됩니다.

`SYSTEM START VIEW` 또는 `SYSTEM START VIEWS`로 해제할 수 있습니다.

:::note
일시 중지 상태는 서버를 재시작해도 유지되지 않습니다. 재시작 후에는 뷰가 설정된 갱신 일정에 따라 다시 갱신됩니다.
Replicated 또는 Shared 데이터베이스에서는 `SYSTEM PAUSE VIEW`가 현재 레플리카에만 영향을 미칩니다.
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

지정된 뷰를 예약된 일정과 관계없이 즉시 갱신합니다.

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

실행 중인 갱신이 완료될 때까지 기다립니다. 실행 중인 갱신이 없으면 즉시 종료됩니다. 가장 최근의 갱신 시도가 실패했다면 오류를 반환합니다.

새 갱신 가능 구체화 뷰를 생성한 직후(EMPTY 키워드 없이) 초기 갱신이 완료될 때까지 기다리는 데 사용할 수 있습니다.

뷰가 Replicated 또는 Shared 데이터베이스에 있고 다른 레플리카에서 갱신이 실행 중인 경우, 해당 갱신이 완료될 때까지 기다립니다.

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

현재 레플리카에서 지정된 뷰의 갱신이 진행 중이면 이를 중단하고 취소합니다. 그렇지 않으면 아무 작업도 하지 않습니다.

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

지정된 [S3Queue](../../engines/table-engines/integrations/s3queue.md) 또는 [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) 테이블에서 지정된 파일이 처리되거나 영구적으로 처리에 실패할 때까지 대기합니다. 파일이 이미 처리된 경우에는 즉시 반환합니다. 파일이 영구적으로 실패한 경우(모든 재시도 횟수를 소진한 경우) 오류가 발생합니다.

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```