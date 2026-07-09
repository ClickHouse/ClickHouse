---
description: '`QueryRunner` 테이블에 삽입된 레코드는 엔진이 로컬 또는 원격 클러스터에서 "fire and forget" 모드로 실행하는 쿼리를 나타냅니다.'
sidebar_label: 'QueryRunner'
sidebar_position: 55
slug: /engines/table-engines/special/query-runner
title: 'QueryRunner 테이블 엔진'
doc_type: 'reference'
---

<div id="queryrunner-table-engine">
  # QueryRunner 테이블 엔진
</div>

`QueryRunner` 테이블에 삽입된 레코드는 엔진이 실행할 쿼리를 나타냅니다.
이 엔진은 비동기 쿼리 실행, 생성된 쿼리의 일괄 실행,
원격 클러스터로의 쿼리 전달, 벤치마크, 퍼징, 섀도 트래픽을 사용한 테스트에 사용할 수 있습니다.

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE runner
(
    query String,
    database String,
    settings Map(LowCardinality(String), String)
)
ENGINE = QueryRunner
SETTINGS
    cluster = 'cluster_name',
    shard = '1',
    mode = 'asynchronous',
    threads = 4,
    max_queue_size = 1000
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }];
```

테이블은 허용된 컬럼인 `query`, `database`, `settings` 중 일부만 포함하여 생성해야 합니다.
`query` 컬럼은 필수이며, 나머지 컬럼은 선택 사항입니다.

| 컬럼         | 유형                    | 의미                                             |
| ---------- | --------------------- | ---------------------------------------------- |
| `query`    | `String`              | 실행할 쿼리입니다.                                     |
| `database` | `String`              | 쿼리의 기본 데이터베이스입니다. 비어 있으면 서버의 기본 데이터베이스가 사용됩니다. |
| `settings` | `Map(String, String)` | 쿼리에 적용되는 설정입니다.                                |

<div id="engine-settings">
  ## 엔진 설정
</div>

| 설정               | 기본값              | 의미                                                                                                                                       |
| ---------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`        | `''`             | 쿼리를 보낼 클러스터의 이름입니다. 비어 있으면 쿼리는 로컬에서 실행됩니다.                                                                                            |
| `shard`          | `'1'`            | 쿼리를 보낼 클러스터 세그먼트의 1-based 인덱스이거나, 쿼리마다 임의의 세그먼트를 선택하려면 `'random'`, 각 쿼리를 모든 세그먼트에서 실행하려면 `'all'`입니다. 이 설정을 사용하려면 `cluster` 설정이 필요합니다. |
| `mode`           | `'asynchronous'` | `synchronous` 모드에서는 삽입된 batch의 모든 쿼리 실행이 끝난 후 INSERT가 반환됩니다. `asynchronous` 모드에서는 쿼리가 큐에 들어가면 바로 INSERT가 반환됩니다.                          |
| `threads`        | `4`              | 쿼리를 실행하는 백그라운드 스레드 수입니다.                                                                                                                 |
| `max_queue_size` | `1000`           | 큐에 대기할 수 있는 최대 쿼리 수입니다. 큐가 가득 차면 새로 삽입된 쿼리는 버려지고 오류가 로그에 기록됩니다.                                                                          |

<div id="details">
  ## 세부 정보
</div>

이 테이블은 `INSERT` 쿼리만 허용합니다.
쿼리는 &quot;fire and forget&quot; 모드로 실행됩니다. 예외가 발생해도 재시도는 수행되지 않으며,
`SELECT` 쿼리의 결과는 버려집니다(결과를 유지하는 유일한 방법은 `INSERT SELECT`뿐입니다).
각 쿼리의 성공 여부는 `system.query_log` 테이블에서 확인할 수 있으며, 이 엔진이 시작한
쿼리에는 시작 서버에서 `is_internal = 1`이 표시됩니다.

큐에 들어간 쿼리는 메모리에 유지되며 서버를 재시작하면 보존되지 않습니다. 서버 종료 시
(또는 테이블에 `DROP`/`DETACH`를 수행할 때) 아직 시작되지 않은 쿼리는 버려집니다. 이미
실행 중인 쿼리 가운데 클러스터로 전달된 쿼리는 취소되고, 로컬에서 실행 중인 쿼리는 완료될 때까지
대기합니다.

실행할 쿼리 자체가 `INSERT`인 경우, 해당 데이터는 인라인이어야 합니다. 즉 `INSERT ... VALUES (...)`,
`INSERT ... SELECT ...`, 또는 쿼리 텍스트에 데이터가 포함된 `INSERT ... FORMAT ...` 형식이어야 합니다. 별도
스트림에서 데이터를 받는 `INSERT`는 지원되지 않습니다.

<div id="local-mode-and-sql-security">
  ## 로컬 모드 및 SQL SECURITY
</div>

`cluster` 설정이 없으면 쿼리는 로컬 server에서 실행됩니다.
어떤 사용자 권한으로 실행될지는 `SQL SECURITY` 절에 따라 결정됩니다.

* `INVOKER` (기본값): 쿼리는 `INSERT`를 수행한 사용자의 권한으로 실행됩니다.
* `DEFINER`: 쿼리는 지정된 `DEFINER` 사용자의 권한으로 실행됩니다. 삽입되는 쿼리는 임의의 내용일 수 있으므로, 이러한 table에 `INSERT` 권한을 부여하면 정의자의 모든 권한을 위임하는 셈이 됩니다.
* `NONE`: 쿼리는 사용자 없이 전체 접근 권한으로 실행됩니다. 테이블 생성 시 `ALLOW_SQL_SECURITY_NONE` 권한이 필요합니다.

<div id="cluster-mode">
  ## 클러스터 모드
</div>

`cluster` 설정을 지정하면 쿼리가 지정된 클러스터로 전송됩니다.

대상 세그먼트는 `shard`로 선택합니다. 즉, 고정된 1부터 시작하는 인덱스(기본값은 `'1'`), 각 쿼리마다 임의의 세그먼트를 선택하는 `'random'`, 또는 클러스터의 모든 세그먼트에서 각 쿼리를 실행하는 `'all'`을 사용할 수 있습니다. 세그먼트 내 레플리카는 서버의 `load_balancing` 설정에 따라 선택됩니다.

`database` 컬럼은 원격 서버에 대한 연결의 기본 데이터베이스를 설정합니다. 기본 데이터베이스는 연결당 한 번만 설정되므로, 서로 다른 각 `database` 값은 자체 연결 풀을 사용합니다. 이 연결 풀은 처음 사용할 때 생성되며 테이블의 수명 주기 동안 재사용됩니다.

`DEFINER`와 `SQL SECURITY`는 로컬 모드에서만 영향을 미치며, 이를 `cluster` 설정과 함께 사용하면 오류가 발생합니다. 원격 서버에서는 쿼리가 클러스터 구성의 자격 증명으로 인증되며 일반 초기 쿼리로 실행됩니다. 이 쿼리는 `system.query_log`에 `is_initial_query = 1` 및 자체 `query_id`와 함께 기록됩니다(이를 생성한 INSERT와 연결되지는 않음). 쿼리를 시작한 서버에서는 전달된 쿼리가 `system.query_log`에 `is_internal = 1`로 기록됩니다.

엔진은 쿼리 결과를 버리므로, 항상 전달된 쿼리를 `discard_query_data = 1`로 실행합니다. 따라서 SELECT 쿼리의 결과 데이터는 네트워크를 통해 전송되지 않습니다(이 동작은 `settings` 컬럼에 설정된 `discard_query_data` 값을 재정의합니다).

<div id="waiting-for-queries-to-finish">
  ## 쿼리 완료까지 대기
</div>

비동기 모드에서는 해당 테이블에 지금까지 제출된 모든 쿼리가 완료될 때까지 기다리기 위해 다음 쿼리를 사용할 수 있습니다:

```sql
SYSTEM WAIT QUERY RUNNER runner;
```

<div id="example">
  ## 예시
</div>

query log에 기록된 최근 `SELECT` 쿼리를 다시 실행합니다:

```sql
INSERT INTO runner (query, database, settings)
SELECT query, current_database, Settings
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND NOT is_internal AND query_kind = 'Select'
  AND event_time > now() - INTERVAL 1 HOUR;
```