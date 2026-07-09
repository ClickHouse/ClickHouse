---
description: 'ClickHouse에서 OpenTelemetry를 사용해 분산 추적 및 메트릭을 수집하는 가이드'
sidebar_label: 'OpenTelemetry로 ClickHouse 추적'
sidebar_position: 62
slug: /operations/opentelemetry
title: 'OpenTelemetry로 ClickHouse 추적'
doc_type: 'guide'
---

[OpenTelemetry](https://opentelemetry.io/)는 분산 애플리케이션의 트레이스와 메트릭을 수집하기 위한 개방형 표준입니다. ClickHouse는 OpenTelemetry를 일부 지원합니다.

<div id="supplying-trace-context-to-clickhouse">
  ## ClickHouse에 추적 컨텍스트 제공하기
</div>

ClickHouse는 [W3C 권고안](https://www.w3.org/TR/trace-context/)에 설명된 추적 컨텍스트 HTTP 헤더를 지원합니다. 또한 ClickHouse 서버 간 또는 클라이언트와 서버 간 통신에 사용되는 네이티브 프로토콜을 통한 추적 컨텍스트도 지원합니다. 수동 테스트 시에는 Trace Context 권고안을 준수하는 추적 컨텍스트 헤더를 `--opentelemetry-traceparent` 및 `--opentelemetry-tracestate` 플래그를 사용해 `clickhouse-client`에 전달할 수 있습니다.

부모 추적 컨텍스트가 제공되지 않거나 제공된 추적 컨텍스트가 위 W3C 표준을 준수하지 않는 경우, ClickHouse는 [opentelemetry&#95;start&#95;trace&#95;probability](/ko/operations/settings/settings#opentelemetry_start_trace_probability) 설정으로 제어되는 확률에 따라 새 추적을 시작할 수 있습니다.

<div id="propagating-the-trace-context">
  ## 추적 컨텍스트 전파
</div>

추적 컨텍스트는 다음과 같은 경우 다운스트림 서비스로 전파됩니다.

* [분산](../engines/table-engines/special/distributed.md) 테이블 엔진을 사용하는 경우처럼 원격 ClickHouse 서버에 보내는 쿼리

* [url](../sql-reference/table-functions/url.md) 테이블 함수. 추적 컨텍스트 정보는 HTTP 헤더로 전송됩니다.

<div id="tracing-clickhouse-keeper-requests">
  ## ClickHouse Keeper 요청 추적
</div>

ClickHouse는 [ClickHouse Keeper](../guides/sre/keeper/index.md) 요청에 대한 OpenTelemetry 추적을 지원합니다(ZooKeeper와 호환되는 조정 서비스). 이 기능을 사용하면 클라이언트가 요청을 제출하는 시점부터 서버 측에서 처리되는 단계까지 Keeper 작업의 전체 수명 주기를 자세히 확인할 수 있습니다.

<div id="enabling-keeper-tracing">
  ### Keeper 추적 활성화
</div>

Keeper 요청에 대한 추적을 활성화하려면 ZooKeeper/Keeper 클라이언트 구성에서 다음 설정을 지정합니다:

```xml
<clickhouse>
    <zookeeper>
        <node>
            <host>keeper1</host>
            <port>9181</port>
        </node>
        <!-- Enable OpenTelemetry tracing context propagation -->
        <pass_opentelemetry_tracing_context>true</pass_opentelemetry_tracing_context>
    </zookeeper>
</clickhouse>
```

<div id="keeper-span-types">
  ### Keeper 스팬 유형
</div>

추적이 활성화되면 ClickHouse는 클라이언트 측과 서버 측 Keeper 작업 모두에 대해 스팬을 생성합니다.

**클라이언트 측 스팬:**

* `zookeeper.create` — 새 노드 생성
* `zookeeper.get` — 노드 데이터 가져오기
* `zookeeper.set` — 노드 데이터 설정
* `zookeeper.remove` — 노드 제거
* `zookeeper.list` — 하위 노드 나열
* `zookeeper.exists` — 노드 존재 여부 확인
* `zookeeper.multi` — 여러 작업을 원자적으로 실행
* `zookeeper.client.requests_queue` — 전송 전 요청이 큐에서 대기한 시간

**서버 측 스팬 (Keeper):**

* `keeper.receive_request` — 클라이언트의 요청을 수신하고 구문 분석하는 작업
* `keeper.dispatcher.requests_queue` — 디스패처에서 요청이 큐에 대기하는 시간
* `keeper.write.pre_commit` — Raft commit 전에 쓰기 요청을 전처리하는 작업
* `keeper.write.commit` — Raft commit 후 쓰기 요청을 처리하는 작업
* `keeper.read.wait_for_write` — 종속된 쓰기 작업을 기다리는 읽기 요청
* `keeper.read.process` — 읽기 요청을 처리하는 작업
* `keeper.dispatcher.responses_queue` — 디스패처에서 응답이 큐에 대기하는 시간
* `keeper.send_response` — 클라이언트에 응답을 전송하는 작업

<div id="sampling-and-performance">
  ### 샘플링 및 성능
</div>

추적 오버헤드를 관리하기 위해 Keeper는 동적 샘플링을 구현합니다. 샘플링 비율은 요청 크기에 따라 1/10,000에서 1/10 사이로 자동 조정됩니다. 샘플링 여부와 관계없이 모든 요청의 소요 시간은 성능 모니터링을 위해 히스토그램 메트릭에 기록됩니다.

<div id="tracing-the-clickhouse-itself">
  ## ClickHouse 자체 추적하기
</div>

ClickHouse는 각 쿼리와 쿼리 계획 또는 분산 쿼리 같은 일부 쿼리 실행 단계에 대해 `trace spans`를 생성합니다.

이 추적 정보를 유용하게 활용하려면 [Jaeger](https://jaegertracing.io/) 또는 [Prometheus](https://prometheus.io/)처럼 OpenTelemetry를 지원하는 모니터링 시스템으로 내보내야 합니다. ClickHouse는 특정 모니터링 시스템에 대한 의존성을 두지 않기 위해 추적 데이터를 시스템 테이블을 통해서만 제공합니다. 표준에서 [요구하는](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span) OpenTelemetry trace 스팬 정보는 [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md) 테이블에 저장됩니다.

이 테이블은 서버 구성에서 활성화되어 있어야 합니다. 기본 config file `config.xml`의 `opentelemetry_span_log` 요소를 참조하십시오. 기본적으로 활성화되어 있습니다.

태그 또는 속성은 키와 값을 담은 2개의 병렬 배열로 저장됩니다. 이를 다루려면 [ARRAY JOIN](../sql-reference/statements/select/array-join.md)을 사용하십시오.

<div id="log-query-settings">
  ## 로그 쿼리 설정
</div>

설정 [log&#95;query&#95;settings](settings/settings.md)을 사용하면 쿼리 실행 중 쿼리 설정 변경 사항을 기록할 수 있습니다. 이 기능을 활성화하면 쿼리 설정에 적용된 모든 변경 사항이 OpenTelemetry 스팬 로그에 기록됩니다. 이 기능은 특히 운영 환경에서 쿼리 성능에 영향을 줄 수 있는 설정 변경 사항을 추적하는 데 유용합니다.

<div id="integration-with-monitoring-systems">
  ## 모니터링 시스템과의 통합
</div>

현재 ClickHouse에서 모니터링 시스템으로 tracing data를 내보낼 수 있는 즉시 사용 가능한 도구는 없습니다.

테스트 목적으로는 [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md) 테이블에 대해 [URL](../engines/table-engines/special/url.md) 엔진을 사용하는 materialized view를 구성하여 내보내기를 설정할 수 있습니다. 이렇게 하면 유입되는 로그 데이터를 trace collector의 HTTP endpoint로 전송할 수 있습니다. 예를 들어, `http://localhost:9411`에서 실행 중인 Zipkin instance로 최소한의 스팬 데이터를 Zipkin v2 JSON 포맷으로 전송하려면 다음과 같이 합니다.

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    CASE WHEN parent_span_id = 0 THEN '' ELSE lower(hex(parent_span_id)) END AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

오류가 발생하면 해당 로그 데이터의 오류가 발생한 부분은 별도 알림 없이 유실됩니다. 데이터가 수신되지 않으면 서버 로그에서 오류 메시지를 확인하십시오.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse로 관측성 솔루션 구축하기 - Part 2 - 트레이스](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)