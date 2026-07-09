---
description: '하드웨어 리소스의 사용량과 ClickHouse 서버 메트릭을 모니터링할 수
  있습니다.'
keywords: ['모니터링', '관측성', '고급 대시보드', '대시보드', '관측성 대시보드']
sidebar_label: '모니터링'
sidebar_position: 45
slug: /operations/monitoring
title: '모니터링'
doc_type: '참고'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # 모니터링
</div>

:::note
이 가이드에서 설명하는 모니터링 데이터는 ClickHouse Cloud에서 확인할 수 있습니다. 아래에서 설명하는 기본 제공 대시보드에 표시될 뿐만 아니라, 기본 및 고급 성능 메트릭은 기본 서비스 콘솔에서도 직접 확인할 수 있습니다.
:::

다음 항목을 모니터링할 수 있습니다.

* 하드웨어 리소스 사용률
* ClickHouse 서버 메트릭

<div id="built-in-advanced-observability-dashboard">
  ## 기본 제공 고급 관측성 대시보드
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="스크린샷 2023-11-12 6 08 58 PM" size="md" />

ClickHouse는 `$HOST:$PORT/dashboard`에서 접근할 수 있는 기본 제공 고급 관측성 대시보드 기능을 제공하며(사용자 이름과 비밀번호 필요), 다음 메트릭을 표시합니다:

* 초당 쿼리 수
* CPU 사용량 (코어)
* 실행 중인 쿼리
* 실행 중인 머지
* 초당 선택 바이트 수
* IO 대기
* CPU 대기
* OS CPU 사용량 (사용자 공간)
* OS CPU 사용량 (커널)
* 디스크에서 읽기
* 파일 시스템에서 읽기
* 메모리 (추적)
* 초당 삽입된 행 수
* 전체 MergeTree 파트 수
* 파티션별 최대 파트 수

<div id="resource-utilization">
  ## 리소스 사용률
</div>

ClickHouse는 다음과 같은 하드웨어 리소스의 상태도 자체적으로 모니터링합니다.

* 프로세서의 부하와 온도
* 스토리지 시스템, RAM 및 네트워크 사용률

이 데이터는 `system.asynchronous_metric_log` 테이블에 수집됩니다.

<div id="clickhouse-server-metrics">
  ## ClickHouse 서버 메트릭
</div>

ClickHouse 서버에는 자체 상태를 모니터링하기 위한 내장 계측 기능이 있습니다.

서버 이벤트를 추적하려면 서버 로그를 사용하십시오. 설정 파일의 [logger](../operations/server-configuration-parameters/settings.md#logger) 섹션을 참조하십시오.

ClickHouse는 다음을 수집합니다:

* 서버의 연산 리소스 사용 방식에 대한 다양한 메트릭
* 쿼리 처리에 대한 일반적인 통계

메트릭은 [system.metrics](/ko/operations/system-tables/metrics), [system.events](/ko/operations/system-tables/events), [system.asynchronous&#95;metrics](/ko/operations/system-tables/asynchronous_metrics) 테이블에서 확인할 수 있습니다.

ClickHouse를 구성하여 메트릭을 [Graphite](https://github.com/graphite-project)로 내보낼 수 있습니다. ClickHouse 서버 설정 파일의 [Graphite 섹션](../operations/server-configuration-parameters/settings.md#graphite)을 참조하십시오. 메트릭 내보내기를 구성하기 전에 공식 [가이드](https://graphite.readthedocs.io/en/latest/install.html)에 따라 Graphite를 설정해야 합니다.

ClickHouse를 구성하여 메트릭을 [Prometheus](https://prometheus.io)로 내보낼 수 있습니다. ClickHouse 서버 설정 파일의 [Prometheus 섹션](../operations/server-configuration-parameters/settings.md#prometheus)을 참조하십시오. 메트릭 내보내기를 구성하기 전에 공식 [가이드](https://prometheus.io/docs/prometheus/latest/installation/)에 따라 Prometheus를 설정해야 합니다.

또한 HTTP API를 통해 서버 가용성을 모니터링할 수 있습니다. `/ping`에 `HTTP GET` 요청을 보내십시오. 서버가 사용 가능하면 `200 OK`로 응답합니다.

클러스터 구성에서 서버를 모니터링하려면 [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) 매개변수를 설정하고 HTTP 리소스 `/replicas_status`를 사용해야 합니다. `/replicas_status`에 대한 요청은 레플리카를 사용할 수 있고 다른 레플리카보다 뒤처지지 않은 경우 `200 OK`를 반환합니다. 레플리카가 지연된 경우 뒤처진 정도에 대한 정보와 함께 `503 HTTP_SERVICE_UNAVAILABLE`를 반환합니다.