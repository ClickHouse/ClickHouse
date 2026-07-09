---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: 'cache 딕셔너리 레이아웃'
sidebar_label: 'cache'
sidebar_position: 6
description: '딕셔너리를 고정 크기의 인메모리 cache에 저장합니다.'
doc_type: '참고'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`cached` 딕셔너리 레이아웃 유형은 고정된 개수의 셀을 가진 캐시에 딕셔너리를 저장합니다.
이 셀에는 자주 사용되는 요소가 들어 있습니다.

딕셔너리 키는 [UInt64](/ko/sql-reference/data-types/int-uint.md) 타입입니다.

딕셔너리를 조회할 때는 먼저 캐시를 조회합니다. 각 데이터 블록(block of data)마다, 캐시에서 찾지 못했거나 오래된 키는 모두 `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`를 사용해 소스에 요청합니다. 이후 받은 데이터를 캐시에 기록합니다.

딕셔너리에서 키를 찾지 못하면 캐시 업데이트 작업이 생성되어 업데이트 큐에 추가됩니다. 업데이트 큐 속성은 `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates` 설정으로 제어할 수 있습니다.

캐시 딕셔너리의 경우 캐시에 있는 데이터의 만료 [lifetime](../lifetime.md)을 설정할 수 있습니다. 셀에 데이터를 로드한 뒤 `lifetime`보다 긴 시간이 지나면 해당 셀의 값은 사용되지 않으며 키는 만료된 상태가 됩니다. 이 키는 다음에 필요할 때 다시 요청됩니다. 이 동작은 `allow_read_expired_keys` 설정으로 구성할 수 있습니다.

이 방식은 딕셔너리를 저장하는 모든 방법 중 가장 효율이 낮습니다. 캐시의 속도는 올바른 설정과 사용 시나리오에 크게 좌우됩니다. 캐시 유형 딕셔너리는 적중률이 충분히 높을 때만 좋은 성능을 냅니다(권장값은 99% 이상). 평균 적중률은 [system.dictionaries](/ko/operations/system-tables/dictionaries.md) 테이블에서 확인할 수 있습니다.

`allow_read_expired_keys` 설정이 1로 지정되면(기본값은 0) 딕셔너리는 비동기 업데이트를 지원할 수 있습니다. 클라이언트가 키를 요청했을 때 해당 키가 모두 캐시에 있지만 일부가 만료된 상태라면, 딕셔너리는 클라이언트에 만료된 키를 반환하고 소스에 비동기로 요청합니다.

캐시 성능을 개선하려면 `LIMIT`가 포함된 서브쿼리(subquery)를 사용하고, 딕셔너리를 외부에서 함수로 호출하십시오.

모든 유형의 소스가 지원됩니다.

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
        <cache>
            <!-- 셀 수 기준 캐시 크기입니다. 2의 거듭제곱으로 올림됩니다. -->
            <size_in_cells>1000000000</size_in_cells>
            <!-- 만료된 키 읽기를 허용합니다. -->
            <allow_read_expired_keys>0</allow_read_expired_keys>
            <!-- 업데이트 큐의 최대 크기입니다. -->
            <max_update_queue_size>100000</max_update_queue_size>
            <!-- 업데이트 작업을 큐에 추가할 때의 최대 timeout(밀리초)입니다. -->
            <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
            <!-- 업데이트 작업 완료를 기다리는 최대 timeout(밀리초)입니다. -->
            <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
            <!-- 캐시 딕셔너리 업데이트에 사용할 최대 스레드 수입니다. -->
            <max_threads_for_updates>4</max_threads_for_updates>
        </cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

충분히 큰 캐시 크기를 설정하십시오. 적절한 셀 수를 정하려면 실험이 필요합니다:

1. 값을 하나 설정합니다.
2. 캐시가 완전히 찰 때까지 쿼리를 실행합니다.
3. `system.dictionaries` 테이블을 사용해 메모리 사용량을 평가합니다.
4. 필요한 메모리 사용량에 도달할 때까지 셀 수를 늘리거나 줄입니다.

:::note
이 레이아웃의 소스로는 ClickHouse를 권장하지 않습니다. 딕셔너리 조회에는 임의의 Point 읽기가 필요하며, 이는 ClickHouse가 최적화한 access pattern이 아닙니다.
:::