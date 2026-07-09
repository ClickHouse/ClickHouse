---
slug: /sql-reference/statements/create/dictionary/sources/redis
title: 'Redis 딕셔너리 소스'
sidebar_position: 10
sidebar_label: 'Redis'
description: 'ClickHouse에서 Redis를 딕셔너리 소스로 구성합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(REDIS(
        host 'localhost'
        port 6379
        storage_type 'simple'
        db_index 0
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
        <redis>
            <host>localhost</host>
            <port>6379</port>
            <storage_type>simple</storage_type>
            <db_index>0</db_index>
        </redis>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

설정 필드:

| Setting        | Description                                                                                                                                                                                                                                                                                      |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`         | Redis 호스트입니다.                                                                                                                                                                                                                                                                                    |
| `port`         | Redis server의 포트입니다.                                                                                                                                                                                                                                                                             |
| `storage_type` | 키 작업에 사용되는 Redis 내부 저장소의 구조입니다. `simple`은 평면 키-값 맵을 사용하며, 단순 키 레이아웃과 단일 컬럼 복합 키 레이아웃(예: `complex_key_cache`, `complex_key_direct`)을 지원합니다. `hash_map`은 Redis 해시를 사용하며, 복합 complex key에 필요합니다. 이 경우 키 컬럼은 정확히 2개여야 합니다. 키 컬럼은 정수형 또는 문자열형이어야 합니다. 범위 레이아웃은 지원되지 않습니다. 기본값은 `simple`입니다. 선택 사항입니다. |
| `db_index`     | Redis 논리 데이터베이스의 숫자 인덱스입니다. 기본값은 `0`입니다. 선택 사항입니다.                                                                                                                                                                                                                                               |