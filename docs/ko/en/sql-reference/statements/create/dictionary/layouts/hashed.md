---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'hashed 딕셔너리 레이아웃 유형'
sidebar_label: 'hashed'
sidebar_position: 3
description: '해시 테이블을 사용하여 딕셔너리를 메모리에 저장합니다: hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: '참고'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

딕셔너리는 해시 테이블 형태로 메모리에 전체가 저장됩니다. 딕셔너리에는 임의의 식별자를 사용하는 요소를 얼마든지 포함할 수 있습니다. 실제로는 키 수가 수천만 개에 이를 수 있습니다.

딕셔너리 키는 [UInt64](/ko/sql-reference/data-types/int-uint.md) 타입입니다.

모든 소스 유형이 지원됩니다. 업데이트 시에는 데이터(파일 또는 테이블의 데이터)를 전체 읽어 들입니다.

구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

설정이 포함된 구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <hashed>
        <!-- shards가 1보다 크면(기본값은 `1`) 딕셔너리가
             데이터를 병렬로 로드합니다. 하나의
             딕셔너리에 요소가 매우 많은 경우 유용합니다. -->
        <shards>10</shards>

        <!-- 병렬 큐에서 block 백로그의 크기입니다.

             병렬 로딩의 병목 지점은 rehash이므로,
             thread가 rehash를 수행하느라 처리가 멈추지 않도록
             어느 정도의 백로그가 필요합니다.

             10000은 메모리와 속도 사이에서 좋은 균형입니다.
             10e10개의 요소가 있는 경우에도 처리 정체 없이 모든 부하를 감당할 수 있습니다. -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- 해시 테이블의 최대 load factor입니다. 값이 클수록 메모리를
             더 효율적으로 활용하지만(낭비되는 메모리가 줄어듦), 읽기 성능은
             저하될 수 있습니다.

             유효한 값: [0.5, 0.99]
             기본값: 0.5 -->
        <max_load_factor>0.5</max_load_factor>
      </hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="sparse_hashed">
  ## sparse_hashed
</div>

`hashed`와 유사하지만, 메모리 사용량을 줄이는 대신 CPU 사용량이 더 많습니다.

딕셔너리 키는 [UInt64](/ko/sql-reference/data-types/int-uint.md) 타입입니다.

구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

이 유형의 딕셔너리에서도 `shards`를 사용할 수 있으며, `sparse_hashed`는 더 느리므로 `hashed`보다 `sparse_hashed`에서 그 중요성이 더 큽니다.

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

이 저장 방식은 복합 [키](../attributes.md#composite-key)에 사용됩니다. `hashed`와 유사합니다.

구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <complex_key_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_sparse_hashed">
  ## complex_key_sparse_hashed
</div>

이 저장 방식은 복합 [키](../attributes.md#composite-key)에 사용됩니다. [sparse&#95;hashed](#sparse_hashed)와 유사합니다.

구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <complex_key_sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />