---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'ssd_cache 딕셔너리 레이아웃 유형'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: '인메모리 인덱스를 사용해 SSD에 딕셔너리 데이터를 저장하는 유형: ssd_cache 또는 complex_key_ssd_cache'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="ssd_cache">
  ## ssd_cache
</div>

`cache`와 유사하지만, 데이터는 SSD에 저장하고 인덱스는 RAM에 저장합니다. 업데이트 큐와 관련된 모든 캐시 딕셔너리 설정도 SSD 캐시 딕셔너리에 적용할 수 있습니다.

딕셔너리 키 타입은 [UInt64](/ko/sql-reference/data-types/int-uint.md)입니다.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
        PATH '/var/lib/clickhouse/user_files/test_dict'))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
        <ssd_cache>
            <!-- 바이트 단위의 기본 읽기 블록 크기입니다. SSD의 페이지 크기와 같게 설정하는 것이 좋습니다. -->
            <block_size>4096</block_size>
            <!-- 바이트 단위의 최대 캐시 파일 크기입니다. -->
            <file_size>16777216</file_size>
            <!-- SSD에서 요소를 읽기 위한 바이트 단위의 RAM 버퍼 크기입니다. -->
            <read_buffer_size>131072</read_buffer_size>
            <!-- SSD로 플러시하기 전에 요소를 모아 두기 위한 바이트 단위의 RAM 버퍼 크기입니다. -->
            <write_buffer_size>1048576</write_buffer_size>
            <!-- 캐시 파일이 저장될 경로입니다. -->
            <path>/var/lib/clickhouse/user_files/test_dict</path>
        </ssd_cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_ssd_cache">
  ## complex_key_ssd_cache
</div>

이 저장소 유형은 복합 [키](../attributes.md#composite-key)에 사용됩니다. `ssd_cache`와 유사합니다.