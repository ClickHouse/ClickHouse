---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'hashed_array 딕셔너리 레이아웃 유형'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: '해시 테이블과 속성 배열을 사용해 딕셔너리를 메모리에 저장합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed_array">
  ## hashed_array
</div>

딕셔너리 전체가 메모리에 저장됩니다. 각 속성은 배열에 저장됩니다. 키 속성은 값이 속성 배열의 인덱스가 되는 해시 테이블 형태로 저장됩니다. 딕셔너리에는 임의의 식별자를 사용하는 요소를 원하는 개수만큼 포함할 수 있습니다. 실제로 키 수는 수천만 개에 이를 수 있습니다.

딕셔너리 키는 [UInt64](/ko/sql-reference/data-types/int-uint.md) 타입입니다.

모든 소스 유형이 지원됩니다. 업데이트 시에는 데이터(파일 또는 테이블의 데이터)를 전체 읽어옵니다.

구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <hashed_array>
      </hashed_array>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_hashed_array">
  ## complex_key_hashed_array
</div>

이 저장소 유형은 복합 [키](../attributes.md#composite-key)와 함께 사용됩니다. [hashed&#95;array](#hashed_array)와 유사합니다.

구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <complex_key_hashed_array />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />