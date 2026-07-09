---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'direct 딕셔너리 레이아웃'
sidebar_label: 'direct'
sidebar_position: 9
description: '캐싱 없이 원본 소스를 직접 조회하는 딕셔너리 레이아웃입니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

딕셔너리는 메모리에 저장되지 않으며, 요청 처리 시 소스에 직접 접근합니다.

딕셔너리 키의 타입은 [UInt64](/ko/sql-reference/data-types/int-uint.md)입니다.

로컬 파일을 제외한 모든 [소스](../sources/#dictionary-sources) 유형이 지원됩니다.

구성 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
      <direct />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_direct">
  ## complex_key_direct
</div>

이 저장소 유형은 복합 [키](../attributes.md#composite-key)에 사용됩니다. `direct`와 유사합니다.