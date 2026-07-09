---
description: '메모리에 딕셔너리를 저장하는 딕셔너리 레이아웃 유형'
sidebar_label: '개요'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: '딕셔너리 레이아웃'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## 딕셔너리 레이아웃 유형
</div>

딕셔너리를 메모리에 저장하는 방식은 여러 가지가 있으며, 각각 CPU와 RAM 사용량 사이에 절충이 있습니다.

| Layout                                                                                                     | Description                                                                             |
| ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| [flat](./flat.md)                                                                                          | 키로 인덱싱된 평면 배열에 데이터를 저장합니다. 가장 빠른 레이아웃이지만, 키는 `UInt64`여야 하며 `max_array_size` 범위 내여야 합니다. |
| [hashed](./hashed.md)                                                                                      | 데이터를 해시 테이블에 저장합니다. 키 크기 제한이 없으며, 요소 수에도 제한이 없습니다.                                      |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | `hashed`와 유사하지만, 메모리 사용량을 줄이는 대신 CPU를 더 사용합니다.                                          |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | `hashed`와 유사하지만, 복합 키용입니다.                                                              |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | `sparse_hashed`와 유사하지만, 복합 키용입니다.                                                       |
| [hashed&#95;array](./hashed-array.md)                                                                      | 속성은 배열에 저장하고, 해시 테이블은 키를 배열 인덱스에 매핑합니다. 속성이 많은 경우 메모리 효율적입니다.                           |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | `hashed_array`와 유사하지만, 복합 키용입니다.                                                        |
| [range&#95;hashed](./range-hashed.md)                                                                      | 정렬된 범위를 사용하는 해시 테이블입니다. 키 + 날짜/시간 범위 기준 조회를 지원합니다.                                      |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | `range_hashed`와 유사하지만, 복합 키용입니다.                                                        |
| [cache](./cache.md)                                                                                        | 고정 크기의 인메모리 캐시입니다. 자주 액세스되는 키만 저장됩니다.                                                   |
| [complex&#95;key&#95;cache](/ko/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | `cache`와 유사하지만, 복합 키용입니다.                                                               |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | `cache`와 유사하지만, 데이터를 SSD에 저장하고 인메모리 인덱스를 사용합니다.                                         |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | `ssd_cache`와 유사하지만, 복합 키용입니다.                                                           |
| [direct](./direct.md)                                                                                      | 인메모리에 저장하지 않고, 각 요청마다 소스를 직접 조회합니다.                                                     |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | `direct`와 유사하지만, 복합 키용입니다.                                                              |
| [ip&#95;trie](./ip-trie.md)                                                                                | 빠른 IP prefix 조회(CIDR 기반)를 위한 trie 구조입니다.                                                |

:::tip 권장 레이아웃
[flat](./flat.md), [hashed](./hashed.md), [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)는 가장 우수한 쿼리 성능을 제공합니다.
캐싱 레이아웃은 성능이 저하될 수 있고 매개변수 튜닝이 어렵기 때문에 권장되지 않습니다. 자세한 내용은 [cache](./cache.md)를 참조하십시오.
:::

<div id="specify-dictionary-layout">
  ## 딕셔너리 레이아웃 지정
</div>

<CloudDetails />

딕셔너리 레이아웃은 `LAYOUT` 절(DDL) 또는 설정 파일 정의의 `layout` 설정으로 구성할 수 있습니다.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- 레이아웃 설정
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- 레이아웃 설정 -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

전체 DDL 구문은 [CREATE DICTIONARY](../overview.md)에서 확인할 수 있습니다.

레이아웃 이름에 `complex-key*`가 포함되지 않은 딕셔너리는 [UInt64](/ko/sql-reference/data-types/int-uint.md) 타입의 키를 사용하며, `complex-key*` 딕셔너리는 임의 타입으로 구성된 복합 키를 사용합니다.

**숫자 키 예시** (`key_column` 컬럼은 [UInt64](/ko/sql-reference/data-types/int-uint.md) 타입임):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**복합 키 예시** (키에 [String](/ko/sql-reference/data-types/string.md) 타입 요소가 1개 있음):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## 딕셔너리 성능 개선
</div>

딕셔너리 성능을 개선하는 방법은 여러 가지가 있습니다.

* 딕셔너리를 사용하는 함수는 `GROUP BY` 뒤에서 호출합니다.
* 추출할 속성을 injective로 표시합니다.
  서로 다른 키가 서로 다른 속성 값에 대응하면 해당 속성을 injective라고 합니다.
  따라서 `GROUP BY`에서 키로 속성 값을 가져오는 함수를 사용하면, 이 함수는 자동으로 `GROUP BY`에서 제외됩니다.

ClickHouse는 딕셔너리 관련 오류가 발생하면 예외를 발생시킵니다.
오류 예시는 다음과 같습니다.

* 접근 중인 딕셔너리를 로드할 수 없습니다.
* `cached` 딕셔너리를 쿼리하는 중 오류가 발생했습니다.

딕셔너리 목록과 각 딕셔너리의 상태는 [system.dictionaries](/ko/operations/system-tables/dictionaries.md) 테이블에서 확인할 수 있습니다.