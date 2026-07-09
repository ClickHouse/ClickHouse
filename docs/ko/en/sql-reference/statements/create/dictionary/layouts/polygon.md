---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: '폴리곤 딕셔너리'
sidebar_label: '폴리곤'
sidebar_position: 12
description: '포인트-인-폴리곤 조회를 위한 폴리곤 딕셔너리를 구성합니다.'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`polygon` (`POLYGON`) 딕셔너리는 점이 다각형 내부에 포함되는지 확인하는 쿼리, 즉 사실상 &quot;역 지오코딩&quot; 조회에 최적화되어 있습니다.
좌표(위도/경도)가 주어지면, 여러 다각형 집합(예: 국가 또는 지역 경계) 중에서 해당 점을 포함하는 다각형/영역을 효율적으로 찾습니다.
위치 좌표를 해당 좌표가 속한 영역에 매핑하는 용도에 적합합니다.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="ClickHouse의 Polygon Dictionaries" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

폴리곤 딕셔너리 구성 예시:

<CloudDetails />

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY polygon_dict_name (
        key Array(Array(Array(Array(Float64)))),
        name String,
        value UInt64
    )
    PRIMARY KEY key
    LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <dictionary>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>Array(Array(Array(Array(Float64))))</type>
                </attribute>
            </key>

            <attribute>
                <name>name</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>

            <attribute>
                <name>value</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>

        <layout>
            <polygon>
                <store_polygon_key_column>1</store_polygon_key_column>
            </polygon>
        </layout>

        ...
    </dictionary>
    ```
  </TabItem>
</Tabs>

<br />

폴리곤 딕셔너리를 구성할 때 키는 다음 두 가지 타입 중 하나여야 합니다:

* 단순 다각형입니다. 점의 배열입니다.
* MultiPolygon입니다. 다각형의 배열입니다. 각 다각형은 점의 2차원 배열입니다. 이 배열의 첫 번째 요소는 다각형의 외곽 경계이며, 그다음 요소들은 제외할 영역을 지정합니다.

점은 좌표의 배열 또는 튜플로 지정할 수 있습니다. 현재 구현에서는 2차원 점만 지원됩니다.

ClickHouse가 지원하는 모든 포맷으로 자체 데이터를 업로드할 수 있습니다.

사용 가능한 [인메모리 저장소](./#storing-dictionaries-in-memory)는 3가지입니다:

| Layout               | Description                                                                                                                                                                                |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `POLYGON_SIMPLE`     | 단순 구현입니다. 각 쿼리마다 모든 다각형을 선형으로 순회하면서, 추가 인덱스 없이 포함 여부를 확인합니다.                                                                                                                               |
| `POLYGON_INDEX_EACH` | 각 다각형마다 별도의 인덱스를 구축하여 대부분의 경우 포함 여부를 빠르게 확인할 수 있습니다(지리적 영역에 최적화됨). 영역 위에 그리드를 덮고, 셀을 재귀적으로 16개의 동일한 부분으로 나눕니다. 재귀 깊이가 `MAX_DEPTH`에 도달하거나 셀이 `MIN_INTERSECTIONS`개 이하의 다각형과만 교차하면 분할이 중지됩니다. |
| `POLYGON_INDEX_CELL` | 위에서 설명한 것과 동일한 옵션으로 그리드를 생성합니다. 각 리프 셀에 대해 그 안에 속하는 모든 다각형 조각에 인덱스를 구축하여 빠른 쿼리 응답이 가능하도록 합니다.                                                                                              |
| `POLYGON`            | `POLYGON_INDEX_CELL`의 동의어입니다.                                                                                                                                                              |

딕셔너리 쿼리는 딕셔너리를 다루는 표준 [함수](/ko/sql-reference/functions/ext-dict-functions.md)를 사용해 수행합니다.
중요한 차이점은 여기서의 키가, 이를 포함하는 다각형을 찾으려는 점이라는 것입니다.

**예시**

위에서 정의한 딕셔너리를 사용하는 예시:

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

`points` 테이블의 각 Point에 대해 마지막 명령을 실행하면, 해당 Point를 포함하는 최소 면적 다각형이 찾아지고 요청한 속성이 출력됩니다.

**예시**

SELECT 쿼리로 폴리곤 딕셔너리의 컬럼을 조회할 수 있습니다. 딕셔너리 구성 또는 해당 DDL 쿼리에서 `store_polygon_key_column = 1`을 설정하면 됩니다.

```sql title="Query"
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

```text title="Response"
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```