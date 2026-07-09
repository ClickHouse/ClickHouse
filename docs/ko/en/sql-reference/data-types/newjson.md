---
description: 'JSON 데이터 작업을 네이티브로 지원하는 ClickHouse의 JSON 데이터 유형 문서'
keywords: ['json', '데이터 유형']
sidebar_label: 'JSON'
sidebar_position: 63
slug: /sql-reference/data-types/newjson
title: 'JSON 데이터 유형'
doc_type: '참고'
---

import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
  <CardSecondary badgeState="success" badgeText="" description="예시, 고급 기능, 그리고 JSON 유형 사용 시 고려 사항은 JSON 모범 사례 가이드에서 확인해 보세요." icon="book" infoText="자세히 보기" infoUrl="/docs/best-practices/use-json-where-appropriate" title="가이드를 찾고 계신가요?" />
</Link>

<br />

`JSON` 유형은 JavaScript Object Notation(JSON) 문서를 하나의 컬럼에 저장합니다.

:::note
ClickHouse Open-Source에서 JSON 데이터 유형은 버전 25.3부터 프로덕션 환경에서 사용할 수 있는 것으로 간주됩니다. 이전 버전에서는 이 유형을 프로덕션 환경에서 사용하지 않는 것이 좋습니다.
:::

`JSON` 유형의 컬럼을 선언하려면 다음 구문을 사용할 수 있습니다:

```sql
<column_name> JSON
(
    max_dynamic_paths=N,
    max_dynamic_types=M,
    some.path TypeName,
    SKIP path.to.skip,
    SKIP REGEXP 'paths_regexp'
)
```

위 구문에서 사용한 매개변수는 다음과 같이 정의됩니다.

| Parameter                   | Description                                                                                                                                                                                                                                                                                                 | Default Value |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `max_dynamic_paths`         | 선택적 매개변수입니다. 별도로 저장되는 단일 데이터 블록 전체에서 몇 개의 경로를 서브컬럼으로 따로 저장할 수 있는지를 나타냅니다(예: MergeTree 테이블의 단일 데이터 파트 전체). <br /><br />이 한도를 초과하면 나머지 모든 경로는 [shared data](#shared-data-structure)라는 단일 구조에 함께 저장됩니다.<br /><br />또한 이 매개변수를 변경하지 않고도 동적 경로 수의 한도를 조정하는 [방법](#controlling-the-number-of-dynamic-paths)도 있습니다. | `1024`        |
| `max_dynamic_types`         | `1`에서 `255` 사이의 선택적 매개변수입니다. `Dynamic` 유형의 단일 경로 컬럼 안에서, 별도로 저장되는 단일 데이터 블록 전체에 걸쳐 몇 개의 서로 다른 데이터 유형을 따로 저장할 수 있는지를 나타냅니다(예: MergeTree 테이블의 단일 데이터 파트 전체). <br /><br />이 한도를 초과하면 모든 새로운 유형은 `shared variant`라는 단일 구조에 함께 저장됩니다.                                                                            | `32`          |
| `some.path TypeName`        | JSON의 특정 경로에 대한 선택적 유형 힌트입니다. 이러한 경로는 항상 지정된 유형의 서브컬럼으로 저장됩니다.                                                                                                                                                                                                                                              |               |
| `SKIP path.to.skip`         | JSON 파싱 중 건너뛰어야 하는 특정 경로에 대한 선택적 힌트입니다. 이러한 경로는 JSON 컬럼에 저장되지 않습니다. 지정한 경로가 중첩된 JSON 객체이면 해당 중첩 객체 전체를 건너뜁니다.                                                                                                                                                                                               |               |
| `SKIP REGEXP 'path_regexp'` | JSON 파싱 중 경로를 건너뛰는 데 사용하는 정규식 기반의 선택적 힌트입니다. 이 정규식과 일치하는 모든 경로는 JSON 컬럼에 저장되지 않습니다.                                                                                                                                                                                                                         |               |

<WhenToUseJson />

<div id="creating-json">
  ## `JSON` 생성하기
</div>

이 섹션에서는 `JSON`을 생성하는 여러 가지 방법을 살펴보겠습니다.

<div id="using-json-in-a-table-column-definition">
  ### 테이블 컬럼 정의에 `JSON` 사용하기
</div>

```sql title="Query (Example 1)"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 1)"
┌─json────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"]}          │
│ {"f":"Hello, World!"}                       │
│ {"a":{"b":"43","e":"10"},"c":["4","5","6"]} │
└─────────────────────────────────────────────┘
```

```sql title="Query (Example 2)"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 2)"
┌─json──────────────────────────────┐
│ {"a":{"b":42},"c":["1","2","3"]}  │
│ {"a":{"b":0},"f":"Hello, World!"} │
│ {"a":{"b":43},"c":["4","5","6"]}  │
└───────────────────────────────────┘
```

<div id="using-cast-with-json">
  ### CAST와 `::JSON` 함께 사용하기
</div>

특수 구문 `::JSON`을 사용하면 다양한 유형을 CAST할 수 있습니다.

<div id="cast-from-string-to-json">
  #### CAST를 사용해 `String`을 `JSON`으로 변환
</div>

```sql title="Query"
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-tuple-to-json">
  #### `Tuple`을 `JSON`으로 CAST
</div>

```sql title="Query"
SET enable_named_columns_in_function_tuple = 1;
SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-map-to-json">
  #### `Map`을 `JSON`으로 CAST
</div>

```sql title="Query"
SET use_variant_as_common_type=1;
SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

:::note
JSON 경로는 평탄화된 형태로 저장됩니다. 즉, `a.b.c`와 같은 경로를 사용해 JSON 객체를 포맷할 때
해당 객체를 `{ "a.b.c" : ... }`로 구성해야 하는지, 아니면 `{ "a": { "b": { "c": ... } } }`로 구성해야 하는지 알 수 없습니다.
현재 구현은 항상 후자로 가정합니다.

예시:

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

다음이 반환됩니다:

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

그리고 **아닙니다**:

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```

:::

<div id="reading-json-paths-as-sub-columns">
  ## JSON 경로를 서브컬럼으로 읽기
</div>

`JSON` 유형은 각 경로를 개별 서브컬럼으로 읽을 수 있습니다.
요청한 경로의 유형이 JSON 유형 선언에 지정되어 있지 않으면
해당 경로의 서브컬럼은 항상 [Dynamic](/ko/sql-reference/data-types/dynamic.md) 유형입니다.

예시는 다음과 같습니다:

```sql title="Query"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────┐
│ {"a":{"b":42,"g":42.42},"c":["1","2","3"],"d":"2020-01-01"} │
│ {"a":{"b":0},"d":"2020-01-02","f":"Hello, World!"}          │
│ {"a":{"b":43,"g":43.43},"c":["4","5","6"]}                  │
└─────────────────────────────────────────────────────────────┘
```

```sql title="Query (Reading JSON paths as sub-columns)"
SELECT json.a.b, json.a.g, json.c, json.d FROM test;
```

```text title="Response (Reading JSON paths as sub-columns)"
┌─json.a.b─┬─json.a.g─┬─json.c──┬─json.d─────┐
│       42 │ 42.42    │ [1,2,3] │ 2020-01-01 │
│        0 │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ    │ 2020-01-02 │
│       43 │ 43.43    │ [4,5,6] │ ᴺᵁᴸᴸ       │
└──────────┴──────────┴─────────┴────────────┘
```

`getSubcolumn` 함수를 사용해 JSON 유형의 서브컬럼을 읽을 수도 있습니다:

```sql title="Query"
SELECT getSubcolumn(json, 'a.b'), getSubcolumn(json, 'a.g'), getSubcolumn(json, 'c'), getSubcolumn(json, 'd') FROM test;
```

```text title="Response"
┌─getSubcolumn(json, 'a.b')─┬─getSubcolumn(json, 'a.g')─┬─getSubcolumn(json, 'c')─┬─getSubcolumn(json, 'd')─┐
│                        42 │ 42.42                     │ [1,2,3]                 │ 2020-01-01              │
│                         0 │ ᴺᵁᴸᴸ                      │ ᴺᵁᴸᴸ                    │ 2020-01-02              │
│                        43 │ 43.43                     │ [4,5,6]                 │ ᴺᵁᴸᴸ                    │
└───────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

요청한 경로가 데이터에 없으면 `NULL` 값으로 채워집니다:

```sql title="Query"
SELECT json.non.existing.path FROM test;
```

```text title="Response"
┌─json.non.existing.path─┐
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
└────────────────────────┘
```

반환된 서브컬럼의 데이터 유형을 확인하겠습니다:

```sql title="Query"
SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM test;
```

```text title="Response"
┌─toTypeName(json.a.b)─┬─toTypeName(json.a.g)─┬─toTypeName(json.c)─┬─toTypeName(json.d)─┐
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
└──────────────────────┴──────────────────────┴────────────────────┴────────────────────┘
```

보시다시피 `a.b`의 유형은 JSON 타입 선언에서 지정한 대로 `UInt32`이고,
그 밖의 모든 서브컬럼의 유형은 `Dynamic`입니다.

또한 특수 구문 `json.some.path.:TypeName`을 사용해 `Dynamic` 타입의 서브컬럼을 읽을 수도 있습니다:

```sql title="Query"
SELECT
    json.a.g.:Float64,
    dynamicType(json.a.g),
    json.d.:Date,
    dynamicType(json.d)
FROM test
```

```text title="Response"
┌─json.a.g.:`Float64`─┬─dynamicType(json.a.g)─┬─json.d.:`Date`─┬─dynamicType(json.d)─┐
│               42.42 │ Float64               │     2020-01-01 │ Date                │
│                ᴺᵁᴸᴸ │ None                  │     2020-01-02 │ Date                │
│               43.43 │ Float64               │           ᴺᵁᴸᴸ │ None                │
└─────────────────────┴───────────────────────┴────────────────┴─────────────────────┘
```

`Dynamic` 서브컬럼은 임의의 데이터 타입으로 캐스팅할 수 있습니다. 이 경우 `Dynamic` 내부의 타입을 지정한 타입으로 캐스팅할 수 없으면 예외가 발생합니다:

```sql title="Query"
SELECT json.a.g::UInt64 AS uint
FROM test;
```

```text title="Response"
┌─uint─┐
│   42 │
│    0 │
│   43 │
└──────┘
```

```sql title="Query"
SELECT json.a.g::UUID AS float
FROM test;
```

```text title="Response"
Received exception from server:
Code: 48. DB::Exception: Received from localhost:9000. DB::Exception:
Conversion between numeric types and UUID is not supported.
Probably the passed UUID is unquoted:
while executing 'FUNCTION CAST(__table1.json.a.g :: 2, 'UUID'_String :: 1) -> CAST(__table1.json.a.g, 'UUID'_String) UUID : 0'.
(NOT_IMPLEMENTED)
```

:::note
Compact MergeTree 파트에서 서브컬럼을 효율적으로 읽으려면 MergeTree 설정 [write&#95;marks&#95;for&#95;substreams&#95;in&#95;compact&#95;parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts)가 사용 설정되어 있는지 확인하십시오.
:::

<div id="reading-json-sub-objects-as-sub-columns">
  ## JSON 하위 객체를 서브컬럼으로 읽기
</div>

`JSON` 타입은 특수 구문 `json.^some.path`를 사용해 중첩된 객체를 `JSON` 타입의 서브컬럼으로 읽을 수 있도록 지원합니다:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'), ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'), ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":"42","g":42.42}},"c":["1","2","3"],"d":{"e":{"f":{"g":"Hello, World","h":["1","2","3"]}}}} │
│ {"d":{"e":{"f":{"h":["4","5","6"]}}},"f":"Hello, World!"}                                                 │
│ {"a":{"b":{"c":"43","e":"10","g":43.43}},"c":["4","5","6"]}                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.^a.b, json.^d.e.f FROM test;
```

```text title="Response"
┌─json.^`a`.b───────────────────┬─json.^`d`.e.f──────────────────────────┐
│ {"c":"42","g":42.42}          │ {"g":"Hello, World","h":["1","2","3"]} │
│ {}                            │ {"h":["4","5","6"]}                    │
│ {"c":"43","e":"10","g":43.43} │ {}                                     │
└───────────────────────────────┴────────────────────────────────────────┘
```

:::note
경로가 기본(`map`) [공유 데이터](#shared-data-structure)에 저장된 경우, 전체 공유 데이터 구조를 스캔해야 하므로 하위 객체의 서브컬럼을 읽는 작업이 비효율적일 수 있습니다. 반면 `map_with_buckets` 또는 `advanced` 공유 데이터 직렬화를 사용하면 공유 데이터에서 서브컬럼을 읽는 작업이 크게 최적화됩니다.
:::

<div id="reading-json-combined-sub-columns">
  ## JSON 결합 서브컬럼 읽기
</div>

`JSON` 타입은 특수 구문 `json.@some.path`를 사용해 경로를 **결합 서브컬럼**으로 읽을 수 있습니다.
지정된 경로의 결합 서브컬럼은 다음을 반환합니다.

* 해당 경로에 리터럴 값이 있으면, 그 경로에 저장된 리터럴 값을 `Dynamic`으로 반환합니다.
* 해당 경로에 리터럴 값은 없지만 중첩된 하위 경로가 있으면, 해당 경로의 JSON 하위 객체를 `Dynamic`으로 반환합니다.
* 해당 경로에 리터럴 값도 하위 경로도 없으면 `NULL`을 반환합니다.

이 기능은 경로가 행에 따라 스칼라 값 또는 중첩 객체를 담을 수 있을 때 유용하며, 리터럴 서브컬럼(`json.a`)과 하위 객체 서브컬럼(`json.^a`)을 각각 따로 쿼리하는 것보다 더 편리합니다.

다음 예시는 경로 `a`에 대한 세 가지 서브컬럼 타입을 비교합니다.

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : 42, "b" : {"c" : 1, "d" : "Hello"}}'), ('{"a" : {"x": 1, "y": 2}, "b" : {"c" : 1}}'), ('{"c" : "World"}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────┐
│ {"a":42,"b":{"c":1,"d":"Hello"}}│
│ {"a":{"x":1,"y":2},"b":{"c":1}}│
│ {"c":"World"}                   │
└─────────────────────────────────┘
```

```sql title="Query"
SELECT
    json.a,
    dynamicType(json.a),
    json.^a,
    toTypeName(json.^a),
    json.@a,
    dynamicType(json.@a)
FROM test;
```

```text title="Response"
┌─json.a─┬─dynamicType(json.a)─┬─json.^a───────┬─toTypeName(json.^a)─┬─json.@a───────┬─dynamicType(json.@a)─┐
│ 42     │ Int64               │ {}            │ JSON                │ 42            │ Int64                │
│ NULL   │ None                │ {"x":1,"y":2} │ JSON                │ {"x":1,"y":2} │ JSON                 │
│ NULL   │ None                │ {}            │ JSON                │ NULL          │ None                 │
└────────┴─────────────────────┴───────────────┴─────────────────────┴───────────────┴──────────────────────┘
```

* 행 1: `a`에는 리터럴 `42`가 들어 있습니다. `json.a`는 이를 `Dynamic(Int64)`로 반환하고, `json.^a`는 빈 하위 객체 `{}`를 반환합니다(`a` 아래에 중첩된 키가 없음). `json.@a`는 리터럴 `42`를 반환합니다.
* 행 2: `a`에는 중첩된 객체가 들어 있습니다. `json.a`는 `NULL`을 반환합니다(해당 경로에 리터럴이 없음). `json.^a`는 하위 객체를 `JSON`으로 반환하고, `json.@a`도 하위 객체를 `Dynamic(JSON)`로 반환합니다.
* 행 3: `a`는 아예 없습니다. `json.a`와 `json.@a`는 모두 `NULL`을 반환하고, `json.^a`는 빈 `{}`를 반환합니다.

:::note
경로가 기본 `map` [공유 데이터](#shared-data-structure)에 저장된 경우, 결합 서브컬럼을 읽으려면 전체 공유 데이터 구조를 스캔해야 하므로 비효율적일 수 있습니다. `map_with_buckets` 또는 `advanced` 공유 데이터 직렬화를 사용하면 공유 데이터에서 서브컬럼을 읽는 작업이 크게 최적화됩니다.
:::

<div id="type-inference-for-paths">
  ## 경로에 대한 유형 추론
</div>

`JSON`을 파싱하는 동안 ClickHouse는 각 JSON 경로에 대해 가장 적절한 데이터 유형을 판별하려고 합니다.
이 동작은 [입력 데이터에서 자동 스키마 추론](/ko/interfaces/schema-inference.md)과 비슷하며,
동일한 설정으로 제어됩니다:

* [input&#95;format&#95;try&#95;infer&#95;dates](/ko/operations/settings/formats#input_format_try_infer_dates)
* [input&#95;format&#95;try&#95;infer&#95;datetimes](/ko/operations/settings/formats#input_format_try_infer_datetimes)
* [schema&#95;inference&#95;make&#95;columns&#95;nullable](/ko/operations/settings/formats#schema_inference_make_columns_nullable)
* [input&#95;format&#95;json&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ko/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
* [input&#95;format&#95;json&#95;infer&#95;incomplete&#95;types&#95;as&#95;strings](/ko/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
* [input&#95;format&#95;json&#95;read&#95;numbers&#95;as&#95;strings](/ko/operations/settings/formats#input_format_json_read_numbers_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;strings](/ko/operations/settings/formats#input_format_json_read_bools_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;numbers](/ko/operations/settings/formats#input_format_json_read_bools_as_numbers)
* [input&#95;format&#95;json&#95;read&#95;arrays&#95;as&#95;strings](/ko/operations/settings/formats#input_format_json_read_arrays_as_strings)
* [input&#95;format&#95;json&#95;infer&#95;array&#95;of&#95;dynamic&#95;from&#95;array&#95;of&#95;different&#95;types](/ko/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

몇 가지 예시를 살펴보겠습니다:

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=1, input_format_try_infer_datetimes=1;
```

```text title="Response"
┌─paths_with_types─────────────────┐
│ {'a':'Date','b':'DateTime64(9)'} │
└──────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
```

```text title="Response"
┌─paths_with_types────────────┐
│ {'a':'String','b':'String'} │
└─────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=1;
```

```text title="Response"
┌─paths_with_types───────────────┐
│ {'a':'Array(Nullable(Int64))'} │
└────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=0;
```

```text title="Response"
┌─paths_with_types─────┐
│ {'a':'Array(Int64)'} │
└──────────────────────┘
```

<div id="handling-arrays-of-json-objects">
  ## JSON 객체 배열 처리
</div>

객체 배열이 포함된 JSON 경로는 `Array(JSON)` 유형으로 파싱되며, 해당 경로용 `Dynamic` 컬럼에 삽입됩니다.
객체 배열을 읽으려면 `Dynamic` 컬럼에서 서브컬럼으로 추출할 수 있습니다:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES
('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
('{"a" : {"b" : [1, 2, 3]}}'),
('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":[{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}},{"c":"43"},{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}]}} │
│ {"a":{"b":["1","2","3"]}}                                                                                                                                               │
│ {"a":{"b":[{"c":"44","f":[[{"h":"2020-01-02"}]]},{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}]}}                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.a.b, dynamicType(json.a.b) FROM test;
```

```text title="Response"
┌─json.a.b──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─dynamicType(json.a.b)────────────────────────────────────┐
│ ['{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}}','{"c":"43"}','{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}'] │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
│ [1,2,3]                                                                                                                                                           │ Array(Nullable(Int64))                                   │
│ ['{"c":"44","f":[[{"h":"2020-01-02"}]]}','{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}']                                                  │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

이미 눈치채셨겠지만, 중첩된 `JSON` 유형의 `max_dynamic_types`/`max_dynamic_paths` 매개변수는 기본값보다 더 작게 설정되었습니다.
이는 JSON 객체의 중첩 배열에서 서브컬럼 수가 제어되지 않은 채 계속 증가하는 것을 방지하기 위해 필요합니다.

이제 중첩된 `JSON` 컬럼에서 서브컬럼을 읽어 보겠습니다:

```sql title="Query"
SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

특수 구문을 사용하면 `Array(JSON)` 서브컬럼 이름을 일일이 쓰지 않아도 됩니다:

```sql title="Query"
SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

경로 뒤에 오는 `[]`의 개수는 배열 수준을 나타냅니다. 예를 들어 `json.path[][]`는 `json.path.:Array(Array(JSON))`로 변환됩니다.

이제 `Array(JSON)` 내부의 경로와 타입을 확인해 보겠습니다:

```sql title="Query"
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b[]))) FROM test;
```

```text title="Response"
┌─arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b.:`Array(JSON)`)))──┐
│ ('c','Int64')                                                         │
│ ('d','String')                                                        │
│ ('f','Array(Array(JSON(max_dynamic_types=8, max_dynamic_paths=64)))') │
│ ('k.j','Int64')                                                       │
│ ('e','Array(Nullable(Int64))')                                        │
└───────────────────────────────────────────────────────────────────────┘
```

`Array(JSON)` 컬럼에서 서브컬럼을 읽어보겠습니다:

```sql title="Query"
SELECT json.a.b[].c.:Int64, json.a.b[].f[][].g.:Float64, json.a.b[].f[][].h.:Date FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c.:`Int64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.g.:`Float64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.h.:`Date`─┐
│ [42,43,NULL]                       │ [[[42.42]],[],[[43.43]]]                                     │ [[[NULL]],[],[['2020-01-01']]]                            │
│ []                                 │ []                                                           │ []                                                        │
│ [44,NULL]                          │ [[[NULL]],[[44.44]]]                                         │ [[['2020-01-02']],[[NULL]]]                               │
└────────────────────────────────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

중첩된 `JSON` 컬럼의 하위 객체 서브컬럼도 읽을 수 있습니다:

```sql title="Query"
SELECT json.a.b[].^k FROM test
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.^`k`─────────┐
│ ['{"j":"1000"}','{}','{"j":"2000"}'] │
│ []                                   │
│ ['{}','{"j":"3000"}']                │
└──────────────────────────────────────┘
```

<div id="handling-json-keys-with-nulls">
  ## NULL 값인 JSON 키 처리
</div>

JSON 구현에서는 `null`과 값이 없는 상태를 동일한 것으로 간주합니다:

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

이는 원래 JSON 데이터에 NULL 값을 가진 해당 경로가 있었는지, 아니면 애초에 그 경로 자체가 없었는지를 판별할 수 없다는 의미입니다.

<div id="handling-json-keys-with-dots">
  ## 점(.)이 포함된 JSON 키 처리
</div>

JSON 컬럼은 내부적으로 모든 경로와 값을 평탄화된 형태로 저장합니다. 즉, 기본적으로 다음 2개의 객체는 동일한 것으로 간주됩니다:

```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

둘 다 내부적으로는 경로 `a.b`와 값 `42`의 쌍으로 저장됩니다. JSON을 포맷팅할 때는 항상 점으로 구분된 경로의 각 부분을 기준으로 중첩된 객체를 만듭니다:

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

보시다시피, 원래 JSON `{"a.b" : 42}`는 이제 `{"a" : {"b" : 42}}` 형식으로 포맷됩니다.

이 제한 때문에 다음과 같은 유효한 JSON 객체도 파싱에 실패합니다:

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

점이 포함된 키를 유지하고 이를 중첩된 객체로 포맷하지 않으려면
설정 [json&#95;type&#95;escape&#95;dots&#95;in&#95;keys](/ko/operations/settings/formats#json_type_escape_dots_in_keys)를 활성화할 수
있습니다(`25.8` 버전부터 사용 가능). 이 경우 파싱 시 JSON 키의 모든 점은
`%2E`로 이스케이프되며, 포맷 시 다시 원래대로 복원됩니다.

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a.b":"42"} │ ['a.b']             │ ['a%2Eb']           │
└──────────────────┴──────────────┴─────────────────────┴─────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, JSONAllPaths(json);
```

```text title="Response"
┌─json──────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ ['a%2Eb','a.b']    │
└───────────────────────────────────────┴────────────────────┘
```

점(.)이 이스케이프된 키를 서브컬럼으로 읽으려면 서브컬럼 이름에도 이스케이프된 점(.)을 사용해야 합니다:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

참고: identifiers 파서 및 분석기의 한계로 인해 서브컬럼 `` json.`a.b` ``은 서브컬럼 `json.a.b`와 동일하며, 이스케이프된 점이 포함된 경로는 읽지 않습니다:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

또한 점이 포함된 키가 있는 JSON path에 대한 hint를 지정하려는 경우(또는 이를 `SKIP`/`SKIP REGEX` 섹션에서 사용하려는 경우), hint에서는 점을 이스케이프해야 합니다:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(`a%2Eb` UInt8) as json, json.`a%2Eb`, toTypeName(json.`a%2Eb`);
```

```text title="Response"
┌─json────────────────────────────────┬─json.a%2Eb─┬─toTypeName(json.a%2Eb)─┐
│ {"a.b":42,"a":{"b":"Hello World!"}} │         42 │ UInt8                  │
└─────────────────────────────────────┴────────────┴────────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(SKIP `a%2Eb`) as json, json.`a%2Eb`;
```

```text title="Response"
┌─json───────────────────────┬─json.a%2Eb─┐
│ {"a":{"b":"Hello World!"}} │ ᴺᵁᴸᴸ       │
└────────────────────────────┴────────────┘
```

<div id="reading-json-type-from-data">
  ## 데이터에서 JSON 타입 읽기
</div>

모든 텍스트 형식
([`JSONEachRow`](/ko/interfaces/formats/JSONEachRow),
[`TSV`](/ko/interfaces/formats/TabSeparated),
[`CSV`](/ko/interfaces/formats/CSV),
[`CustomSeparated`](/ko/interfaces/formats/CustomSeparated),
[`Values`](/ko/interfaces/formats/Values) 등)은 `JSON` 타입을 읽을 수 있습니다.

예시:

```sql title="Query"
SELECT json FROM format(JSONEachRow, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP d.e, SKIP REGEXP \'b.*\')', '
{"json" : {"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}}
{"json" : {"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}}
{"json" : {"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}}
{"json" : {"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}}
{"json" : {"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}}
')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

`CSV`/`TSV`/기타 등의 텍스트 형식에서는 `JSON` 객체를 포함하는 문자열에서 `JSON`을 파싱합니다:

```sql title="Query"
SELECT json FROM format(TSV, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP REGEXP \'b.*\')',
'{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

<div id="reaching-the-limit-of-dynamic-paths-inside-json">
  ## JSON 내부 동적 경로 한도에 도달한 경우
</div>

`JSON` 데이터 타입은 내부적으로 제한된 수의 경로만 개별 서브컬럼으로 저장할 수 있습니다.
기본적으로 이 한도는 `1024`이며, 타입 선언에서 `max_dynamic_paths` 매개변수를 사용해 변경할 수 있습니다.

한도에 도달하면 `JSON` 컬럼에 새로 삽입되는 모든 경로는 하나의 공유 데이터 구조에 저장됩니다.
이러한 경로도 여전히 서브컬럼으로 읽을 수 있지만,
효율이 다소 떨어질 수 있습니다([공유 데이터에 대한 섹션 참조](#shared-data-structure)).
이 한도는 서로 다른 서브컬럼이 지나치게 많아져 테이블을 사용할 수 없게 되는 상황을 방지하기 위해 필요합니다.

이제 몇 가지 시나리오에서 한도에 도달하면 어떤 일이 발생하는지 살펴보겠습니다.

<div id="reaching-the-limit-during-data-parsing">
  ### 데이터 파싱 중 한도에 도달한 경우
</div>

데이터에서 `JSON` 객체를 파싱하는 동안 현재 데이터 블록의 한도에 도달하면,
모든 새 경로는 공유 데이터 구조에 저장됩니다. 다음 두 가지 인트로스펙션 함수 `JSONDynamicPaths`, `JSONSharedDataPaths`를 사용할 수 있습니다:

```sql title="Query"
SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM format(JSONEachRow, 'json JSON(max_dynamic_paths=3)', '
{"json" : {"a" : {"b" : 42}, "c" : [1, 2, 3]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-01"}}
{"json" : {"a" : {"b" : 44}, "c" : [4, 5, 6]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-02", "e" : "Hello", "f" : {"g" : 42.42}}}
{"json" : {"a" : {"b" : 43}, "c" : [7, 8, 9], "f" : {"g" : 43.43}, "h" : "World"}}
')
```

```text title="Response"
┌─json───────────────────────────────────────────────────────────┬─JSONDynamicPaths(json)─┬─JSONSharedDataPaths(json)─┐
│ {"a":{"b":"42"},"c":["1","2","3"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-01"}                              │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"44"},"c":["4","5","6"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-02","e":"Hello","f":{"g":42.42}}  │ ['a.b','c','d']        │ ['e','f.g']               │
│ {"a":{"b":"43"},"c":["7","8","9"],"f":{"g":43.43},"h":"World"} │ ['a.b','c','d']        │ ['f.g','h']               │
└────────────────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────┘
```

보시다시피 경로 `e`와 `f.g`를 삽입한 뒤 제한에 도달했고,
이들은 공유 데이터 구조에 삽입되었습니다.

<div id="during-merges-of-data-parts-in-mergetree-table-engines">
  ### MergeTree 테이블 엔진에서 데이터 파트가 머지되는 동안
</div>

`MergeTree` 테이블에서 여러 데이터 파트를 머지할 때, 결과 데이터 파트의 `JSON` 컬럼이 동적 경로 한도에 도달해
소스 파트의 모든 경로를 서브컬럼으로 저장하지 못할 수 있습니다.
이 경우 ClickHouse는 머지 후 어떤 경로를 서브컬럼으로 유지하고 어떤 경로를 공유 데이터 구조에 저장할지 결정합니다.
대부분의 경우 ClickHouse는
NULL이 아닌 값이 가장 많은 경로를 유지하고, 가장 드문 경로를 공유 데이터 구조로 옮기려고 합니다. 다만, 이는 구현에 따라 달라질 수 있습니다.

이러한 머지의 예시를 살펴보겠습니다.
먼저 `JSON` 컬럼이 있는 테이블을 생성하고, 동적 경로 한도를 `3`으로 설정한 다음, 서로 다른 `5`개의 경로를 가진 값을 삽입하겠습니다:

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

각 삽입 시 `JSON` 컬럼에 단일 경로가 포함된 별도의 데이터 파트(data part)가 생성됩니다:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       5 │ ['a']         │ []                │ all_1_1_0 │
│       4 │ ['b']         │ []                │ all_2_2_0 │
│       3 │ ['c']         │ []                │ all_3_3_0 │
│       2 │ ['d']         │ []                │ all_4_4_0 │
│       1 │ ['e']         │ []                │ all_5_5_0 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

이제 모든 파트를 하나로 머지한 뒤 어떤 일이 일어나는지 살펴보겠습니다:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│      15 │ ['a','b','c'] │ ['d','e']         │ all_1_5_2 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

보시다시피 ClickHouse는 가장 자주 나타나는 경로인 `a`, `b`, `c`는 그대로 유지하고, `d`와 `e` 경로는 공유 데이터 구조로 옮겼습니다.

<div id="shared-data-structure">
  ## 공유 데이터 구조
</div>

이전 섹션에서 설명했듯이 `max_dynamic_paths` 한도에 이르면 모든 새 경로는 하나의 공유 데이터 구조에 저장됩니다.
이 섹션에서는 공유 데이터 구조의 세부 사항과 이 구조에서 경로 서브컬럼을 읽는 방법을 살펴봅니다.

JSON 컬럼의 내용을 검사하는 데 사용되는 함수에 대한 자세한 내용은 [&quot;인트로스펙션 함수&quot;](/ko/sql-reference/data-types/newjson#introspection-functions) 섹션을 참조하십시오.

<div id="shared-data-structure-in-memory">
  ### 메모리 내 공유 데이터 구조
</div>

메모리에서 공유 데이터 구조는 평탄화된 JSON 경로와 바이너리로 인코딩된 값의 매핑을 저장하는 `Map(String, String)` 타입의 서브컬럼일 뿐입니다.
여기에서 경로 서브컬럼을 추출하려면 이 `Map` 컬럼의 모든 행을 순회하며 요청된 경로와 해당 값을 찾으면 됩니다.

<div id="shared-data-structure-in-merge-tree-parts">
  ### MergeTree 파트의 공유 데이터 구조
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 테이블에서는 모든 내용을 디스크(로컬 또는 원격)에 저장하는 데이터 파트에 데이터를 저장합니다. 또한 디스크의 데이터는 메모리에 저장되는 방식과 다를 수 있습니다.
현재 MergeTree 데이터 파트에는 3가지 공유 데이터 구조 직렬화(serialization) 방식인 `map`, `map_with_buckets`,
`advanced`가 있습니다.

직렬화 버전은 MergeTree
설정 [object&#95;shared&#95;data&#95;serialization&#95;version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
및 [object&#95;shared&#95;data&#95;serialization&#95;version&#95;for&#95;zero&#95;level&#95;parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
로 제어됩니다
(0 수준 파트는 테이블에 데이터를 삽입할 때 생성되는 파트이며, 머지 과정에서 생성되는 파트는 더 높은 수준을 가집니다).

참고: 공유 데이터 구조 직렬화 변경은
`v3` [object serialization version](../../operations/settings/merge-tree-settings.md#object_serialization_version)에서만 지원됩니다

<div id="shared-data-map">
  #### 맵
</div>

`map` 직렬화 version에서는 공유 데이터가 메모리에 저장되는 방식과 동일하게 `Map(String, String)` 유형의 단일 컬럼으로 직렬화됩니다. 이 유형의 직렬화에서 경로 서브컬럼을 읽으려면 ClickHouse가 전체 `Map` 컬럼을 읽은 뒤,
메모리에서 요청된 경로를 추출합니다.

이 직렬화은 데이터를 쓰거나 전체 `JSON` 컬럼을 읽을 때는 효율적이지만, 경로 서브컬럼을 읽을 때는 비효율적입니다.

<div id="shared-data-map-with-buckets">
  #### 버킷이 있는 맵
</div>

`map_with_buckets` 직렬화 버전에서는 공유 데이터가 `Map(String, String)` 유형의 `N`개 컬럼(&quot;버킷&quot;)으로 직렬화됩니다.
각 버킷에는 경로의 부분 집합만 포함됩니다. 이 직렬화 유형에서 경로 서브컬럼을 읽으려면 ClickHouse는
단일 버킷에서 전체 `Map` 컬럼을 읽고 메모리에서 요청된 경로를 추출합니다.

이 직렬화는 데이터를 쓰거나 전체 `JSON` 컬럼을 읽을 때는 덜 효율적이지만, 경로 서브컬럼을 읽을 때는 더 효율적입니다.
필요한 버킷의 데이터만 읽기 때문입니다.

버킷 수 `N`은 MergeTree 설정 [object&#95;shared&#95;data&#95;buckets&#95;for&#95;compact&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (기본값 8)
및 [object&#95;shared&#95;data&#95;buckets&#95;for&#95;wide&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (기본값 32)로 제어됩니다.
두 설정 모두 허용되는 최댓값은 256입니다.

<div id="shared-data-advanced">
  #### 고급
</div>

`advanced` 직렬화 버전에서는 공유 데이터가 요청된 경로의 데이터만 읽을 수 있도록 하는 추가 정보를 저장하는 특수한 데이터 구조로 직렬화되며, 이를 통해 경로 서브컬럼 읽기 성능을 극대화합니다.
또한 이 직렬화는 버킷도 지원하므로 각 버킷에는 경로의 일부 집합만 포함됩니다.

이 직렬화는 데이터 쓰기에는 상당히 비효율적이므로(따라서 zero-level 파트에는 이 직렬화를 사용하는 것을 권장하지 않습니다), 전체 `JSON` 컬럼을 읽는 효율은 `map` 직렬화보다 약간 떨어지지만 경로 서브컬럼을 읽을 때는 매우 효율적입니다.

참고: 이 데이터 구조 내부에 추가 정보를 일부 저장하므로, 이 직렬화를 사용할 때의 디스크 저장 크기는
`map` 및 `map_with_buckets` 직렬화보다 더 큽니다.

새로운 공유 데이터 직렬화와 구현 세부 사항에 대한 더 자세한 개요는 [블로그 게시물](https://clickhouse.com/blog/json-data-type-gets-even-better)을 참조하십시오.

<div id="controlling-the-number-of-dynamic-paths">
  ## MergeTree 파트 내 JSON의 동적 경로 수 제어하기
</div>

JSON에서 동적 경로 수의 한도를 설정하는 가장 기본적인 방법은 JSON 타입 선언에서 `max_dynamic_paths` 매개변수를 사용하는 것입니다.
하지만 기존 컬럼의 `max_dynamic_paths`를 변경하려면 `ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)`를 실행해야 하며, 그러면 기존의 모든 파트를 재작성하는 백그라운드 뮤테이션이 시작됩니다.
이러한 뮤테이션은 상당히 부담이 클 수 있으며, 완료될 때까지 서버 성능에 영향을 줄 수 있습니다. 이를 피하려면 새 데이터 파트에 대해 MergeTree 테이블의 동적 경로 한도를 변경하는 데 도움이 되는 다음 3개의 설정을 사용할 수 있습니다:

* `merge_max_dynamic_subcolumns_in_wide_part` - Wide 데이터 파트로 머지하는 동안 각 JSON 컬럼의 동적 서브컬럼 수를 제한하는 MergeTree 설정입니다.
* `merge_max_dynamic_subcolumns_in_compact_part` - Compact 데이터 파트로 머지하는 동안 각 JSON 컬럼의 동적 서브컬럼 수를 제한하는 MergeTree 설정입니다.
* `max_dynamic_subcolumns_in_json_type_parsing` - JSON 데이터를 JSON 컬럼으로 파싱하는 동안 각 JSON 컬럼의 동적 서브컬럼 수를 제한하는 세션 설정입니다.

참고: 위 설정들의 값이 더 크더라도 동적 경로 한도는 `max_dynamic_paths` 매개변수에 지정된 값을 초과할 수 없습니다.

<div id="introspection-functions">
  ## 인트로스펙션 함수
</div>

JSON 컬럼의 내용을 검사하는 데 유용한 함수는 여러 가지가 있습니다:

* [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
* [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
* [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
* [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
* [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
* [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
* [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
* [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
* [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**예시**

`2020-01-01` 날짜의 [GH Archive](https://www.gharchive.org/) 데이터셋 내용을 살펴보겠습니다:

```sql title="Query"
SELECT arrayJoin(distinctJSONPaths(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
```

```text title="Response"
┌─arrayJoin(distinctJSONPaths(json))─────────────────────────┐
│ actor.avatar_url                                           │
│ actor.display_login                                        │
│ actor.gravatar_id                                          │
│ actor.id                                                   │
│ actor.login                                                │
│ actor.url                                                  │
│ created_at                                                 │
│ id                                                         │
│ org.avatar_url                                             │
│ org.gravatar_id                                            │
│ org.id                                                     │
│ org.login                                                  │
│ org.url                                                    │
│ payload.action                                             │
│ payload.before                                             │
│ payload.comment._links.html.href                           │
│ payload.comment._links.pull_request.href                   │
│ payload.comment._links.self.href                           │
│ payload.comment.author_association                         │
│ payload.comment.body                                       │
│ payload.comment.commit_id                                  │
│ payload.comment.created_at                                 │
│ payload.comment.diff_hunk                                  │
│ payload.comment.html_url                                   │
│ payload.comment.id                                         │
│ payload.comment.in_reply_to_id                             │
│ payload.comment.issue_url                                  │
│ payload.comment.line                                       │
│ payload.comment.node_id                                    │
│ payload.comment.original_commit_id                         │
│ payload.comment.original_position                          │
│ payload.comment.path                                       │
│ payload.comment.position                                   │
│ payload.comment.pull_request_review_id                     │
...
│ payload.release.node_id                                    │
│ payload.release.prerelease                                 │
│ payload.release.published_at                               │
│ payload.release.tag_name                                   │
│ payload.release.tarball_url                                │
│ payload.release.target_commitish                           │
│ payload.release.upload_url                                 │
│ payload.release.url                                        │
│ payload.release.zipball_url                                │
│ payload.size                                               │
│ public                                                     │
│ repo.id                                                    │
│ repo.name                                                  │
│ repo.url                                                   │
│ type                                                       │
└─arrayJoin(distinctJSONPaths(json))─────────────────────────┘
```

```sql title="Query"
SELECT arrayJoin(distinctJSONPathsAndTypes(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
SETTINGS date_time_input_format = 'best_effort'
```

```text title="Response"
┌─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┐
│ ('actor.avatar_url',['String'])                             │
│ ('actor.display_login',['String'])                          │
│ ('actor.gravatar_id',['String'])                            │
│ ('actor.id',['Int64'])                                      │
│ ('actor.login',['String'])                                  │
│ ('actor.url',['String'])                                    │
│ ('created_at',['DateTime'])                                 │
│ ('id',['String'])                                           │
│ ('org.avatar_url',['String'])                               │
│ ('org.gravatar_id',['String'])                              │
│ ('org.id',['Int64'])                                        │
│ ('org.login',['String'])                                    │
│ ('org.url',['String'])                                      │
│ ('payload.action',['String'])                               │
│ ('payload.before',['String'])                               │
│ ('payload.comment._links.html.href',['String'])             │
│ ('payload.comment._links.pull_request.href',['String'])     │
│ ('payload.comment._links.self.href',['String'])             │
│ ('payload.comment.author_association',['String'])           │
│ ('payload.comment.body',['String'])                         │
│ ('payload.comment.commit_id',['String'])                    │
│ ('payload.comment.created_at',['DateTime'])                 │
│ ('payload.comment.diff_hunk',['String'])                    │
│ ('payload.comment.html_url',['String'])                     │
│ ('payload.comment.id',['Int64'])                            │
│ ('payload.comment.in_reply_to_id',['Int64'])                │
│ ('payload.comment.issue_url',['String'])                    │
│ ('payload.comment.line',['Int64'])                          │
│ ('payload.comment.node_id',['String'])                      │
│ ('payload.comment.original_commit_id',['String'])           │
│ ('payload.comment.original_position',['Int64'])             │
│ ('payload.comment.path',['String'])                         │
│ ('payload.comment.position',['Int64'])                      │
│ ('payload.comment.pull_request_review_id',['Int64'])        │
...
│ ('payload.release.node_id',['String'])                      │
│ ('payload.release.prerelease',['Bool'])                     │
│ ('payload.release.published_at',['DateTime'])               │
│ ('payload.release.tag_name',['String'])                     │
│ ('payload.release.tarball_url',['String'])                  │
│ ('payload.release.target_commitish',['String'])             │
│ ('payload.release.upload_url',['String'])                   │
│ ('payload.release.url',['String'])                          │
│ ('payload.release.zipball_url',['String'])                  │
│ ('payload.size',['Int64'])                                  │
│ ('public',['Bool'])                                         │
│ ('repo.id',['Int64'])                                       │
│ ('repo.name',['String'])                                    │
│ ('repo.url',['String'])                                     │
│ ('type',['String'])                                         │
└─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┘
```

<div id="alter-modify-column-to-json-type">
  ## ALTER MODIFY COLUMN으로 JSON 타입으로 변경
</div>

기존 테이블에서 컬럼 타입을 새 `JSON` 타입으로 변경할 수 있습니다. 현재는 `String` 타입에서만 `ALTER`를 지원합니다.

**예시**

```sql title="Query"
CREATE TABLE test (json String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test VALUES ('{"a" : 42}'), ('{"a" : 43, "b" : "Hello"}'), ('{"a" : 44, "b" : [1, 2, 3]}'), ('{"c" : "2020-01-01"}');
ALTER TABLE test MODIFY COLUMN json JSON;
SELECT json, json.a, json.b, json.c FROM test;
```

```text title="Response"
┌─json─────────────────────────┬─json.a─┬─json.b──┬─json.c─────┐
│ {"a":"42"}                   │ 42     │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ       │
│ {"a":"43","b":"Hello"}       │ 43     │ Hello   │ ᴺᵁᴸᴸ       │
│ {"a":"44","b":["1","2","3"]} │ 44     │ [1,2,3] │ ᴺᵁᴸᴸ       │
│ {"c":"2020-01-01"}           │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ    │ 2020-01-01 │
└──────────────────────────────┴────────┴─────────┴────────────┘
```

<div id="lazy-type-hints">
  ## Lazy Type Hints (실험 기능)
</div>

:::note
이 기능은 실험 기능이며, `allow_experimental_json_lazy_type_hints` 설정을 활성화해야 합니다.
:::

`ALTER TABLE ... MODIFY COLUMN`을 사용해 JSON 컬럼에 타입 힌트를 추가하거나 수정하면, ClickHouse는 일반적으로 새 타입 힌트를 구체화하기 위해 모든 데이터 파트를 재작성합니다. 대량의 이력 데이터(수백 TB)를 보유한 테이블에서는 이 작업에 매우 큰 비용이 들 수 있습니다.

**Lazy type hints**를 사용하면 기존 데이터를 재작성하지 않고 메타데이터만 변경하여 타입 힌트를 추가할 수 있습니다:

* **기존 파트**: 타입 힌트는 `Dynamic`에서 힌트된 유형으로 CAST되어 쿼리 시점에 적용됩니다.
* **새 파트**: 타입 힌트는 `INSERT` 작업 중에 구체화됩니다.
* **머지**: 파트가 머지될 때 타입 힌트가 구체화됩니다.

즉, 타입 힌트를 즉시 추가할 수 있으며, 이후 일반적인 백그라운드 머지가 진행되면서 데이터가 점진적으로 변환됩니다.

<div id="enabling-lazy-type-hints">
  ### Lazy Type Hints 활성화하기
</div>

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

<div id="lazy-type-hints-example">
  ### 예시
</div>

```sql title="Query"
-- Create a table and insert data
CREATE TABLE test_lazy (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_lazy VALUES ('{"user_id": "123", "score": "95.5"}');

-- Enable experimental setting
SET allow_experimental_json_lazy_type_hints = 1;

-- Add type hints - this completes instantly without mutation
ALTER TABLE test_lazy MODIFY COLUMN json JSON(user_id UInt64, score Float64);

-- Query the data - type hints are applied at read time
SELECT json.user_id, toTypeName(json.user_id), json.score, toTypeName(json.score) FROM test_lazy;
```

```text title="Response"
┌─json.user_id─┬─toTypeName(json.user_id)─┬─json.score─┬─toTypeName(json.score)─┐
│          123 │ UInt64                   │       95.5 │ Float64                │
└──────────────┴──────────────────────────┴────────────┴────────────────────────┘
```

<div id="verifying-no-mutation-occurred">
  ### 뮤테이션이 발생하지 않았는지 확인하기
</div>

`system.mutations` 테이블을 확인하면 `ALTER`가 뮤테이션 없이 완료되었는지 검증할 수 있습니다:

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

Lazy Type Hints가 활성화된 상태에서는 이 쿼리가 어떤 행도 반환하지 않으므로, 해당 작업이 메타데이터만 수정하는 작업이었음을 확인할 수 있습니다.

<div id="materializing-type-hints">
  ### 타입 힌트 구체화하기
</div>

기존 데이터에 타입 힌트를 구체화하려면 다음 방법 중 하나를 사용할 수 있습니다.

1. **백그라운드 머지 대기**: 파트가 머지될 때 ClickHouse가 타입 힌트를 자동으로 구체화합니다
2. **강제 머지**: `OPTIMIZE TABLE test_lazy FINAL`을 사용하여 모든 파트를 즉시 머지합니다
3. **파트 재작성**: `ALTER TABLE test_lazy REWRITE PARTS`를 사용하여 새 메타데이터로 파트를 재작성합니다

<div id="lazy-type-hints-limitations">
  ### 제한 사항
</div>

* 이 기능은 실험적 기능이며 향후 버전에서 변경될 수 있습니다
* 쿼리 시점에 타입을 변환하면, 특히 큰 JSON 객체의 경우 미리 구체화된 타입보다 성능 오버헤드가 크게 발생할 수 있습니다
* 이 기능은 `typed_paths`(타입 힌트)를 수정할 때만 적용됩니다. `max_dynamic_paths`, `SKIP`, `SKIP REGEXP`와 같은 다른 JSON 매개변수는 여전히 뮤테이션이 필요합니다

<div id="comparison-between-values-of-the-json-type">
  ## JSON 타입 값 간 비교
</div>

JSON 객체는 맵과 비슷한 방식으로 비교됩니다.

예시:

```sql title="Query"
CREATE TABLE test (json1 JSON, json2 JSON) ENGINE=Memory;
INSERT INTO test FORMAT JSONEachRow
{"json1" : {}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : [1, 2, 3]}}
{"json1" : {"a" : 42}, "json2" : {"a" : "Hello"}}
{"json1" : {"a" : 42}, "json2" : {"b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42, "b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41, "b" : 42}}

SELECT json1, json2, json1 < json2, json1 = json2, json1 > json2 FROM test;
```

```text title="Response"
┌─json1──────┬─json2───────────────┬─less(json1, json2)─┬─equals(json1, json2)─┬─greater(json1, json2)─┐
│ {}         │ {}                  │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {}                  │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"41"}          │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"42"}          │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {"a":["1","2","3"]} │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"Hello"}       │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"b":"42"}          │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"42","b":"42"} │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"41","b":"42"} │                  0 │                    0 │                     1 │
└────────────┴─────────────────────┴────────────────────┴──────────────────────┴───────────────────────┘
```

**참고:** 2개의 경로에 서로 다른 데이터 타입의 값이 있으면, 해당 값은 `Variant` 데이터 타입의 [비교 규칙](/ko/sql-reference/data-types/variant#comparing-values-of-variant-data)에 따라 비교됩니다.

<div id="data-skipping-indexes-for-json">
  ## JSON의 데이터 스키핑 인덱스
</div>

[데이터 스키핑 인덱스](/ko/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)는 `JSON` 컬럼에 대해 다음 3가지 방식으로 사용할 수 있습니다:

1. **특정 서브컬럼에 대한 인덱스** — 일반 컬럼과 마찬가지로, 알려진 JSON 경로에 표준 스킵 인덱스를 생성합니다. 그러면 해당 경로의 *값*이 인덱싱됩니다.
2. **`JSONAllPaths`를 사용하는 경로 기반 인덱스** — 각 그래뉼에 존재하는 *경로 집합*을 인덱싱하여, 쿼리된 경로를 포함할 수 없는 그래뉼을 건너뜁니다.
3. **`JSONAllValues`를 사용하는 값 기반 인덱스** — [텍스트 인덱스](/ko/engines/table-engines/mergetree-family/textindexes.md)를 사용해 모든 JSON 경로에 있는 *모든 값*을 인덱싱하여, 단일 인덱스로 모든 JSON 서브컬럼에 대한 전문 검색을 가속화합니다.

<div id="json-indexes-on-subcolumns">
  ### 특정 서브컬럼의 인덱스
</div>

일반 컬럼에 사용하는 것과 동일한 구문으로 모든 JSON 서브컬럼에 스킵 인덱스를 생성할 수 있습니다.
[지원되는 인덱스 유형](/ko/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)은 모두 사용할 수 있습니다(`minmax`, `set`, `bloom_filter`, `tokenbf_v1`, `ngrambf_v1` 등).

인덱스 표현식에서 JSON 서브컬럼을 참조하는 방법은 두 가지입니다:

* **JSON 타입 힌트**에 선언된 타입 지정 경로 — 이름으로 직접 접근합니다: `json.a`.
* **명시적 CAST가 있는 동적 경로** — `::` CAST 구문을 사용합니다: `json.b::String`.

예를 들어 `json.a || json.b::String`처럼 여러 서브컬럼을 결합한 표현식도 사용할 수 있습니다.

<div id="json-indexes-on-subcolumns-example">
  #### 예시
</div>

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id UInt32),
    INDEX idx_sensor data.sensor_id TYPE minmax GRANULARITY 1,
    INDEX idx_location data.location::String TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

유형이 지정된 서브컬럼 `data.sensor_id`에 대한 `minmax` 인덱스는 스캔 대상을 일치하는 그래뉼로 좁힙니다:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id < 2;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: minmax GRANULARITY 1
        Parts: 1/2
        Granules: 2/8
```

`data.location::String` CAST 서브컬럼에 대한 `bloom_filter` 인덱스도 동작합니다:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  ### JSONAllPaths를 사용한 경로 기반 인덱스
</div>

[`JSONAllPaths`](/ko/sql-reference/functions/json-functions#JSONAllPaths) 함수를 사용하면 `JSON` 컬럼에도 [데이터 스키핑 인덱스](/ko/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)를 생성할 수 있습니다.
이는 `mapKeys`를 사용해 [`Map`](/ko/sql-reference/data-types/map) 컬럼에 스킵 인덱스를 생성하는 방식과 유사합니다. 인덱스는 각 그래뉼에 존재하는 JSON 경로 집합을 저장하고, 이를 사용해 쿼리한 경로를 포함할 수 없는 그래뉼을 건너뜁니다.

<div id="json-indexes-jsonallpaths-supported-types">
  #### 지원되는 인덱스 유형
</div>

`JSONAllPaths`는 다음 스킵 인덱스 유형에서 사용할 수 있습니다:

* [`bloom_filter`](/ko/engines/table-engines/mergetree-family/mergetree#bloom-filter) — `equals`, `in`, `IS NOT NULL`을 지원합니다.
* [`tokenbf_v1`](/ko/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — `equals` 및 `IS NOT NULL`을 지원합니다.
* [`ngrambf_v1`](/ko/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — `equals` 및 `IS NOT NULL`을 지원합니다.
* [`text`](/ko/engines/table-engines/mergetree-family/textindexes) (역인덱스) — `equals`, `in`, `IS NOT NULL`을 지원합니다.

<div id="json-indexes-on-subcolumns-example">
  #### 예시
</div>

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

`EXPLAIN indexes = 1`을 사용하면 스킵 인덱스가 실제로 사용되는지 확인할 수 있습니다. 경로가 하나의 파트에만 존재하는 경우, 인덱스는 다른 파트는 건너뜁니다:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

어떤 파트에도 해당 경로가 없으면 모든 파트와 그래뉼을 건너뜁니다:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL`도 인덱스를 사용합니다 — 경로가 없는 그래뉼은 값이 `NULL`이므로 건너뜁니다:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallpaths-how-it-works">
  #### 작동 방식
</div>

`JSONAllPaths(json_column)` 표현식은 JSON 값에 있는 모든 경로를 포함하는 `Array(String)`를 생성합니다.
스킵 인덱스는 이러한 경로 문자열을 데이터 구조(블룸 필터 또는 inverted index)에 저장합니다.
쿼리에서 `json.some.path`로 필터링하면 인덱스는 각 그래뉼에 `"some.path"` 문자열이 있는지 확인하고, 없는 그래뉼은 건너뜁니다.

<div id="json-indexes-jsonallpaths-safety-with-missing-paths">
  #### 누락된 경로에 대한 안전성
</div>

JSON 경로가 그래뉼에 없으면 서브컬럼은 다음과 같이 평가됩니다.

* `Dynamic` 유형(예: `json.path`) 및 널 허용 `Nullable` 유형의 서브컬럼(예: `json.path.:Int64`)은 `NULL`로 평가됩니다 — `NULL`과의 비교는 항상 false를 반환하므로 스키핑은 안전합니다.
* 널 허용 `Nullable`이 아닌 CAST 표현식(예: `json.path::Int64`는 경로가 없으면 `0`을 생성함)은 해당 유형의 기본값으로 평가됩니다 — 스키핑은 비교 대상 값이 기본값과 다를 때만 안전합니다. 인덱스가 이 차이를 자동으로 처리합니다.

<div id="json-indexes-jsonallvalues">
  ### JSONAllValues를 사용한 전문 검색
</div>

[텍스트 인덱스](/ko/engines/table-engines/mergetree-family/textindexes.md)는 [`JSONAllValues`](/ko/sql-reference/functions/json-functions#JSONAllValues) 함수를 통해 JSON 컬럼의 전문 검색 성능을 높이는 데 사용할 수 있습니다.
`JSONAllValues`는 JSON 컬럼의 모든 값을 `Array(String)`으로 반환하며, 이 값들은 텍스트 인덱스로 인덱싱할 수 있습니다.
`JSONAllValues(json_column)`에 단일 인덱스 하나만 생성해도 모든 JSON 경로를 포괄하므로, 각 경로별로 별도 인덱스를 만들지 않고도 모든 서브컬럼에서 전문 검색을 수행할 수 있습니다.

자세한 설명과 예시는 텍스트 인덱스 문서의 [JSONAllValues를 사용한 값 기반 인덱스](/ko/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues)를 참조하십시오.

<div id="tips-for-better-usage-of-the-json-type">
  ## JSON 타입을 더 효과적으로 사용하기 위한 팁
</div>

`JSON` 컬럼을 생성하고 데이터를 로드하기 전에 다음 팁을 고려하십시오:

* 데이터를 충분히 살펴보고, 가능한 한 많은 경로 힌트와 해당 타입을 지정하십시오. 이렇게 하면 저장 및 읽기 효율이 크게 향상됩니다.
* 어떤 경로가 필요하고 어떤 경로는 전혀 필요하지 않은지 미리 생각해 보십시오. 필요하지 않은 경로는 `SKIP` 섹션에 지정하고, 필요한 경우 `SKIP REGEXP` 섹션에도 지정하십시오. 이렇게 하면 저장 효율이 향상됩니다.
* `max_dynamic_paths` 매개변수를 지나치게 큰 값으로 설정하지 마십시오. 저장 및 읽기 효율이 떨어질 수 있습니다.
  이는 메모리, CPU 등의 시스템 매개변수에 크게 좌우되지만, 일반적으로 로컬 파일 시스템 스토리지의 경우 `max_dynamic_paths`를 10 000보다 크게 설정하지 않고, 원격 파일 시스템 스토리지의 경우 1024보다 크게 설정하지 않는 것이 좋습니다.

<div id="further-reading">
  ## 더 읽어보기
</div>

* [ClickHouse용 강력한 새 JSON 데이터 타입을 구축한 방법](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
* [10억 문서 JSON 챌린지: ClickHouse vs. MongoDB, Elasticsearch 등](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)