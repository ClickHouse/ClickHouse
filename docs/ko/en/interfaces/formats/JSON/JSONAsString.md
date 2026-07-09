---
alias: []
description: 'JSONAsString 포맷에 대한 문서'
input_format: true
keywords: ['JSONAsString']
output_format: false
slug: /interfaces/formats/JSONAsString
title: 'JSONAsString'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 설명
</div>

이 포맷에서는 단일 JSON 객체를 하나의 값으로 해석합니다.
입력에 JSON 객체가 여러 개 있는 경우(쉼표로 구분됨) 각각을 별도의 행으로 해석합니다.
입력 데이터가 `[]`로 둘러싸여 있으면 JSON 객체 배열로 해석합니다.

:::note
이 포맷은 [String](/ko/sql-reference/data-types/string.md) 타입의 단일 필드가 있는 테이블에서만 파싱할 수 있습니다.
나머지 컬럼은 [`DEFAULT`](/ko/sql-reference/statements/create/table.md/#default) 또는 [`MATERIALIZED`](/ko/sql-reference/statements/create/view#materialized-view)로 설정하거나,
생략해야 합니다.
:::

전체 JSON 객체를 String으로 직렬화한 후에는 [JSON 함수](/ko/sql-reference/functions/json-functions.md)를 사용해 이를 처리할 수 있습니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="basic-example">
  ### 기본 예시
</div>

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

<div id="an-array-of-json-objects">
  ### JSON 객체의 배열
</div>

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

<div id="format-settings">
  ## 포맷 설정
</div>
