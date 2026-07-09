---
description: 'ClickHouse의 UUID 데이터 타입에 대한 문서'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

UUID는 레코드를 식별하는 데 사용되는 16바이트 값인 Universally Unique Identifier입니다. UUID에 대한 자세한 내용은 [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier)를 참조하십시오.

UUIDv4 및 UUIDv7과 같은 다양한 UUID 변형이 존재하지만([여기](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis) 참조), ClickHouse는 삽입된 UUID가 특정 변형을 준수하는지 검증하지 않습니다.
UUID는 내부적으로 16개의 임의 바이트로 이루어진 시퀀스로 처리되며, SQL 수준에서는 [8-4-4-4-12 표현](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation)을 사용합니다.

예시 UUID 값:

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

기본 UUID 값은 모두 0입니다. 예를 들어 새 레코드가 삽입될 때 UUID 컬럼 값이 지정되지 않으면 이 값이 사용됩니다:

```text
00000000-0000-0000-0000-000000000000
```

:::warning
역사적인 이유로 UUID는 뒤쪽 절반을 기준으로 정렬됩니다.

이는 UUIDv4 값에서는 문제가 없지만, 프라이머리 인덱스 정의에 사용되는 UUIDv7 컬럼에서는 성능이 저하될 수 있습니다(순서 지정 키나 파티션 키에 사용하는 경우는 괜찮습니다).
좀 더 구체적으로 말하면, UUIDv7 값은 앞쪽 절반에 타임스탬프가 있고 뒤쪽 절반에 카운터가 있습니다.
따라서 희소 프라이머리 키 인덱스에서의 UUIDv7 정렬(즉, 각 index granule의 첫 번째 값)은 카운터 필드를 기준으로 이루어집니다.
UUID가 앞쪽 절반(타임스탬프)을 기준으로 정렬된다고 가정하면, 쿼리 시작 시 프라이머리 키 인덱스 분석 단계에서 하나를 제외한 모든 파트의 모든 마크를 프루닝할 것으로 예상됩니다.
하지만 뒤쪽 절반(카운터)을 기준으로 정렬하면, 모든 파트에서 최소 1개의 마크가 반환될 것으로 예상되며 그 결과 불필요한 디스크 접근이 발생합니다.
:::

예시:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

우회 방법으로, UUID를 뒷부분에서 추출한 타임스탬프로 변환할 수 있습니다:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

결과(동일한 데이터가 삽입되었다고 가정할 경우):

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## UUID 생성
</div>

ClickHouse는 무작위 UUID 버전 4 값을 생성하는 [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) 함수를 제공합니다.

<div id="usage-example">
  ## 사용 예시
</div>

**예시 1**

이 예시는 UUID 컬럼이 있는 테이블(table)을 생성하고 해당 테이블에 값을 삽입하는 과정을 보여줍니다.

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**예시 2**

이 예시에서는 레코드를 삽입할 때 UUID 컬럼 값을 지정하지 않으므로, 기본 UUID 값이 삽입됩니다:

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## 제약 사항
</div>

UUID 데이터 타입은 [String](../../sql-reference/data-types/string.md) 데이터 타입에서 지원되는 함수만 지원합니다(예: [min](/ko/sql-reference/aggregate-functions/reference/min), [max](/ko/sql-reference/aggregate-functions/reference/max), [count](/ko/sql-reference/aggregate-functions/reference/count)).

UUID 데이터 타입은 산술 연산(예: [abs](/ko/sql-reference/functions/arithmetic-functions#abs))이나 [sum](/ko/sql-reference/aggregate-functions/reference/sum), [avg](/ko/sql-reference/aggregate-functions/reference/avg)와 같은 집계 함수를 지원하지 않습니다.