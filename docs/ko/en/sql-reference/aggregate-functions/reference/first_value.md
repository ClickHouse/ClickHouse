---
description: '[`any`](../../../sql-reference/aggregate-functions/reference/any.md)의 별칭이지만,
  [윈도우 함수](../../window-functions/index.md)와의 호환성을 위해 도입되었습니다. 윈도우 함수에서는
  때때로 `NULL` 값을 처리해야 하기 때문입니다(기본적으로 모든 ClickHouse 집계 함수는 NULL 값을 무시합니다).'
slug: /sql-reference/aggregate-functions/reference/first_value
title: 'first_value'
doc_type: 'reference'
---

[`any`](../../../sql-reference/aggregate-functions/reference/any.md)의 별칭이지만, [윈도우 함수](../../window-functions/index.md)와의 호환성을 위해 도입되었습니다. 윈도우 함수에서는 때때로 `NULL` 값을 처리해야 하기 때문입니다(기본적으로 모든 ClickHouse 집계 함수는 NULL 값을 무시합니다).

[윈도우 함수](../../window-functions/index.md)와 일반 집계 모두에서 `NULL`을 유지하는 수정자(`RESPECT NULLS`)를 선언할 수 있습니다.

`any`와 마찬가지로, 윈도우 함수 없이 사용할 경우 소스 스트림에 순서가 지정되어 있지 않으면 결과는 무작위가 되며 반환 유형은 입력 유형과 일치합니다(`Null`은 입력이 널 허용이거나 `-OrNull` combinator가 추가된 경우에만 반환됩니다).

<div id="examples">
  ## 예시
</div>

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null);
```

<div id="example1">
  ### 예시 1
</div>

기본적으로는 NULL 값이 무시됩니다.

```sql
SELECT first_value(b) FROM test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

<div id="example2">
  ### 예시 2
</div>

NULL 값은 무시됩니다.

```sql
SELECT first_value(b) ignore nulls FROM test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

<div id="example3">
  ### 예시 3
</div>

NULL 값이 허용됩니다.

```sql
SELECT first_value(b) respect nulls FROM test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

<div id="example4">
  ### 예시 4
</div>

`ORDER BY`가 포함된 하위 쿼리를 사용해 안정화한 결과입니다.

```sql
SELECT
    first_value_respect_nulls(b),
    first_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─any_respect_nulls(b)─┬─any(b)─┐
│                 ᴺᵁᴸᴸ │      3 │
└──────────────────────┴────────┘
```