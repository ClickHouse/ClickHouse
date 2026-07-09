---
description: '`anyLast`와 유사하게 마지막으로 나타난 값을 선택하지만, NULL도 허용할 수 있습니다.'
slug: /sql-reference/aggregate-functions/reference/last_value
title: 'last_value'
doc_type: 'reference'
---

`anyLast`와 유사하게 마지막으로 나타난 값을 선택하지만, NULL도 허용할 수 있습니다.
대부분 [윈도우 함수](../../window-functions/index.md)와 함께 사용합니다.
윈도우 함수를 사용하지 않으면 입력 스트림에 순서가 지정되지 않은 경우 결과가 무작위가 될 수 있습니다.

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

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null)
```

<div id="example1">
  ### 예시 1
</div>

NULL 값은 기본적으로 무시됩니다.

```sql
SELECT last_value(b) FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example2">
  ### 예시 2
</div>

NULL 값은 무시됩니다.

```sql
SELECT last_value(b) ignore nulls FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example3">
  ### 예시 3
</div>

NULL 값이 허용됩니다.

```sql
SELECT last_value(b) respect nulls FROM test_data
```

```text
┌─last_value_respect_nulls(b)─┐
│                        ᴺᵁᴸᴸ │
└─────────────────────────────┘
```

<div id="example4">
  ### 예시 4
</div>

`ORDER BY`를 사용한 하위 쿼리로 결과를 안정적으로 만듭니다.

```sql
SELECT
    last_value_respect_nulls(b),
    last_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─last_value_respect_nulls(b)─┬─last_value(b)─┐
│                        ᴺᵁᴸᴸ │             5 │
└─────────────────────────────┴───────────────┘
```