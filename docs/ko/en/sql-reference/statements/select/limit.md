---
description: 'LIMIT 절에 대한 문서'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'LIMIT 절'
doc_type: '참고'
---

`LIMIT` 절은 쿼리 결과로 반환되는 행 수를 제어합니다.

<div id="basic-syntax">
  ## 기본 구문
</div>

**첫 몇 개 행 선택:**

```sql
LIMIT m
```

결과에서 첫 `m`개 행을 반환하며, `m`개 미만이면 모든 레코드를 반환합니다.

**대체 TOP 구문(MS SQL Server 호환):**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

이는 `LIMIT m`과 동일하며 Microsoft SQL Server 쿼리와의 호환성을 위해 사용할 수 있습니다.

**오프셋을 사용한 SELECT:**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

처음 `n`개 행을 건너뛴 후, 다음 `m`개 행을 반환합니다.

두 형식 모두 `n`과 `m`은 0 이상의 정수여야 합니다.

<div id="negative-limits">
  ## 음수 제한
</div>

음수 값을 사용해 결과 집합(result set)의 *끝*에서 행을 선택합니다:

| 구문                   | 결과                           |
| -------------------- | ---------------------------- |
| `LIMIT -m`           | 마지막 `m`개 행                   |
| `LIMIT -m OFFSET -n` | 마지막 `n`개 행을 건너뛴 후 마지막 `m`개 행 |
| `LIMIT m OFFSET -n`  | 마지막 `n`개 행을 건너뛴 후 처음 `m`개 행  |
| `LIMIT -m OFFSET n`  | 처음 `n`개 행을 건너뛴 후 마지막 `m`개 행  |

`LIMIT -n, -m` 구문은 `LIMIT -m OFFSET -n`과 동일합니다.

<div id="fractional-limits">
  ## 소수형 LIMIT
</div>

0과 1 사이의 소수 값을 사용해 행의 일정 비율을 선택합니다:

| 구문                      | 결과                                         |
| ----------------------- | ------------------------------------------ |
| `LIMIT 0.1`             | 처음 10%의 행                                  |
| `LIMIT 1 OFFSET 0.5`    | 중앙값에 해당하는 행                                |
| `LIMIT 0.25 OFFSET 0.5` | 세 번째 사분위수(처음 50%를 건너뛴 후 나머지에서 25%에 해당하는 행) |

:::note

* 소수는 0보다 크고 1보다 작은 [Float64](../../data-types/float.md) 값이어야 합니다.
* 소수로 계산된 행 수는 다음 정수로 올림됩니다.
  :::

<div id="combining-limit-types">
  ## LIMIT 유형 조합하기
</div>

표준 정수와 소수 또는 음수 오프셋을 함께 사용할 수 있습니다:

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

<div id="limit--with-ties-modifier">
  ## LIMIT ... WITH TIES
</div>

`WITH TIES` 수정자는 LIMIT의 마지막 행과 `ORDER BY` 값이 동일한 추가 행도 포함합니다.

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

`WITH TIES`를 사용하면 마지막 값과 같은 모든 행이 포함됩니다:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

6행은 5행과 같은 값(`2`)을 가지므로 포함됩니다.

`OFFSET` 키워드로 오프셋을 지정한 경우에도 동일하게 적용됩니다:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 3 OFFSET 2 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

처음 2개의 행을 건너뛰고 3개를 가져오면 일반적으로 `1, 1, 2`가 반환되지만, 두 번째 `2`는 마지막 행과 값이 같기 때문에 포함됩니다.

`WITH TIES`는 음수 제한과 offset에도 적용됩니다. 처음 선택된 행과 동일한 `ORDER BY` 값을 가진 추가 행도 포함합니다:

```sql
SELECT number % 3 AS n FROM numbers(15)
ORDER BY n LIMIT -4 OFFSET -3 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

`WITH TIES`가 없으면 결과는 `1, 1, 2, 2`입니다. `WITH TIES`를 사용하면 처음 선택된 행과 값이 같기 때문에 값이 `1`인 추가 행 3개가 더 포함됩니다.

이 수정자는 [`ORDER BY ... WITH FILL`](/ko/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier) 수정자와 함께 사용할 수 있습니다.

<div id="considerations">
  ## 고려 사항
</div>

**비결정적 결과:** [`ORDER BY`](../../../sql-reference/statements/select/order-by.md) 절이 없으면 반환되는 행이 임의로 결정될 수 있으며, 쿼리를 실행할 때마다 달라질 수 있습니다.

**서버 측 제한:** 반환되는 행 수는 [limit](../../../operations/settings/settings.md#limit) 설정의 영향을 받을 수도 있습니다.

<div id="see-also">
  ## 관련 항목
</div>

* [LIMIT BY](/ko/sql-reference/statements/select/limit-by) — 값 그룹별 행 수를 제한하며, 각 범주에서 상위 N개 결과를 가져오는 데 유용합니다.