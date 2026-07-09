---
description: 'HAVING 절에 대한 문서'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'HAVING 절'
doc_type: 'reference'
---

[GROUP BY](/ko/sql-reference/statements/select/group-by)에서 생성된 집계 결과를 필터링할 수 있습니다. [WHERE](../../../sql-reference/statements/select/where.md) 절과 비슷하지만, `WHERE`는 집계 전에 적용되고 `HAVING`은 집계 후에 적용된다는 점이 다릅니다.

`SELECT` 절의 집계 결과는 별칭을 통해 `HAVING` 절에서 참조할 수 있습니다. 또는 `HAVING` 절은 쿼리 결과로 반환되지 않는 추가 집계 결과를 기준으로 필터링할 수도 있습니다.

<div id="example">
  ## 예시
</div>

다음과 같은 `sales` 테이블이 있는 경우:

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

다음과 같이 쿼리할 수 있습니다:

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

이렇게 하면 해당 지역에서 총매출이 10,000을 초과한 영업 담당자 목록이 표시됩니다.

<div id="limitations">
  ## 제한 사항
</div>

집계를 수행하지 않는 경우 `HAVING`은 사용할 수 없습니다. 대신 `WHERE`를 사용하십시오.