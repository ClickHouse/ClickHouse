---
description: 'QUALIFY 절 문서'
sidebar_label: 'QUALIFY'
slug: /sql-reference/statements/select/qualify
title: 'QUALIFY 절'
doc_type: '참고'
---

윈도우 함수 결과를 필터링할 수 있습니다. [WHERE](../../../sql-reference/statements/select/where.md) 절과 유사하지만, `WHERE`는 윈도우 함수가 평가되기 전에 적용되고 `QUALIFY`는 평가된 후에 적용된다는 점이 다릅니다.

`SELECT` 절의 윈도우 함수 결과는 해당 별칭(alias)을 사용해 `QUALIFY` 절에서 참조할 수 있습니다. 또한 `QUALIFY` 절에서는 쿼리 결과에 반환되지 않는 추가 윈도우 함수의 결과를 기준으로 필터링할 수도 있습니다.

<div id="limitations">
  ## 제한 사항
</div>

평가할 윈도우 함수가 없는 경우 `QUALIFY`는 사용할 수 없습니다. 대신 `WHERE`를 사용하십시오.

<div id="examples">
  ## 예시
</div>

예시:

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```