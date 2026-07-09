---
description: 'PREWHERE 절 문서'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'PREWHERE 절'
doc_type: 'reference'
---

Prewhere는 필터링을 더 효율적으로 적용하기 위한 최적화 기능입니다. `PREWHERE` 절을 명시적으로 지정하지 않아도 기본적으로 활성화됩니다. 이 기능은 [WHERE](../../../sql-reference/statements/select/where.md) 조건의 일부를 prewhere 단계로 자동으로 옮겨 처리하는 방식으로 동작합니다. `PREWHERE` 절의 역할은 기본 동작보다 더 적절하게 제어할 수 있다고 판단될 때만 이 최적화를 제어하는 것입니다.

prewhere 최적화를 사용하면 먼저 prewhere 표현식을 실행하는 데 필요한 컬럼만 읽습니다. 그런 다음 쿼리의 나머지 부분을 실행하는 데 필요한 다른 컬럼을 읽는데, 이때는 prewhere 표현식이 일부 행에서라도 `true`인 블록만 읽습니다. 모든 행에서 prewhere 표현식이 `false`인 블록이 많고, prewhere에 쿼리의 다른 부분보다 더 적은 컬럼만 필요하다면, 쿼리 실행 시 디스크에서 읽어야 하는 데이터 양을 크게 줄일 수 있는 경우가 많습니다.

<div id="controlling-prewhere-manually">
  ## PREWHERE 수동 제어
</div>

이 절은 `WHERE` 절과 같은 의미입니다. 차이점은 테이블에서 어떤 데이터를 읽느냐에 있습니다. 쿼리에서 일부 컬럼에만 사용되지만 데이터를 크게 걸러낼 수 있는 필터링 조건에 대해 `PREWHERE`를 수동으로 제어하면, 읽어야 하는 데이터 양을 줄일 수 있습니다.

하나의 쿼리에 `PREWHERE`와 `WHERE`를 동시에 지정할 수 있습니다. 이 경우 `PREWHERE`가 `WHERE`보다 먼저 적용됩니다.

[optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) 설정이 0이면, 표현식의 일부를 `WHERE`에서 `PREWHERE`로 자동 이동하는 휴리스틱이 비활성화됩니다.

쿼리에 [FINAL](/ko/sql-reference/statements/select/from#final-modifier) 수정자가 있으면 `PREWHERE` 최적화가 항상 올바르게 동작하지는 않습니다. 이 최적화는 [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) 및 [optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) 설정이 모두 활성화된 경우에만 적용됩니다.

:::note
`PREWHERE` 절은 `FINAL`보다 먼저 실행되므로, 테이블의 `ORDER BY` 절에 없는 필드를 `PREWHERE`와 함께 사용하면 `FROM ... FINAL` 쿼리의 결과가 왜곡될 수 있습니다.
:::

<div id="limitations">
  ## 제한 사항
</div>

`PREWHERE`는 [*MergeTree](../../../engines/table-engines/mergetree-family/index.md) 계열 테이블에서만 지원됩니다.

<div id="example">
  ## 예시
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```