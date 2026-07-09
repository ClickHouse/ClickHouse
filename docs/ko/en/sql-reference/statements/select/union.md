---
description: 'UNION 절 문서'
sidebar_label: 'UNION'
slug: /sql-reference/statements/select/union
title: 'UNION 절'
doc_type: 'reference'
---

`UNION`은 `UNION ALL` 또는 `UNION DISTINCT`를 명시적으로 지정해 사용할 수 있습니다.

`ALL` 또는 `DISTINCT`를 지정하지 않으면 `union_default_mode` 설정에 따라 동작이 달라집니다. `UNION ALL`과 `UNION DISTINCT`의 차이점은 `UNION DISTINCT`가 union 결과에 대해 중복 제거를 수행한다는 점입니다. 이는 `UNION ALL`이 포함된 하위 쿼리에 `SELECT DISTINCT`를 적용하는 것과 동일합니다.

`UNION`을 사용하면 여러 `SELECT` 쿼리의 결과를 이어 붙여 결합할 수 있습니다. 예시:

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

결과 컬럼은 인덱스(`SELECT` 내 순서)를 기준으로 매칭됩니다. 컬럼 이름이 일치하지 않으면 최종 결과의 이름은 첫 번째 쿼리의 이름을 따릅니다.

`UNION`에서는 타입 캐스팅이 수행됩니다. 예를 들어, 결합되는 두 쿼리에 호환 가능한 타입의 동일한 필드가 있고 하나는 non-`Nullable`, 다른 하나는 `Nullable` 타입인 경우, 결과 `UNION`의 해당 필드는 `Nullable` 타입이 됩니다.

`UNION`의 일부인 쿼리는 `()`로 감쌀 수 있습니다. [ORDER BY](../../../sql-reference/statements/select/order-by.md) 및 [LIMIT](../../../sql-reference/statements/select/limit.md)는 최종 결과가 아니라 각각의 개별 쿼리에 적용됩니다. 최종 결과에 변환을 적용해야 한다면, `UNION`이 포함된 모든 쿼리를 [FROM](../../../sql-reference/statements/select/from.md) 절의 하위 쿼리로 넣을 수 있습니다.

`UNION ALL` 또는 `UNION DISTINCT`를 명시적으로 지정하지 않고 `UNION`을 사용하는 경우, [union&#95;default&#95;mode](/ko/operations/settings/settings#union_default_mode) 설정을 사용해 union mode를 지정할 수 있습니다. 설정 값은 `ALL`, `DISTINCT` 또는 빈 문자열일 수 있습니다. 하지만 `union_default_mode` 설정이 빈 문자열인 상태에서 `UNION`을 사용하면 예외가 발생합니다. 다음 예시는 설정 값에 따라 쿼리 결과가 어떻게 달라지는지 보여줍니다.

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

`UNION/UNION ALL/UNION DISTINCT`의 일부인 쿼리는 동시에 실행될 수 있으며, 그 결과는 서로 뒤섞여 반환될 수 있습니다.

**관련 항목**

* [insert&#95;null&#95;as&#95;default](../../../operations/settings/settings.md#insert_null_as_default) 설정.
* [union&#95;default&#95;mode](/ko/operations/settings/settings#union_default_mode) 설정.