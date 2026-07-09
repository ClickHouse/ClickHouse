---
description: 'QUOTA 문서'
sidebar_label: 'QUOTA'
sidebar_position: 46
slug: /sql-reference/statements/alter/quota
title: 'ALTER QUOTA'
doc_type: 'reference'
---

쿼터를 변경합니다.

구문:

```sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time | queries_per_normalized_hash} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

키 `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address`, `normalized_query_hash`는 [system.quotas](../../../operations/system-tables/quotas.md) 테이블의 필드에 해당합니다.

`IPV4_PREFIX_BITS` 및 `IPV6_PREFIX_BITS` 옵션은 `KEYED BY`가 `ip_address` 또는 `forwarded_ip_address`인 경우에만 사용할 수 있습니다. 이 옵션들은 [system.quotas](../../../operations/system-tables/quotas.md) 테이블의 필드에 해당합니다.

매개변수 `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `execution_time`, `queries_per_normalized_hash`는 [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md) 테이블의 필드에 해당합니다.

`ON CLUSTER` 절을 사용하면 클러스터에 쿼터를 생성할 수 있습니다. [Distributed DDL](../../../sql-reference/distributed-ddl.md)을 참조하십시오.

**예시**

현재 사용자의 최대 쿼리 수를 15개월 동안 123개로 제한하는 제약 조건:

```sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

`default` 사용자의 경우, 30분 동안의 최대 실행 시간을 0.5초로 제한하고, 5개의 15분 구간 동안 최대 쿼리 수는 321개로, 최대 오류 수는 10개로 제한합니다:

```sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```