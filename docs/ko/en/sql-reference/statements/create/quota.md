---
description: 'QUOTA 문서'
sidebar_label: 'QUOTA'
sidebar_position: 42
slug: /sql-reference/statements/create/quota
title: 'CREATE QUOTA'
doc_type: 'reference'
---

사용자 또는 역할에 할당할 수 있는 [QUOTA](../../../guides/sre/user-management/index.md#quotas-management)를 생성합니다.

구문:

```sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | written_bytes | execution_time | failed_sequential_authentications | queries_per_normalized_hash} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

키 `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address`, `normalized_query_hash`는 [system.quotas](../../../operations/system-tables/quotas.md) 테이블의 필드에 해당합니다.

`IPV4_PREFIX_BITS` 및 `IPV6_PREFIX_BITS` 옵션은 `KEYED BY`가 `ip_address` 또는 `forwarded_ip_address`인 경우에만 사용할 수 있습니다. 이 옵션은 [system.quotas](../../../operations/system-tables/quotas.md) 테이블의 해당 필드에 대응합니다.

매개변수 `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `written_bytes`, `execution_time`, `failed_sequential_authentications`, `queries_per_normalized_hash`는 [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md) 테이블의 필드에 해당합니다.

`ON CLUSTER` 절을 사용하면 클러스터에 QUOTA를 생성할 수 있습니다. [분산 DDL](../../../sql-reference/distributed-ddl.md)을 참조하십시오.

**예시**

현재 사용자의 최대 쿼리 수를 15개월 동안 123회로 제한하는 조건:

```sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

`default` 사용자에 대해 30분 동안 최대 실행 시간을 0.5초로 제한하고, 5개 QUOTA 동안 최대 쿼리 수는 321개, 최대 오류 수는 10개로 제한합니다:

```sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

각 개별 정규화 쿼리 패턴에 자체 버킷이 할당되고 시간당 100회 실행으로 제한되도록 QUOTA를 생성합니다:

```sql
CREATE QUOTA qC KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO default;
```

개별 정규화된 쿼리 패턴은 시간당 최대 50회까지만 실행되도록 제한합니다(QUOTA 키 유형과 관계없이):

```sql
CREATE QUOTA qD FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 50 TO default;
```

XML 구성(ClickHouse Cloud에서는 지원되지 않음)을 사용하는 추가 예시는 [QUOTA 가이드](/ko/operations/quotas)에서 확인할 수 있습니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse로 단일 페이지 애플리케이션 만들기](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)