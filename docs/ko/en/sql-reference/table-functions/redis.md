---
description: '이 테이블 함수는 ClickHouse를 [Redis](https://redis.io/)와 통합할 수 있도록 합니다.'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

이 테이블 함수는 ClickHouse를 [Redis](https://redis.io/)와 통합할 수 있도록 합니다.

<div id="syntax">
  ## 구문
</div>

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

<div id="arguments">
  ## 인수
</div>

| Argument    | Description                                                           |
| ----------- | --------------------------------------------------------------------- |
| `host:port` | Redis server 주소입니다. 포트는 생략할 수 있으며, 이 경우 기본 Redis 포트인 6379가 사용됩니다.     |
| `key`       | 컬럼 목록에 있는 임의의 컬럼 이름입니다.                                               |
| `structure` | 이 함수가 반환하는 ClickHouse 테이블의 스키마입니다.                                    |
| `db_index`  | Redis DB 인덱스 범위는 0~15이며, 기본값은 0입니다.                                   |
| `password`  | 사용자 비밀번호이며, 기본값은 빈 문자열입니다.                                            |
| `pool_size` | Redis의 최대 연결 풀 크기이며, 기본값은 16입니다.                                      |
| `primary`   | 반드시 지정해야 하며, 프라이머리 키는 하나의 컬럼만 지원합니다. 프라이머리 키는 Redis key로 바이너리 직렬화됩니다. |

* 프라이머리 키를 제외한 컬럼은 해당 순서대로 Redis value로 바이너리 직렬화됩니다.
* key에 대해 equals 또는 in 필터링이 있는 쿼리는 Redis의 여러 key lookup으로 최적화됩니다. key 필터링 없이 쿼리하면 전체 테이블 스캔이 발생하며, 이는 비용이 큰 작업입니다.

현재 `redis` 테이블 함수에서는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 지원하지 않습니다.

<div id="returned_value">
  ## 반환 값
</div>

key는 Redis 키이고, 나머지 컬럼은 함께 묶여 Redis 값이 되는 테이블 객체입니다.

<div id="usage-example">
  ## 사용 예시
</div>

Redis에서 읽기:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Redis에 삽입:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

<div id="related">
  ## 관련
</div>

* [`Redis` 테이블 엔진](/ko/engines/table-engines/integrations/redis.md)
* [Redis를 딕셔너리 소스로 사용하는 방법](/ko/sql-reference/statements/create/dictionary/sources/redis)