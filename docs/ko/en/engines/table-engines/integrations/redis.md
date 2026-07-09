---
description: '이 엔진을 사용하면 ClickHouse를 Redis와 통합할 수 있습니다.'
sidebar_label: 'Redis'
sidebar_position: 175
slug: /engines/table-engines/integrations/redis
title: 'Redis 테이블 엔진'
doc_type: 'guide'
---

이 엔진을 사용하면 ClickHouse를 [Redis](https://redis.io/)와 통합할 수 있습니다. Redis는 키-값(kv) 모델을 사용하므로, `where k=xx` 또는 `where k in (xx, xx)`처럼 포인트 조회 방식으로만 쿼리할 것을 강력히 권장합니다.

<div id="creating-a-table">
  ## 테이블(Table) 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**엔진 매개변수**

* `host:port` — Redis 서버 주소입니다. 포트는 생략할 수 있으며, 기본 Redis 포트 `6379`가 사용됩니다.
* `db_index` — Redis DB 인덱스는 0부터 15까지 지정할 수 있으며, 기본값은 0입니다.
* `password` — 사용자 비밀번호이며, 기본값은 빈 문자열입니다.
* `pool_size` — Redis 최대 연결 풀 크기이며, 기본값은 16입니다.
* `primary_key_name` - 컬럼 목록에 포함된 임의의 컬럼 이름입니다.

:::note 직렬화
`PRIMARY KEY`는 1개의 컬럼만 지원합니다. 프라이머리 키는 Redis 키로 바이너리 형식으로 직렬화됩니다.
프라이머리 키를 제외한 컬럼은 해당 순서대로 Redis 값으로 바이너리 형식으로 직렬화됩니다.
:::

인수는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용하여 전달할 수도 있습니다. 이 경우 `host`와 `port`는 별도로 지정해야 합니다. 이 방식은 운영 환경에 권장됩니다. 현재는 이름이 지정된 컬렉션을 사용해 Redis에 전달하는 모든 매개변수를 필수로 지정해야 합니다.

:::note 필터링
`key equals` 또는 `in` 필터링이 있는 쿼리는 Redis에서 여러 키를 조회하는 방식으로 최적화됩니다. 필터링 키 없이 쿼리하면 전체 테이블 스캔이 발생하며, 이는 부담이 큰 작업입니다.
:::

<div id="usage-example">
  ## 사용 예시
</div>

일반 인수를 사용하는 `Redis` 엔진으로 ClickHouse에 테이블을 생성합니다:

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

또는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용할 수 있습니다:

```xml
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>0</db_index>
    </redis_creds>
</named_collections>
```

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

삽입:

```sql title="Query"
INSERT INTO redis_table VALUES('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

```sql title="Query"
SELECT COUNT(*) FROM redis_table;
```

```text title="Response"
┌─count()─┐
│       2 │
└─────────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE key='1';
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE v1=2;
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

업데이트:

프라이머리 키는 수정할 수 없습니다.

```sql title="Query"
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

삭제:

```sql title="Query"
ALTER TABLE redis_table DELETE WHERE key='1';
```

TRUNCATE:

Redis DB를 비동기로 플러시합니다. 또한 `TRUNCATE`는 SYNC 모드도 지원합니다.

```sql title="Query"
TRUNCATE TABLE redis_table SYNC;
```

Join:

다른 테이블과 조인합니다.

```sql title="Query"
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

<div id="limitations">
  ## 제한 사항
</div>

Redis engine는 `where k > xx`와 같은 스캔 쿼리도 지원하지만, 몇 가지 제한 사항이 있습니다.

1. 스캔 쿼리는 리해싱 중인 매우 드문 경우 일부 중복 키를 반환할 수 있습니다. 자세한 내용은 [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269)을 참조하십시오.
2. 스캔 중에는 키가 생성되거나 삭제될 수 있으므로, 결과 데이터셋은 특정 시점의 유효한 상태를 나타내지 못할 수 있습니다.