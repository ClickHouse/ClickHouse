---
description: '테이블 함수 `remote`를 사용하면 [분산](../../engines/table-engines/special/distributed.md) 테이블을 생성하지 않고도
  원격 서버에 동적으로 액세스할 수 있습니다. 테이블 함수 `remoteSecure`는 `remote`와 동일하지만
  보안 연결을 사용합니다.'
sidebar_label: 'remote'
sidebar_position: 175
slug: /sql-reference/table-functions/remote
title: 'remote, remoteSecure'
doc_type: 'reference'
---

테이블 함수 `remote`를 사용하면 [분산](../../engines/table-engines/special/distributed.md) 테이블을 생성하지 않고도 원격 서버에 동적으로 액세스할 수 있습니다. 테이블 함수 `remoteSecure`는 `remote`와 동일하지만 보안 연결을 사용합니다.

두 함수 모두 `SELECT` 및 `INSERT` 쿼리에서 사용할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

<div id="parameters">
  ## 매개변수
</div>

| 인수               | 설명                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `addresses_expr` | 원격 서버 주소 또는 여러 원격 서버 주소를 생성하는 표현식입니다. 포맷: `host` 또는 `host:port`.<br /><br />    `host`는 서버 이름이나 IPv4 또는 IPv6 주소로 지정할 수 있습니다. IPv6 주소는 `[]`로 지정해야 합니다.<br /><br />    `port`는 원격 서버의 TCP 포트입니다. 포트를 생략하면 테이블 함수 `remote`에는 서버 구성 파일의 [tcp&#95;port](../../operations/server-configuration-parameters/settings.md#tcp_port)(기본값 9000)를 사용하고, 테이블 함수 `remoteSecure`에는 [tcp&#95;port&#95;secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure)(기본값 9440)를 사용합니다.<br /><br />    IPv6 주소에는 포트 지정이 필요합니다.<br /><br />    매개변수 `addresses_expr`만 지정하면 `db`와 `table`은 기본적으로 `system.one`을 사용합니다.<br /><br />    유형: [String](../../sql-reference/data-types/string.md). |
| `db`             | 데이터베이스 이름입니다. 유형: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `table`          | 테이블 이름입니다. 유형: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `user`           | 사용자 이름입니다. 지정하지 않으면 `default`를 사용합니다. 유형: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `password`       | 사용자 비밀번호입니다. 지정하지 않으면 빈 비밀번호를 사용합니다. 유형: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `sharding_key`   | 데이터를 노드 간에 분산하기 위한 샤딩 키입니다. 예시: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. 유형: [UInt32](../../sql-reference/data-types/int-uint.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

인수는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용해 전달할 수도 있습니다.

<div id="returned-value">
  ## 반환 값
</div>

원격 서버에 있는 테이블입니다.

<div id="usage">
  ## 사용
</div>

테이블 함수 `remote` 및 `remoteSecure`는 각 요청마다 연결을 다시 설정하므로, 대신 `Distributed` 테이블을 사용하는 것이 좋습니다. 또한 호스트명이 설정되어 있으면 이름이 확인되며, 여러 레플리카로 작업할 때 오류가 집계되지 않습니다. 많은 수의 쿼리를 처리할 때는 항상 `Distributed` 테이블을 미리 생성하고, `remote` 테이블 함수는 사용하지 마십시오.

`remote` 테이블 함수는 다음과 같은 경우에 유용할 수 있습니다:

* 한 시스템에서 다른 시스템으로 데이터를 일회성으로 마이그레이션하는 경우
* 데이터 비교, 디버깅, 테스트를 위해 특정 서버에 접근하는 경우, 즉 애드혹 연결
* 조사 목적으로 여러 ClickHouse 클러스터 간에 수행하는 쿼리
* 수동으로 수행하는 드문 분산 요청
* 서버 집합을 매번 다시 정의하는 분산 요청

<div id="addresses">
  ### 주소
</div>

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

여러 주소는 쉼표로 구분해 나열할 수 있습니다. 이 경우 ClickHouse는 분산 처리를 사용하여 지정된 모든 주소로 쿼리를 전송합니다(서로 다른 데이터를 가진 세그먼트와 비슷함). 예시:

```text
example01-01-1,example01-02-1
```

<div id="examples">
  ## 예시
</div>

<div id="selecting-data-from-a-remote-server">
  ### 원격 서버에서 데이터 조회:
</div>

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

또는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용합니다:

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

<div id="inserting-data-into-a-table-on-a-remote-server">
  ### 원격 서버의 테이블에 데이터 삽입하기:
</div>

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

<div id="migration-of-tables-from-one-system-to-another">
  ### 한 시스템에서 다른 시스템으로 테이블을 마이그레이션하기:
</div>

이 예시에서는 샘플 데이터셋의 테이블 하나를 사용합니다. 데이터베이스는 `imdb`이고 테이블은 `actors`입니다.

<div id="on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data">
  #### 원본 ClickHouse 시스템에서 (현재 데이터가 저장된 시스템)
</div>

* 원본 데이터베이스와 테이블 이름(`imdb.actors`)을 확인합니다

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

* 원본에서 CREATE TABLE 문을 확인합니다:

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
```

결과

```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
```

<div id="on-the-destination-clickhouse-system">
  #### 대상 ClickHouse 시스템에서
</div>

* 대상 데이터베이스를 생성합니다:

  ```sql
  CREATE DATABASE imdb
  ```

* 원본의 CREATE TABLE 문을 사용해 대상 테이블을 생성합니다:

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

<div id="back-on-the-source-deployment">
  #### 원본 배포로 돌아가기
</div>

원격 시스템에 생성한 새 데이터베이스(database)와 테이블(table)에 데이터를 삽입하세요. host, port, username, password, 대상 데이터베이스, 대상 테이블이 필요합니다.

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

<div id="globs-in-addresses">
  ## 글로빙
</div>

`{ }` 안의 패턴은 세그먼트 집합을 생성하고 레플리카를 지정하는 데 사용됩니다. `{ }` 쌍이 여러 개 있으면 해당 집합의 데카르트 곱이 생성됩니다.

다음 패턴 유형을 지원합니다.

* `{a,b,c}` - 대체 문자열 `a`, `b`, `c` 중 하나를 나타냅니다. 이 패턴은 첫 번째 세그먼트 주소에서는 `a`로, 두 번째 세그먼트 주소에서는 `b`로 대체되는 방식으로 적용됩니다. 예를 들어 `example0{1,2}-1`은 주소 `example01-1`과 `example02-1`을 생성합니다.
* `{N..M}` - 숫자 범위입니다. 이 패턴은 `N`부터 `M`까지(양 끝값 포함) 인덱스가 증가하는 세그먼트 주소를 생성합니다. 예를 들어 `example0{1..2}-1`은 `example01-1`과 `example02-1`을 생성합니다.
* `{0n..0m}` - 앞에 0이 붙는 숫자 범위입니다. 이 패턴은 인덱스의 앞자리 0을 유지합니다. 예를 들어 `example{01..03}-1`은 `example01-1`, `example02-1`, `example03-1`을 생성합니다.
* `{a|b}` - `|`로 구분된 여러 변형 중 하나입니다. 이 패턴은 레플리카를 지정합니다. 예를 들어 `example01-{1|2}`는 레플리카 `example01-1`과 `example01-2`를 생성합니다.

쿼리는 정상 상태인 첫 번째 레플리카로 전송됩니다. 하지만 `remote`의 경우 레플리카는 현재 [load&#95;balancing](../../operations/settings/settings.md#load_balancing) 설정에 지정된 순서대로 순회됩니다.
생성되는 주소 수는 [table&#95;function&#95;remote&#95;max&#95;addresses](../../operations/settings/settings.md#table_function_remote_max_addresses) 설정으로 제한됩니다.