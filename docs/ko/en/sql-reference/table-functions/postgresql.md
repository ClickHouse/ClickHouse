---
description: '원격 PostgreSQL 서버에 저장된 데이터에 대해 `SELECT` 및 `INSERT` 쿼리를 실행할 수 있습니다.'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'reference'
---

원격 PostgreSQL 서버에 저장된 데이터에 대해 `SELECT` 및 `INSERT` 쿼리를 실행할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## 인수
</div>

| 인수            | 설명                                                                                 |
| ------------- | ---------------------------------------------------------------------------------- |
| `host:port`   | PostgreSQL 서버 주소입니다.                                                               |
| `database`    | 원격 데이터베이스 이름입니다.                                                                   |
| `table`       | 원격 테이블 이름 또는 PostgreSQL에 그대로 전달되는 쿼리입니다([테이블 이름 대신 쿼리 전달하기](#passing-a-query) 참조). |
| `user`        | PostgreSQL 사용자입니다.                                                                 |
| `password`    | 사용자 비밀번호입니다.                                                                       |
| `schema`      | 기본값이 아닌 테이블 스키마입니다. 선택 사항입니다.                                                      |
| `on_conflict` | 충돌 해결 전략입니다. 예시: `ON CONFLICT DO NOTHING`. 선택 사항입니다.                               |

인수는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용해 전달할 수도 있습니다. 이 경우 `host`와 `port`는 각각 지정해야 합니다. 이 방식은 프로덕션 환경에서 사용하는 것을 권장합니다.

<div id="returned_value">
  ## 반환 값
</div>

원본 PostgreSQL 테이블과 동일한 컬럼으로 구성된 테이블 객체입니다.

:::note
컬럼 이름 목록이 포함된 테이블 이름과 테이블 함수 `postgresql(...)`를 `INSERT` 쿼리에서 구분하려면 `FUNCTION` 또는 `TABLE FUNCTION` 키워드를 사용해야 합니다. 아래 예시를 참조하십시오.
:::

<div id="implementation-details">
  ## 구현 세부 사항
</div>

PostgreSQL 측의 `SELECT` 쿼리는 읽기 전용 PostgreSQL 트랜잭션 내에서 `COPY (SELECT ...) TO STDOUT` 형태로 실행되며, 각 `SELECT` 쿼리 후 커밋됩니다.

`=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`과 같은 단순한 `WHERE` 절은 PostgreSQL 서버에서 실행됩니다.

모든 조인, 집계, 정렬, `IN [ array ]` 조건, 그리고 `LIMIT` 샘플링 제약은 PostgreSQL 쿼리가 완료된 후에만 ClickHouse에서 실행됩니다.

<div id="passing-a-query">
  ## 테이블 이름 대신 쿼리 전달하기
</div>

테이블 이름 대신 세 번째 인수로 `SELECT` 쿼리를 사용할 수 있으며, 이 쿼리는 PostgreSQL에 그대로 전달됩니다. 결과 테이블의 구조는 쿼리 결과로부터 자동으로 추론됩니다. 쿼리는 서브쿼리로 작성하거나 `query` 함수로 감싸서 작성할 수 있습니다:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

이는 조인, 집계 또는 기타 처리를 PostgreSQL로 푸시다운하는 데 유용합니다. 이러한 테이블은 읽기 전용이므로 `INSERT`는 허용되지 않습니다. 동일한 구문은 [`PostgreSQL`](/ko/engines/table-engines/integrations/postgresql) 테이블 엔진에서도 지원됩니다.

:::note
`(SELECT ...)` 형식의 서브쿼리는 ClickHouse에서 파싱된 후 PostgreSQL 방언(PostgreSQL 식별자 인용 및 문자열 리터럴 이스케이프)으로 다시 직렬화되어 서버로 전송됩니다. 따라서 유효한 ClickHouse SQL이어야 합니다. ClickHouse가 파싱하지 않는 PostgreSQL 전용 구문을 전달하려면 `query('...')` 형식을 사용하십시오. 이 경우 텍스트가 PostgreSQL로 변경 없이 그대로 전송됩니다.

전달된 쿼리에는 바깥쪽 ClickHouse 쿼리의 `WHERE`, `LIMIT`, 집계 등이 **푸시다운되지 않으며**, 전체 쿼리 결과를 가져온 후 ClickHouse에서 적용됩니다. PostgreSQL에서 읽는 데이터를 제한하려면 필터를 전달된 쿼리 내부에 넣으십시오. [`external_table_strict_query = 1`](/ko/operations/settings/settings#external_table_strict_query)을 사용하면 푸시다운할 수 없는 바깥쪽 필터는 로컬에서 적용되는 대신 예외와 함께 거부됩니다.
:::

PostgreSQL 측의 `INSERT` 쿼리는 PostgreSQL 트랜잭션 내부에서 `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN`으로 실행되며, 각 `INSERT` statement 뒤에 자동 커밋됩니다.

PostgreSQL Array 타입은 ClickHouse 배열로 변환됩니다.

:::note
주의하십시오. PostgreSQL에서는 Integer[]와 같은 배열 데이터 타입 컬럼에 행마다 차원이 다른 배열이 포함될 수 있지만, ClickHouse에서는 모든 행에서 동일한 차원의 다차원 배열만 허용됩니다.
:::

여러 레플리카를 지원하며, `|`로 구분하여 나열해야 합니다. 예를 들면 다음과 같습니다:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

or

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

PostgreSQL 딕셔너리 소스의 레플리카 우선순위를 지원합니다. 맵에서 숫자가 클수록 우선순위는 낮아집니다. 가장 높은 우선순위는 `0`입니다.

<div id="examples">
  ## 예시
</div>

PostgreSQL 테이블:

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
(1 row)
```

일반 인수를 사용한 ClickHouse 데이터 선택:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

또는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용:

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

삽입:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

기본 스키마 외의 스키마 사용:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## 관련
</div>

* [PostgreSQL 테이블 엔진](../../engines/table-engines/integrations/postgresql.md)
* [PostgreSQL를 딕셔너리 소스로 사용하는 방법](/ko/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### PeerDB를 사용해 Postgres 데이터 복제 또는 마이그레이션하기
</div>

> 테이블 함수 외에도, Postgres에서 ClickHouse로 지속적인 데이터 파이프라인을 설정할 때는 ClickHouse의 [PeerDB](https://docs.peerdb.io/introduction)를 사용할 수 있습니다. PeerDB는 변경 데이터 캡처(CDC)를 사용해 Postgres에서 ClickHouse로 데이터를 복제하도록 특별히 설계된 도구입니다.