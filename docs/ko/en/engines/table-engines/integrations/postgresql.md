---
description: 'PostgreSQL 테이블 엔진은 원격 PostgreSQL 서버에 저장된 데이터에 대해 `SELECT` 및 `INSERT` 쿼리를 지원합니다.'
sidebar_label: 'PostgreSQL'
sidebar_position: 160
slug: /engines/table-engines/integrations/postgresql
title: 'PostgreSQL 테이블 엔진'
doc_type: 'guide'
---

PostgreSQL 테이블 엔진은 원격 PostgreSQL 서버에 저장된 데이터에 대해 `SELECT` 및 `INSERT` 쿼리를 지원합니다.

:::note
현재 이 테이블 엔진은 PostgreSQL 12 이상 버전만 지원합니다.
:::

:::tip
[Managed Postgres](/ko/docs/cloud/managed-postgres) 서비스를 확인해 보십시오. 컴퓨트와 물리적으로 함께 배치된 NVMe 스토리지를 기반으로 하므로, EBS와 같은 네트워크 연결 스토리지를 사용하는 대안과 비교할 때 디스크 입출력에 병목이 있는 워크로드에서 최대 10배 더 빠른 성능을 제공하며, ClickPipes의 Postgres CDC 커넥터를 사용해 Postgres 데이터를 ClickHouse로 복제할 수 있습니다.
:::

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

[CREATE TABLE](/ko/sql-reference/statements/create/table) 쿼리에 대한 자세한 설명은 해당 문서를 참조하십시오.

테이블 구조는 원본 PostgreSQL 테이블 구조와 다를 수 있습니다:

* 컬럼 이름은 원본 PostgreSQL 테이블과 동일해야 하지만, 해당 컬럼 중 일부만 사용해도 되며 순서도 자유롭게 지정할 수 있습니다.
* 컬럼 타입은 원본 PostgreSQL 테이블과 다를 수 있습니다. ClickHouse는 값을 ClickHouse 데이터 타입으로 [cast](../../../engines/database-engines/postgresql.md#data_types-support)하려고 시도합니다.
* [external&#95;table&#95;functions&#95;use&#95;nulls](/ko/operations/settings/settings#external_table_functions_use_nulls) 설정은 널 허용 컬럼을 처리하는 방식을 정의합니다. 기본값은 1입니다. 값이 0이면 테이블 함수는 널 허용 컬럼을 생성하지 않고 null 대신 기본값을 삽입합니다. 이는 배열 내부의 NULL 값에도 적용됩니다.

**엔진 매개변수**

* `host:port` — PostgreSQL 서버 주소입니다.
* `database` — 원격 데이터베이스 이름입니다.
* `table` — 원격 테이블 이름 또는 PostgreSQL에 그대로 전달되는 쿼리입니다([Passing a query instead of a table name](#passing-a-query) 참조).
* `user` — PostgreSQL 사용자입니다.
* `password` — 사용자 비밀번호입니다.
* `schema` — 기본이 아닌 테이블 스키마입니다. 선택 사항입니다.
* `on_conflict` — 충돌 해결 전략입니다. 예시: `ON CONFLICT DO NOTHING`. 선택 사항입니다. 참고: 이 옵션을 추가하면 삽입 효율이 떨어집니다.

[이름이 지정된 컬렉션](/ko/operations/named-collections.md)(21.11 버전부터 사용 가능)은 운영 환경에 사용하는 것을 권장합니다. 다음은 예시입니다:

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

일부 매개변수는 키 값 인수로 재정의할 수 있습니다:

```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

<div id="implementation-details">
  ## 구현 세부 정보
</div>

PostgreSQL 쪽의 `SELECT` 쿼리는 각 `SELECT` 쿼리 뒤에 커밋이 수행되는 읽기 전용 PostgreSQL 트랜잭션 내에서 `COPY (SELECT ...) TO STDOUT` 형태로 실행됩니다.

`=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`과 같은 단순한 `WHERE` 절은 PostgreSQL 서버에서 실행됩니다.

모든 조인, 집계, 정렬, `IN [ array ]` 조건, 그리고 `LIMIT` 샘플링 제약 조건은 PostgreSQL 쿼리가 완료된 후에만 ClickHouse에서 실행됩니다.

<div id="passing-a-query">
  ## 테이블 이름 대신 쿌리 전달하기
</div>

테이블 이름 대신 `table` 인수에 PostgreSQL로 있는 그대로 전달되는 `SELECT` 쿼리를 사용할 수 있습니다. 테이블의 구조는 쿼리 결과를 바탕으로 추론됩니다. 쿼리는 서브쿼리로 작성하거나 `query` 함수로 감싸서 작성할 수 있습니다.

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

조인, 집계 또는 기타 처리를 PostgreSQL로 푸시다운하는 데 유용합니다. 이러한 테이블은 읽기 전용이므로 `INSERT`는 허용되지 않습니다. 동일한 구문은 [`postgresql`](/ko/sql-reference/table-functions/postgresql) 테이블 함수에서도 지원됩니다.

:::note
하위 쿼리 형식 `(SELECT ...)`은 ClickHouse에서 파싱한 후 서버로 전송되기 전에 PostgreSQL 방언(PostgreSQL 식별자 인용 및 문자열 리터럴 이스케이프)으로 다시 직렬화됩니다. 따라서 유효한 ClickHouse SQL이어야 합니다. ClickHouse가 파싱하지 않는 PostgreSQL 전용 구문을 전달하려면 텍스트가 PostgreSQL로 그대로 전송되는 `query('...')` 형식을 사용하십시오.

전달된 쿼리 바깥에 있는 ClickHouse 쿼리의 `WHERE`, `LIMIT`, 집계 등은 **푸시다운되지 않으며**, 전체 쿼리 결과를 가져온 후 ClickHouse에서 적용됩니다. PostgreSQL에서 읽는 데이터를 제한하려면 필터를 전달하는 쿼리 내부에 넣으십시오. [`external_table_strict_query = 1`](/ko/operations/settings/settings#external_table_strict_query)을 사용하면 푸시다운할 수 없는 외부 필터는 로컬에서 적용되는 대신 예외와 함께 거부됩니다.
:::

PostgreSQL 측의 `INSERT` 쿼리는 각 `INSERT` statement 후 자동 커밋되는 PostgreSQL 트랜잭션 내부에서 `COPY \"table_name\" (field1, field2, ... fieldN) FROM STDIN`으로 실행됩니다.

PostgreSQL `Array` 타입은 ClickHouse 배열로 변환됩니다.

:::note
주의하십시오. PostgreSQL에서는 `type_name[]`처럼 생성된 배열 데이터에 동일한 컬럼의 서로 다른 테이블 행마다 차원이 다른 다차원 배열이 포함될 수 있습니다. 그러나 ClickHouse에서는 동일한 컬럼의 모든 테이블 행에서 다차원 배열의 차원 수가 같아야만 합니다.
:::

여러 레플리카를 지원하며 `|`로 나열해야 합니다. 예를 들면 다음과 같습니다.

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

PostgreSQL 딕셔너리 소스는 레플리카 우선순위를 지원합니다. 맵의 숫자가 클수록 우선순위는 낮아집니다. 가장 높은 우선순위는 `0`입니다.

아래 예시에서는 레플리카 `example01-1`의 우선순위가 가장 높습니다:

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

<div id="usage-example">
  ## 사용 예시
</div>

<div id="table-in-postgresql">
  ### PostgreSQL 테이블
</div>

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

<div id="creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above">
  ### ClickHouse에서 테이블을 생성하고 위에서 만든 PostgreSQL 테이블에 연결하기
</div>

이 예시에서는 [PostgreSQL 테이블 엔진](/ko/engines/table-engines/integrations/postgresql.md)을 사용해 ClickHouse 테이블을 PostgreSQL 테이블에 연결하고, PostgreSQL 데이터베이스에 대해 SELECT와 INSERT SQL 문을 모두 사용합니다:

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query">
  ### SELECT 쿼리를 사용해 PostgreSQL 테이블의 초기 데이터를 ClickHouse 테이블에 삽입하기
</div>

[PostgreSQL 테이블 함수](/ko/sql-reference/table-functions/postgresql.md)는 PostgreSQL의 데이터를 ClickHouse로 복사합니다. 이는 PostgreSQL 대신 ClickHouse에서 데이터를 쿼리하거나 분석해 쿼리 성능을 높일 때 자주 사용되며, PostgreSQL에서 ClickHouse로 데이터를 마이그레이션하는 데에도 사용할 수 있습니다. 여기서는 PostgreSQL의 데이터를 ClickHouse로 복사할 것이므로, ClickHouse에서 MergeTree 테이블 엔진을 사용하고 이름은 postgresql&#95;copy로 지정합니다:

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-incremental-data-from-postgresql-table-into-clickhouse-table">
  ### PostgreSQL 테이블의 증분 데이터를 ClickHouse 테이블에 삽입하기
</div>

초기 삽입 후 PostgreSQL 테이블과 ClickHouse 테이블 간 동기화를 계속 수행하려면, ClickHouse에서 WHERE 절을 사용해 timestamp 또는 고유 시퀀스 ID를 기준으로 PostgreSQL에 새로 추가된 데이터만 삽입할 수 있습니다.

이를 위해서는 다음과 같이 이전에 추가한 최대 ID 또는 timestamp를 추적해야 합니다:

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

그런 다음 PostgreSQL 테이블에서 최댓값을 초과하는 값을 삽입

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

<div id="selecting-data-from-the-resulting-clickhouse-table">
  ### 생성된 ClickHouse 테이블에서 데이터 조회
</div>

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

<div id="using-non-default-schema">
  ### 기본 스키마가 아닌 스키마 사용
</div>

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**관련 항목**

* [`postgresql` 테이블 함수](../../../sql-reference/table-functions/postgresql.md)
* [PostgreSQL를 딕셔너리 소스로 사용](/ko/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse와 PostgreSQL - 데이터 세계의 환상적인 조합 - 1부](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* 블로그: [ClickHouse와 PostgreSQL - 데이터 세계의 환상적인 조합 - 2부](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)