---
description: '원격 PostgreSQL 서버의 데이터베이스에 연결할 수 있습니다.'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

원격 [PostgreSQL](https://www.postgresql.org) 서버의 데이터베이스에 연결할 수 있습니다. ClickHouse와 PostgreSQL 간에 데이터를 교환하기 위한 읽기 및 쓰기 작업(`SELECT` 및 `INSERT` 쿼리)을 지원합니다.

`SHOW TABLES` 및 `DESCRIBE TABLE` 쿼리를 사용해 원격 PostgreSQL의 테이블 목록과 테이블 구조를 실시간으로 확인할 수 있습니다.

테이블 구조 수정(`ALTER TABLE ... ADD|DROP COLUMN`)도 지원합니다. `use_table_cache` 매개변수(아래 엔진 매개변수 참조)를 `1`로 설정하면 테이블 구조가 캐시되어 수정 여부를 확인하지 않지만, `DETACH` 및 `ATTACH` 쿼리로 업데이트할 수 있습니다.

<div id="creating-a-database">
  ## 데이터베이스 생성
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**엔진 매개변수**

* `host:port` — PostgreSQL 서버 주소입니다.
* `database` — 원격 데이터베이스 이름입니다.
* `user` — PostgreSQL 사용자입니다.
* `password` — 사용자 비밀번호입니다.
* `schema` — PostgreSQL 스키마입니다.
* `use_table_cache` — 데이터베이스 테이블 구조를 캐시할지 여부를 지정합니다. 선택 사항입니다. 기본값은 `0`입니다.

<div id="data_types-support">
  ## 데이터 타입 지원
</div>

| PostgreSQL       | ClickHouse                                                         |
| ---------------- | ------------------------------------------------------------------ |
| DATE             | [Date](../../sql-reference/data-types/date.md)                     |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)             |
| REAL             | [Float32](../../sql-reference/data-types/float.md)                 |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)                 |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (아래 참고 사항 참조) |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)                |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)                |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)                |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)               |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)               |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)                 |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))      |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)                   |

:::note
정밀도 `p`가 76(`Decimal256`이 지원하는 최대값)보다 큰 PostgreSQL `numeric(p, 0)`(예: 일반적으로 256비트 정수 저장에 사용되는 `numeric(78, 0)`)은 `Decimal` 대신 [`Int256`](../../sql-reference/data-types/int-uint.md)에 매핑됩니다. `Int256` 범위에 들어가지 않는 값은 오류와 함께 거부됩니다.
:::

<div id="examples-of-use">
  ## 사용 예시
</div>

PostgreSQL 서버와 데이터를 주고받는 ClickHouse 데이터베이스:

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

PostgreSQL 테이블에서 데이터 읽기:

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

PostgreSQL 테이블에 데이터 쓰기:

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

PostgreSQL에서 테이블 구조가 변경된 경우를 살펴보겠습니다:

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

데이터베이스를 생성할 때 `use_table_cache` 매개변수가 `1`로 설정되었으므로 ClickHouse의 테이블 구조가 캐시되어 수정되지 않았습니다:

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

테이블을 DETACH한 후 다시 ATTACH하면 구조가 업데이트되었습니다:

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse와 PostgreSQL - 데이터 분야의 완벽한 조합 - 1부](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* 블로그: [ClickHouse와 PostgreSQL - 데이터 분야의 완벽한 조합 - 2부](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)