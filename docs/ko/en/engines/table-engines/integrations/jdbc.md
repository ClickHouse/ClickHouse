---
description: 'ClickHouse가 JDBC를 통해 외부 데이터베이스에 연결할 수 있게 합니다.'
sidebar_label: 'JDBC'
sidebar_position: 100
slug: /engines/table-engines/integrations/jdbc
title: 'JDBC 테이블 엔진'
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="jdbc-table-engine">
  # JDBC 테이블 엔진
</div>

<CloudNotSupportedBadge />

:::note
clickhouse-jdbc-bridge에는 Experimental 코드가 포함되어 있으며 더 이상 지원되지 않습니다. 신뢰성 문제와 보안 취약점이 있을 수 있습니다. 사용에 따른 위험은 사용자 책임입니다.
ClickHouse는 애드혹 쿼리 시나리오(Postgres, MySQL, MongoDB 등)에서 더 나은 대안을 제공하는 ClickHouse 내장 테이블 함수를 사용할 것을 권장합니다.
:::

ClickHouse가 [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity)를 통해 외부 데이터베이스에 연결할 수 있도록 합니다.

JDBC 연결을 구현하기 위해 ClickHouse는 데몬으로 실행해야 하는 별도의 프로그램인 [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge)를 사용합니다.

이 엔진은 [널 허용](../../../sql-reference/data-types/nullable.md) 데이터 타입을 지원합니다.

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource, external_database, external_table)
```

**엔진 매개변수**

* `datasource` — 외부 DBMS의 URI 또는 이름입니다.

  URI 포맷: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
  MySQL의 예시: `jdbc:mysql://localhost:3306/?user=root&password=root`.

* `external_database` — 외부 DBMS에 있는 데이터베이스 이름 또는 명시적으로 정의된 테이블 스키마입니다(예시 참조).

* `external_table` — 외부 데이터베이스에 있는 테이블 이름 또는 `select * from table1 where column1=1`과 같은 select 쿼리입니다.

* 이러한 매개변수는 [이름이 지정된 컬렉션](/ko/operations/named-collections.md)을 사용하여 전달할 수도 있습니다.

<div id="usage-example">
  ## 사용 예시
</div>

해당 콘솔 클라이언트로 MySQL 서버에 직접 연결해 테이블을 생성합니다:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

ClickHouse 서버에서 테이블을 생성하고 해당 테이블의 데이터를 조회합니다:

```sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

```sql
SELECT *
FROM jdbc_table
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

```sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

<div id="see-also">
  ## 관련 항목
</div>

* [JDBC 테이블 함수](../../../sql-reference/table-functions/jdbc.md).