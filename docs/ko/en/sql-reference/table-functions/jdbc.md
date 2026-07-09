---
description: 'JDBC 드라이버를 통해 연결된 테이블을 반환합니다.'
sidebar_label: 'jdbc'
sidebar_position: 100
slug: /sql-reference/table-functions/jdbc
title: 'jdbc'
doc_type: 'reference'
---

:::note
clickhouse-jdbc-bridge에는 실험적 코드가 포함되어 있으며 더 이상 지원되지 않습니다. 신뢰성 문제와 보안 취약점이 있을 수 있습니다. 사용에 따른 위험은 전적으로 사용자 책임입니다.
ClickHouse는 일회성 쿼리 시나리오(Postgres, MySQL, MongoDB 등)에서는 더 나은 대안을 제공하는 ClickHouse의 기본 제공 테이블 함수를 사용할 것을 권장합니다.
:::

JDBC 테이블 함수는 JDBC 드라이버를 통해 연결된 테이블을 반환합니다.

이 테이블 함수를 사용하려면 별도의 [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) 프로그램이 실행 중이어야 합니다.
널 허용 타입(Nullable)을 지원합니다(쿼리하는 원격 테이블의 DDL 기준).

<div id="syntax">
  ## 구문
</div>

```sql
jdbc(datasource, external_database, external_table)
jdbc(datasource, external_table)
jdbc(named_collection)
```

<div id="examples">
  ## 예시
</div>

외부 데이터베이스 이름 대신 스키마를 지정할 수 있습니다:

```sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

```sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'select * from schema.table')
```

```sql
SELECT * FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

```sql
SELECT *
FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

```sql
SELECT a.datasource AS server1, b.datasource AS server2, b.name AS db
FROM jdbc('mysql-dev?datasource_column', 'show databases') a
INNER JOIN jdbc('self?datasource_column', 'show databases') b ON a.Database = b.name
```