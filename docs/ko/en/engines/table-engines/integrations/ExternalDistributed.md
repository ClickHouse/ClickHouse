---
description: '`ExternalDistributed` 엔진은 원격 서버의 MySQL 또는 PostgreSQL에 저장된 데이터에 대해 `SELECT` 쿼리를 실행할 수 있습니다. MySQL 또는 PostgreSQL 엔진을 인수로 받을 수 있으므로 세그먼트 분할이 가능합니다.'
sidebar_label: 'ExternalDistributed'
sidebar_position: 55
slug: /engines/table-engines/integrations/ExternalDistributed
title: 'ExternalDistributed 테이블 엔진'
doc_type: 'reference'
---

`ExternalDistributed` 엔진은 원격 서버의 MySQL 또는 PostgreSQL에 저장된 데이터에 대해 `SELECT` 쿼리를 실행할 수 있습니다. [MySQL](../../../engines/table-engines/integrations/mysql.md) 또는 [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) 엔진을 인수로 받을 수 있으므로 세그먼트 분할이 가능합니다.

<div id="creating-a-table">
  ## 테이블 생성하기
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

[CREATE TABLE](/ko/sql-reference/statements/create/table) 쿼리에 대한 자세한 설명은 해당 문서를 참조하십시오.

테이블 구조는 원본 테이블 구조와 다를 수 있습니다.

* 컬럼 이름은 원본 테이블과 동일해야 하지만, 그중 일부만 사용해도 되며 순서는 임의로 지정할 수 있습니다.
* 컬럼 타입은 원본 테이블의 것과 달라도 됩니다. ClickHouse는 값을 ClickHouse 데이터 타입으로 [형변환](/ko/sql-reference/functions/type-conversion-functions#CAST)하려고 시도합니다.

**엔진 매개변수**

* `engine` — 테이블 엔진 `MySQL` 또는 `PostgreSQL`.
* `host:port` — MySQL 또는 PostgreSQL 서버 주소.
* `database` — 원격 데이터베이스 이름.
* `table` — 원격 테이블 이름.
* `user` — 사용자 이름.
* `password` — 사용자 비밀번호.

<div id="implementation-details">
  ## 구현 세부 정보
</div>

여러 레플리카를 지원하며, 레플리카는 `|`로, 세그먼트는 `,`로 구분해 나열해야 합니다. 예시:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

레플리카를 지정하면 읽을 때 각 세그먼트마다 사용 가능한 레플리카 중 하나가 선택됩니다. 연결에 실패하면 다음 레플리카를 선택하며, 모든 레플리카에 대해 이 과정을 반복합니다. 모든 레플리카에 대한 연결 시도가 실패하면 같은 방식으로 여러 번 다시 시도합니다.

세그먼트 수와 각 세그먼트의 레플리카 수는 얼마든지 지정할 수 있습니다.

**관련 항목**

* [MySQL 테이블 엔진](../../../engines/table-engines/integrations/mysql.md)
* [PostgreSQL 테이블 엔진](../../../engines/table-engines/integrations/postgresql.md)
* [분산 테이블 엔진](../../../engines/table-engines/special/distributed.md)