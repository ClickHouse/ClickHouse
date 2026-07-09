---
description: 'PostgreSQL 테이블의 초기 데이터 덤프를 사용해 ClickHouse 테이블을 생성하고
  복제 프로세스를 시작합니다.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'MaterializedPostgreSQL 테이블 엔진'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # MaterializedPostgreSQL 테이블 엔진
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
ClickHouse Cloud 사용자는 PostgreSQL을 ClickHouse로 복제할 때 [ClickPipes](/ko/integrations/clickpipes)를 사용하는 것이 권장됩니다. ClickPipes는 PostgreSQL용 고성능 CDC(Change Data Capture)를 네이티브로 지원합니다.
:::

PostgreSQL 테이블의 초기 데이터 덤프를 기반으로 ClickHouse 테이블을 생성하고 복제 프로세스를 시작합니다. 즉, 원격 PostgreSQL 데이터베이스의 PostgreSQL 테이블에서 새로운 변경 사항이 발생할 때마다 이를 적용하는 백그라운드 작업을 실행합니다.

:::note
이 테이블 엔진은 Experimental 기능입니다. 사용하려면 설정 파일에서 `allow_experimental_materialized_postgresql_table`를 1로 설정하거나 `SET` 명령을 사용하십시오.

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

둘 이상의 테이블이 필요한 경우에는 테이블 엔진 대신 [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) 데이터베이스 엔진을 사용하고, 복제할 테이블을 지정하는 `materialized_postgresql_tables_list` 설정을 사용하는 것을 강력히 권장합니다(향후에는 데이터베이스 `schema`도 추가할 수 있게 될 예정입니다). 이렇게 하면 CPU 사용량, 연결 수, 그리고 원격 PostgreSQL 데이터베이스 내 replication slot 수 측면에서 훨씬 더 효율적입니다.

<div id="creating-a-table">
  ## 테이블 만들기
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**엔진 매개변수**

* `host:port` — PostgreSQL 서버 주소입니다.
* `database` — 원격 데이터베이스 이름입니다.
* `table` — 원격 테이블 이름입니다.
* `user` — PostgreSQL 사용자 이름입니다.
* `password` — 사용자 비밀번호입니다.

<div id="requirements">
  ## 요구 사항
</div>

1. PostgreSQL 구성 파일에서 [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) 설정 값은 `logical`이어야 하며, `max_replication_slots` 매개변수 값은 최소 `2`여야 합니다.

2. `MaterializedPostgreSQL` 엔진을 사용하는 테이블에는 기본 키가 있어야 하며, 이 기본 키는 PostgreSQL 테이블의 레플리카 아이덴티티 인덱스(기본값: 기본 키(primary key))와 동일해야 합니다([레플리카 아이덴티티 인덱스에 대한 자세한 내용](../../../engines/database-engines/materialized-postgresql.md#requirements) 참조).

3. [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)) 데이터베이스만 사용할 수 있습니다.

4. 구현상 PostgreSQL 함수 [pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html)가 필요하므로 `MaterializedPostgreSQL` 테이블 엔진은 PostgreSQL 버전 &gt;= 11에서만 작동합니다.

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_version` — 트랜잭션 카운터. 유형: [UInt64](../../../sql-reference/data-types/int-uint.md).

* `_sign` — 삭제 표시. 유형: [Int8](../../../sql-reference/data-types/int-uint.md). 가능한 값:
  * `1` — 행이 삭제되지 않음,
  * `-1` — 행이 삭제됨.

이 컬럼들은 테이블을 생성할 때 추가할 필요가 없습니다. `SELECT` 쿼리에서 항상 사용할 수 있습니다.
`_version` 컬럼은 `WAL`의 `LSN` 위치와 같으므로, 복제가 얼마나 최신 상태인지 확인하는 데 사용할 수 있습니다.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
[**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) 값의 복제는 지원되지 않습니다. 대신 해당 데이터 타입의 기본값이 사용됩니다.
:::