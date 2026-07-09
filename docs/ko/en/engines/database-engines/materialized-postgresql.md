---
description: 'PostgreSQL 데이터베이스의 테이블을 기반으로 ClickHouse 데이터베이스를 생성합니다.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 60
slug: /engines/database-engines/materialized-postgresql
title: 'MaterializedPostgreSQL'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql">
  # MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
ClickHouse Cloud 사용자는 PostgreSQL을 ClickHouse로 복제할 때 [ClickPipes](/ko/integrations/clickpipes)를 사용하는 것이 권장됩니다. ClickPipes는 PostgreSQL용 고성능 CDC(Change Data Capture)를 네이티브로 지원합니다.
:::

PostgreSQL 데이터베이스의 테이블을 사용해 ClickHouse 데이터베이스를 생성합니다. `MaterializedPostgreSQL` 엔진을 사용하는 데이터베이스는 먼저 PostgreSQL 데이터베이스의 스냅샷을 생성한 다음, 필요한 테이블을 로드합니다. 필요한 테이블에는 지정된 데이터베이스의 스키마 부분 집합에 속한 테이블 부분 집합이 포함될 수 있습니다. 스냅샷과 함께 데이터베이스 엔진은 LSN을 획득하며, 테이블의 초기 덤프가 완료되면 WAL에서 업데이트를 가져오기 시작합니다. 데이터베이스가 생성된 후 PostgreSQL 데이터베이스에 새로 추가된 테이블은 복제에 자동으로 추가되지 않습니다. 이러한 테이블은 `ATTACH TABLE db.table` 쿼리를 사용해 수동으로 추가해야 합니다.

복제는 PostgreSQL 논리적 복제 프로토콜을 통해 구현됩니다. 이 프로토콜은 DDL 복제는 지원하지 않지만, 복제를 중단시키는 호환되지 않는 변경 사항(컬럼 타입 변경, 컬럼 추가/제거)이 발생했는지는 파악할 수 있습니다. 이러한 변경 사항이 감지되면 해당 테이블은 더 이상 업데이트를 수신하지 않습니다. 이 경우 테이블 전체를 다시 로드하려면 `ATTACH`/ `DETACH PERMANENTLY` 쿼리를 사용해야 합니다. DDL이 복제를 중단시키지 않는 경우(예: 컬럼 이름 변경)에는 테이블이 계속 업데이트를 수신합니다(삽입은 위치 기준으로 수행됨).

:::note
이 데이터베이스 엔진은 Experimental 상태입니다. 사용하려면 설정 파일에서 `allow_experimental_database_materialized_postgresql`을 1로 설정하거나 `SET` 명령을 사용하십시오:

```sql
SET allow_experimental_database_materialized_postgresql=1
```

:::

<div id="creating-a-database">
  ## 데이터베이스 생성
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**엔진 매개변수**

* `host:port` — PostgreSQL 서버 endpoint입니다.
* `database` — PostgreSQL 데이터베이스 이름입니다.
* `user` — PostgreSQL 사용자 이름입니다.
* `password` — 사용자 비밀번호입니다.

<div id="example-of-use">
  ## 사용 예시
</div>

```sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgres_db.postgres_table;
```

<div id="dynamically-adding-table-to-replication">
  ## 복제에 새 테이블 동적으로 추가하기
</div>

`MaterializedPostgreSQL` 데이터베이스를 생성한 후에는 연결된 PostgreSQL 데이터베이스의 새 테이블이 자동으로 감지되지 않습니다. 이러한 테이블은 수동으로 추가할 수 있습니다:

```sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
버전 22.1 이전에는 테이블을 복제 대상에 추가하면 삭제되지 않은 임시 replication slot(이름: `{db_name}_ch_replication_slot_tmp`)이 남았습니다. 22.1 이전 버전의 ClickHouse에서 테이블을 ATTACH하는 경우, 반드시 이 슬롯을 수동으로 삭제하십시오(`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`). 그렇지 않으면 디스크 사용량이 증가합니다. 이 문제는 22.1에서 수정되었습니다.
:::

<div id="dynamically-removing-table-from-replication">
  ## 복제 대상에서 테이블을 동적으로 제거하기
</div>

특정 테이블을 복제 대상에서 제외할 수 있습니다:

```sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

<div id="schema">
  ## PostgreSQL 스키마
</div>

PostgreSQL [스키마](https://www.postgresql.org/docs/9.1/ddl-schemas.html)는 3가지 방식으로 구성할 수 있습니다(21.12 버전부터 지원).

1. `MaterializedPostgreSQL` 데이터베이스 엔진 하나당 하나의 스키마를 사용합니다. 이 경우 `materialized_postgresql_schema` 설정을 사용해야 합니다.
   테이블은 테이블 이름만으로 접근합니다:

```sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. 하나의 `MaterializedPostgreSQL` 데이터베이스 엔진에 대해, 지정된 테이블 집합이 있는 스키마를 원하는 수만큼 사용할 수 있습니다. 이 경우 `materialized_postgresql_tables_list` 설정을 사용해야 합니다. 각 테이블은 해당 스키마 이름과 함께 지정합니다.
   테이블에 접근할 때는 스키마 이름과 테이블 이름을 함께 사용합니다:

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

하지만 이 경우 `materialized_postgresql_tables_list`의 모든 테이블(table)은 스키마(schema) 이름과 함께 지정해야 합니다.
`materialized_postgresql_tables_list_with_schema = 1`이 필요합니다.

경고: 이 경우 테이블 이름에는 점을 사용할 수 없습니다.

3. 하나의 `MaterializedPostgreSQL` 데이터베이스 엔진에 대해 전체 테이블 집합을 포함하는 스키마를 여러 개 사용할 수 있습니다. 이 경우 설정 `materialized_postgresql_schema_list`를 사용해야 합니다.

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

경고: 이 경우 테이블(table) 이름에 점(.)을 사용할 수 없습니다.

<div id="requirements">
  ## 요구 사항
</div>

1. PostgreSQL 구성 파일에서 [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) 설정값은 `logical`이어야 하며, `max_replication_slots` 매개변수 값은 최소 `2` 이상이어야 합니다.

2. 각 복제된 테이블은 다음 [replica identity](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) 중 하나로 설정되어 있어야 합니다.

* 기본 키(primary key) (기본값)

* 인덱스

```bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

기본 키(primary key)는 항상 먼저 확인됩니다. 기본 키가 없으면 replica identity index로 지정된 인덱스를 확인합니다.
인덱스가 replica identity로 사용되는 경우, 테이블에는 해당 인덱스가 하나만 있어야 합니다.
다음 명령으로 특정 테이블에 사용되는 유형을 확인할 수 있습니다.

```bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

:::note
[**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) 값의 복제는 지원되지 않습니다. 대신 해당 데이터 타입의 기본값이 사용됩니다.
:::

<div id="settings">
  ## 설정
</div>

<div id="materialized-postgresql-tables-list">
  ### `materialized_postgresql_tables_list`
</div>

[MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md) 데이터베이스 엔진을 통해 복제할 PostgreSQL 데이터베이스 테이블 목록을 쉼표로 구분하여 설정합니다.

각 테이블은 괄호 안에 복제할 컬럼의 부분 집합을 지정할 수 있습니다. 컬럼 부분 집합을 지정하지 않으면 해당 테이블의 모든 컬럼이 복제됩니다.

```sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
```

기본값: 빈 목록 — PostgreSQL 데이터베이스 전체가 복제됨을 의미합니다.

<div id="materialized-postgresql-schema">
  ### `materialized_postgresql_schema`
</div>

기본값: 빈 문자열입니다. (기본 스키마(schema)가 사용됩니다)

<div id="materialized-postgresql-schema-list">
  ### `materialized_postgresql_schema_list`
</div>

기본값: 빈 목록입니다. (기본 스키마(schema)가 사용됩니다)

<div id="materialized-postgresql-max-block-size">
  ### `materialized_postgresql_max_block_size`
</div>

데이터를 PostgreSQL 데이터베이스 테이블에 플러시하기 전에 메모리에 수집되는 행 수를 설정합니다.

가능한 값:

* 양의 정수.

기본값: `65536`.

<div id="materialized-postgresql-replication-slot">
  ### `materialized_postgresql_replication_slot`
</div>

사용자가 생성한 replication slot입니다. `materialized_postgresql_snapshot`과 함께 사용해야 합니다.

<div id="materialized-postgresql-snapshot">
  ### `materialized_postgresql_snapshot`
</div>

[PostgreSQL 테이블의 초기 덤프](../../engines/database-engines/materialized-postgresql.md)를 수행할 기준이 되는 스냅샷을 식별하는 문자열입니다. `materialized_postgresql_replication_slot`과 함께 사용해야 합니다.

```sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
```

설정은 필요에 따라 DDL 쿼리로 변경할 수 있습니다. 다만 `materialized_postgresql_tables_list` 설정은 변경할 수 없습니다. 이 설정의 테이블 목록을 업데이트하려면 `ATTACH TABLE` 쿼리를 사용하십시오.

```sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
```

<div id="materialized_postgresql_use_unique_replication_consumer_identifier">
  ### `materialized_postgresql_use_unique_replication_consumer_identifier`
</div>

복제 시 고유한 consumer 식별자를 사용합니다. 기본값: `0`.
`1`로 설정하면 동일한 `PostgreSQL` 테이블을 가리키는 여러 `MaterializedPostgreSQL` 테이블을 구성할 수 있습니다.

<div id="materialized-postgresql-use-extended-date-and-time-types">
  ### `materialized_postgresql_use_extended_date_and_time_types`
</div>

PostgreSQL `date` 및 `timestamp`/`timestamptz` 타입을, PostgreSQL 타입의 더 넓은 값 범위를 지원하는 ClickHouse `Date32` 및 `DateTime64`에 매핑합니다. 기본값: `1`.
`0`으로 설정하면 대신 더 좁은 `Date` 및 `DateTime` 타입이 사용됩니다(이 경우 해당 범위를 벗어나는 값이나 초 미만 정밀도가 있는 값은 표현할 수 없습니다).

이 설정은 중첩 테이블이 생성될 때 타입 추론(type inference)으로 선택되는 컬럼 타입만 제어하므로 `CREATE DATABASE` 시점에 지정해야 합니다. 이후에는 `ALTER DATABASE ... MODIFY SETTING`으로 변경할 수 없습니다(이미 생성된 중첩 테이블은 고정된 컬럼 타입을 유지하므로 이러한 변경은 거부됩니다). 변경하려면 데이터베이스를 다시 생성해야 합니다. 컬럼 타입을 명시적으로 선언하는 `MaterializedPostgreSQL` 테이블 엔진에는 적용되지 않습니다.

<div id="notes">
  ## 참고
</div>

<div id="logical-replication-slot-failover">
  ### 논리적 복제 replication slot 장애 조치
</div>

프라이머리에 존재하는 논리적 복제 replication slot은 대기 레플리카에서는 사용할 수 없습니다.
따라서 장애 조치가 발생하면 새 프라이머리(기존 물리적 대기 인스턴스)는 이전 프라이머리에 있던 슬롯을 알지 못하게 됩니다. 이로 인해 PostgreSQL로부터의 복제가 중단됩니다.
이 문제를 해결하려면 replication slot을 직접 관리하고 영구적인 replication slot을 정의해야 합니다(자세한 내용은 [여기](https://patroni.readthedocs.io/en/latest/SETTINGS.html)에서 확인할 수 있습니다). 슬롯 이름은 `materialized_postgresql_replication_slot` 설정으로 전달해야 하며, `EXPORT SNAPSHOT` 옵션으로 내보내야 합니다. snapshot 식별자는 `materialized_postgresql_snapshot` 설정으로 전달해야 합니다.

이 기능은 실제로 필요한 경우에만 사용해야 합니다. 특별한 필요가 없거나 그 이유를 충분히 이해하지 못한 경우에는 테이블 엔진이 자체 replication slot을 생성하고 관리하도록 두는 편이 더 좋습니다.

**예시 ([@bchrobot](https://github.com/bchrobot) 제공)**

1. PostgreSQL에서 replication slot을 구성합니다.

   ```yaml
   apiVersion: "acid.zalan.do/v1"
   kind: postgresql
   metadata:
     name: acid-demo-cluster
   spec:
     numberOfInstances: 2
     postgresql:
       parameters:
         wal_level: logical
     patroni:
       slots:
         clickhouse_sync:
           type: logical
           database: demodb
           plugin: pgoutput
   ```

2. replication slot이 준비될 때까지 기다린 다음 transaction을 시작하고 transaction snapshot 식별자를 내보냅니다:

   ```sql
   BEGIN;
   SELECT pg_export_snapshot();
   ```

3. ClickHouse에서 데이터베이스를 생성합니다:

   ```sql
   CREATE DATABASE demodb
   ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
   SETTINGS
     materialized_postgresql_replication_slot = 'clickhouse_sync',
     materialized_postgresql_snapshot = '0000000A-0000023F-3',
     materialized_postgresql_tables_list = 'table1,table2,table3';
   ```

4. ClickHouse DB로의 복제가 확인되면 PostgreSQL transaction을 종료합니다. 장애 조치 후에도 복제가 계속되는지 확인합니다:

   ```bash
   kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
   ```

<div id="required-permissions">
  ### 필요한 권한
</div>

1. [CREATE PUBLICATION](https://www.postgresql.org/docs/14/sql-createpublication.html) -- 쿼리 생성 권한.

2. [CREATE&#95;REPLICATION&#95;SLOT](https://www.postgresql.org/docs/10/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-SLOT) -- 복제 권한.

3. [pg&#95;drop&#95;replication&#95;slot](https://www.postgresql.org/docs/9.5/functions-admin.html#FUNCTIONS-REPLICATION) -- 복제 권한 또는 슈퍼유저.

4. [DROP PUBLICATION](https://www.postgresql.org/docs/10/sql-droppublication.html) -- publication의 소유자(MaterializedPostgreSQL engine 자체의 `username`).

`2`와 `3` 명령을 실행하지 않고, 해당 권한 없이도 진행할 수 있습니다. `materialized_postgresql_replication_slot` 및 `materialized_postgresql_snapshot` 설정을 사용하십시오. 다만 각별한 주의가 필요합니다.

다음 테이블에 대한 액세스 권한:

1. pg&#95;publication

2. pg&#95;replication&#95;slots

3. pg&#95;publication&#95;tables

<div id="backup-and-restore">
  ### 백업 및 복원
</div>

`MaterializedPostgreSQL` 데이터베이스는 백업할 수 있습니다. 모든 복제된 테이블(Replicated Table)의 데이터는 중첩된 `ReplacingMergeTree` 테이블에 저장되므로, `BACKUP DATABASE`는 내부 테이블에 위임하는 방식으로 해당 데이터를 백업합니다.

```sql
BACKUP DATABASE postgres_db TO Disk('backups', 'postgres_db.zip');
```

`MaterializedPostgreSQL` 데이터베이스 또는 테이블을 **제자리에서 복원하는 기능은 지원되지 않습니다**. 복원된 `MaterializedPostgreSQL` 객체는 즉시 실제 PostgreSQL 소스에서 복제를 시작하므로, 그 위에 백업 스냅샷을 복원하면 스냅샷과 현재 원격 상태가 뒤섞이게 됩니다. 따라서 이 경우 RESTORE는 안전을 위해 실패합니다. 대신 캡처된 데이터를 일반 `ReplacingMergeTree` 테이블로 복원하십시오:

* 데이터베이스 백업에서는 각 테이블에 저장된 정의가 이미 합성된 중첩 `ReplacingMergeTree`이며(`MaterializedPostgreSQL` 엔진이 아님), 따라서 각 테이블을 아직 존재하지 않는 새 테이블로 바로 복원할 수 있습니다:

  ```sql
  RESTORE TABLE postgres_db.table1 AS restored_db.table1
  FROM Disk('backups', 'postgres_db.zip')
  SETTINGS allow_different_table_def = 1;
  ```

* 독립형 `MaterializedPostgreSQL` 테이블 백업에서는 저장된 정의가 `MaterializedPostgreSQL` 엔진 자체입니다. 중첩 테이블과 동일한 구조(`_sign` 및 `_version` 컬럼 포함)로 `ReplacingMergeTree` 테이블을 미리 생성한 다음, 여기에 복원하십시오:

  ```sql
  RESTORE TABLE src AS existing_replacing_mergetree
  FROM Disk('backups', 'table.zip')
  SETTINGS allow_different_table_def = 1;
  ```