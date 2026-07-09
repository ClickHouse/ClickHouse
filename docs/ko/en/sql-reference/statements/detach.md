---
description: 'DETACH에 대한 문서'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'DETACH SQL 문'
doc_type: 'reference'
---

서버가 테이블, materialized view, 딕셔너리 또는 데이터베이스의 존재를 &quot;잊게&quot; 합니다.

**구문**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

분리해도 테이블, materialized view, 딕셔너리 또는 데이터베이스의 데이터나 메타데이터(metadata)는 삭제되지 않습니다. 엔터티가 `PERMANENTLY`로 분리되지 않은 경우, 다음 서버 시작 시 서버가 메타데이터를 읽고 해당 테이블/뷰/딕셔너리/데이터베이스를 다시 불러옵니다. 엔터티가 `PERMANENTLY`로 분리된 경우에는 자동으로 다시 불러오지 않습니다.

테이블, 딕셔너리 또는 데이터베이스가 영구적으로 분리되었는지와 관계없이 두 경우 모두 [ATTACH](../../sql-reference/statements/attach.md) 쿼리를 사용해 다시 연결할 수 있습니다.
시스템 로그 테이블도 다시 연결할 수 있습니다(예: `query_log`, `text_log` 등). 다른 시스템 테이블은 다시 연결할 수 없습니다. 다음 서버 시작 시 서버가 해당 테이블들을 다시 불러옵니다.

`ATTACH MATERIALIZED VIEW`는 짧은 구문(`SELECT` 없음)에서는 작동하지 않지만, `ATTACH TABLE` 쿼리를 사용하면 연결할 수 있습니다.

이미 분리된(임시) 테이블은 영구적으로 분리할 수 없다는 점에 유의하십시오. 하지만 다시 연결한 후 다시 영구적으로 분리할 수는 있습니다.

또한 분리된 테이블은 [DROP](../../sql-reference/statements/drop.md#drop-table)할 수 없고, 영구적으로 분리된 테이블과 같은 이름으로 [CREATE TABLE](../../sql-reference/statements/create/table.md)할 수도 없으며, [RENAME TABLE](../../sql-reference/statements/rename.md) 쿼리로 다른 테이블로 대체할 수도 없습니다.

`SYNC` 수정자는 지연 없이 작업을 실행합니다.

**예시**

테이블 생성:

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

테이블 분리하기:

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
ClickHouse Cloud에서는 `PERMANENTLY` 절을 사용해야 합니다. 예: `DETACH TABLE <table> PERMANENTLY`. 이 절을 사용하지 않으면 클러스터가 재시작될 때(예: 업그레이드 중) 테이블이 다시 ATTACH됩니다.
:::

**관련 항목**

* [Materialized View](/ko/sql-reference/statements/create/view#materialized-view)
* [딕셔너리](./create/dictionary/overview.md)