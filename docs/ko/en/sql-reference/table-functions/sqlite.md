---
description: 'SQLite 데이터베이스에 저장된 데이터에 대해 쿼리를 실행할 수 있습니다.'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: '참고'
---

[SQLite](../../engines/database-engines/sqlite.md) 데이터베이스에 저장된 데이터에 대해 쿼리를 실행할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## 인수
</div>

* `db_path` — SQLite 데이터베이스가 저장된 파일의 경로입니다. [String](../../sql-reference/data-types/string.md).
* `table_name` — SQLite 데이터베이스에 있는 테이블 이름 또는 SQLite에 그대로 전달되는 쿼리입니다([테이블 이름 대신 쿼리 전달](#passing-a-query) 참조). [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## 반환 값
</div>

* 원래 `SQLite` 테이블과 동일한 컬럼으로 구성된 테이블 객체입니다.

<div id="passing-a-query">
  ## 테이블 이름 대신 쿼리 전달
</div>

테이블 이름 대신 두 번째 인수로 SQLite에 그대로 전달되는 `SELECT` 쿼리를 사용할 수 있습니다. 결과 테이블의 구조는 쿼리 결과를 바탕으로 자동 추론됩니다. 쿼리는 서브쿼리로 작성하거나 `query` 함수로 감쌀 수 있습니다:

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

이러한 테이블은 읽기 전용이므로 `INSERT`는 허용되지 않습니다. 동일한 구문은 [`SQLite`](/ko/engines/table-engines/integrations/sqlite) 테이블 엔진에서도 지원됩니다.

:::note
하위 쿼리 형식 `(SELECT ...)`은 ClickHouse에서 파싱된 뒤 SQLite로 전송되기 전에 다시 직렬화됩니다. 따라서 유효한 ClickHouse SQL이어야 합니다. ClickHouse가 파싱하지 않는 SQLite 전용 구문을 전달하려면 `query('...')` 형식을 사용하십시오. 이 형식의 텍스트는 그대로 SQLite로 전송됩니다.

전달된 쿼리로는 바깥쪽 ClickHouse 쿼리의 `WHERE`, `LIMIT`, 집계 등은 **푸시다운되지 않으며**, 전체 쿼리 결과를 가져온 후 ClickHouse에서 적용됩니다. SQLite에서 읽는 데이터를 제한하려면 필터를 전달된 쿼리 내부에 넣으십시오. [`external_table_strict_query = 1`](/ko/operations/settings/settings#external_table_strict_query)을 사용하면 푸시다운할 수 없는 바깥쪽 필터는 로컬에서 적용하는 대신 예외와 함께 거부됩니다.
:::

<div id="example">
  ## 예시
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## 관련
</div>

* [SQLite](../../engines/table-engines/integrations/sqlite.md) 테이블 엔진
* [SQLite 데이터베이스 엔진](../../engines/database-engines/sqlite.md) — 데이터 타입 지원 섹션