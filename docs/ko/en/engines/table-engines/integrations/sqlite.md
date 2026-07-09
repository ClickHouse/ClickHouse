---
description: '이 엔진은 SQLite에서 데이터를 가져오고 SQLite로 데이터를 내보낼 수 있으며, ClickHouse에서 SQLite 테이블에 직접 쿼리할 수 있도록 지원합니다.'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'SQLite 테이블 엔진'
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # SQLite 테이블 엔진
</div>

<CloudNotSupportedBadge />

이 엔진을 사용하면 SQLite로 데이터를 가져오고 내보낼 수 있으며, ClickHouse에서 SQLite 테이블에 직접 쿼리할 수도 있습니다.

<div id="creating-a-table">
  ## 테이블 만들기
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**엔진 매개변수**

* `db_path` — 데이터베이스가 저장된 SQLite 파일의 경로입니다.
* `table` — SQLite 데이터베이스의 테이블 이름 또는 SQLite에 그대로 전달할 쿼리입니다([테이블 이름 대신 쿼리 전달](#passing-a-query) 참조).

<div id="passing-a-query">
  ## 테이블 이름 대신 쿼리 사용하기
</div>

테이블 이름 대신 `table` 인수에 SQLite로 그대로 전달되는 `SELECT` 쿼리를 지정할 수 있습니다. 테이블 구조는 쿼리 결과로부터 자동으로 추론됩니다. 쿼리는 서브쿼리로 작성하거나 `query` 함수로 감싸서 작성할 수 있습니다.

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

이러한 테이블은 읽기 전용이므로 `INSERT`는 허용되지 않습니다. 동일한 구문은 [`sqlite`](/ko/sql-reference/table-functions/sqlite) 테이블 함수에서도 지원됩니다.

:::note
서브쿼리(subquery) 형식 `(SELECT ...)`은 ClickHouse에서 구문 분석된 뒤 SQLite로 전송되기 전에 다시 직렬화됩니다. 따라서 유효한 ClickHouse SQL이어야 합니다. ClickHouse가 구문 분석하지 않는 SQLite 전용 구문을 전달하려면 `query('...')` 형식을 사용하십시오. 이 형식의 텍스트는 수정 없이 그대로 SQLite로 전송됩니다.

전달된 쿼리로는 이를 감싸는 ClickHouse 쿼리의 바깥쪽 `WHERE`, `LIMIT`, 집계 등의 처리가 **푸시다운되지 않으며**, 전체 쿼리 결과를 가져온 뒤 ClickHouse에서 적용됩니다. SQLite에서 읽는 데이터를 제한하려면 필터를 전달된 쿼리 안에 넣으십시오. [`external_table_strict_query = 1`](/ko/operations/settings/settings#external_table_strict_query)을 사용하면 푸시다운할 수 없는 바깥쪽 필터는 로컬에서 적용되는 대신 예외를 발생시키며 거부됩니다.
:::

<div id="data-types-support">
  ## 데이터 타입 지원
</div>

테이블 정의에서 ClickHouse 컬럼 타입을 명시적으로 지정하면, 다음 ClickHouse 타입을 SQLite TEXT 컬럼에서 파싱할 수 있습니다.

* [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* 모든 정수 타입 ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

기본 타입 매핑은 [SQLite 데이터베이스 엔진](../../../engines/database-engines/sqlite.md#data_types-support) 문서를 참조하십시오.

<div id="usage-example">
  ## 사용 예시
</div>

다음은 SQLite 테이블을 생성하는 쿼리입니다:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

테이블의 데이터를 반환합니다:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**관련 항목**

* [SQLite](../../../engines/database-engines/sqlite.md) 엔진
* [sqlite](../../../sql-reference/table-functions/sqlite.md) 테이블 함수