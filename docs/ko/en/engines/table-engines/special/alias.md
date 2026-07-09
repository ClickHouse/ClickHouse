---
description: 'Alias 테이블 엔진은 다른 테이블에 대한 투명한 프록시를 생성합니다. 모든 작업은 대상 테이블로 전달되며, Alias 자체는 데이터를 저장하지 않습니다.'
sidebar_label: 'Alias'
sidebar_position: 5
slug: /engines/table-engines/special/alias
title: 'Alias 테이블 엔진'
doc_type: '참고'
---

<div id="alias-table-engine">
  # Alias 테이블 엔진
</div>

`Alias` 엔진은 다른 테이블에 대한 프록시를 생성합니다. 모든 읽기/쓰기 작업은 대상 테이블로 전달되며, `Alias` 자체는 데이터를 저장하지 않고 대상 테이블에 대한 참조만 유지합니다.

<div id="creating-a-table">
  ## 테이블 생성하기
</div>

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_table)
```

또는 데이터베이스 이름을 명시할 수 있습니다:

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_db, target_table)
```

:::note
`Alias` 테이블은 명시적인 컬럼 정의를 지원하지 않습니다. 컬럼은 대상 테이블(target table)에서 자동으로 상속됩니다. 따라서 별칭 테이블은 항상 대상 테이블의 스키마(schema)와 일치합니다.
:::

<div id="engine-parameters">
  ## 엔진 매개변수
</div>

* **`target_db (optional)`** — 대상 테이블이 포함된 데이터베이스(database)의 이름입니다.
* **`target_table`** — 대상 테이블의 이름입니다.

:::note
`target_db`를 생략하고 `target_table`이 완전 수식되지 않은 경우(예: `Alias('my_table')`), 대상은 세션의 현재 데이터베이스가 아니라 별칭 자체가 속한 동일한 데이터베이스로 해석됩니다.
:::

<div id="supported-operations">
  ## 지원되는 작업
</div>

`Alias` 테이블 엔진은 모든 주요 작업을 모두 지원합니다. 

<div id="operations-on-target">
  ### 대상 테이블 작업
</div>

다음 작업은 대상 테이블로 프록시되어 수행됩니다:

| 작업                           | 지원 | 설명                                 |
| ---------------------------- | -- | ---------------------------------- |
| `SELECT`                     | ✅  | 대상 테이블에서 데이터 읽기                    |
| `INSERT`                     | ✅  | 대상 테이블에 데이터 쓰기                     |
| `INSERT SELECT`              | ✅  | 대상 테이블에 일괄 삽입                      |
| `ALTER TABLE ADD COLUMN`     | ✅  | 대상 테이블에 컬럼 추가                      |
| `ALTER TABLE MODIFY SETTING` | ✅  | 대상 테이블 설정 수정                       |
| `ALTER TABLE PARTITION`      | ✅  | 대상에 대한 파티션 작업 (DETACH/ATTACH/DROP) |
| `ALTER TABLE UPDATE`         | ✅  | 대상 테이블의 행 업데이트 (mutation)          |
| `ALTER TABLE DELETE`         | ✅  | 대상 테이블에서 행 삭제 (mutation)           |
| `OPTIMIZE TABLE`             | ✅  | 대상 테이블 최적화 (파트 머지)                 |
| `TRUNCATE TABLE`             | ✅  | 대상 테이블 비우기                         |

<div id="operations-on-alias">
  ### 별칭 자체에 수행하는 작업
</div>

다음 작업은 대상 테이블(target table)에는 영향을 주지 않고 별칭에만 영향을 미칩니다:

| 작업             | 지원 | 설명                                 |
| -------------- | -- | ---------------------------------- |
| `DROP TABLE`   | ✅  | 별칭만 삭제되며, 대상 테이블은 변경되지 않습니다     |
| `RENAME TABLE` | ✅  | 별칭의 이름만 변경되며, 대상 테이블은 변경되지 않습니다 |

<div id="usage-examples">
  ## 사용 예시
</div>

<div id="basic-alias-creation">
  ### 기본 Alias 생성
</div>

같은 데이터베이스에 간단한 Alias를 생성합니다:

```sql
-- Create source table
CREATE TABLE source_data (
    id UInt32,
    name String,
    value Float64
) ENGINE = MergeTree
ORDER BY id;

-- Insert some data
INSERT INTO source_data VALUES (1, 'one', 10.1), (2, 'two', 20.2);

-- Create alias
CREATE TABLE data_alias ENGINE = Alias('source_data');

-- Query through alias
SELECT * FROM data_alias;
```

```text
┌─id─┬─name─┬─value─┐
│  1 │ one  │  10.1 │
│  2 │ two  │  20.2 │
└────┴──────┴───────┘
```

<div id="cross-database-alias">
  ### 데이터베이스 간 별칭
</div>

다른 데이터베이스에 있는 테이블을 가리키는 별칭을 생성합니다:

```sql
-- Create databases
CREATE DATABASE db1;
CREATE DATABASE db2;

-- Create source table in db1
CREATE TABLE db1.events (
    timestamp DateTime,
    event_type String,
    user_id UInt32
) ENGINE = MergeTree
ORDER BY timestamp;

-- Create alias in db2 pointing to db1.events
CREATE TABLE db2.events_alias ENGINE = Alias('db1', 'events');

-- Or using database.table format
CREATE TABLE db2.events_alias2 ENGINE = Alias('db1.events');

-- Both aliases work identically
INSERT INTO db2.events_alias VALUES (now(), 'click', 100);
SELECT * FROM db2.events_alias2;
```

<div id="write-operations">
  ### Alias를 통한 쓰기 작업
</div>

모든 쓰기 작업은 대상 테이블로 전달됩니다:

```sql
CREATE TABLE metrics (
    ts DateTime,
    metric_name String,
    value Float64
) ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE metrics_alias ENGINE = Alias('metrics');

-- Insert through alias
INSERT INTO metrics_alias VALUES 
    (now(), 'cpu_usage', 45.2),
    (now(), 'memory_usage', 78.5);

-- Insert with SELECT
INSERT INTO metrics_alias 
SELECT now(), 'disk_usage', number * 10 
FROM system.numbers 
LIMIT 5;

-- Verify data is in the target table
SELECT count() FROM metrics;  -- Returns 7
SELECT count() FROM metrics_alias;  -- Returns 7
```

<div id="schema-modification">
  ### 스키마 수정
</div>

ALTER 연산은 대상 테이블의 스키마를 수정합니다.

```sql
CREATE TABLE users (
    id UInt32,
    name String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE users_alias ENGINE = Alias('users');

-- Add column through alias
ALTER TABLE users_alias ADD COLUMN email String DEFAULT '';

-- Column is added to target table
DESCRIBE users;
```

```text
┌─name──┬─type───┬─default_type─┬─default_expression─┐
│ id    │ UInt32 │              │                    │
│ name  │ String │              │                    │
│ email │ String │ DEFAULT      │ ''                 │
└───────┴────────┴──────────────┴────────────────────┘
```

<div id="data-mutations">
  ### 데이터 뮤테이션
</div>

UPDATE 및 DELETE 연산을 지원합니다:

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float64,
    status String DEFAULT 'active'
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE products_alias ENGINE = Alias('products');

INSERT INTO products_alias VALUES 
    (1, 'item_one', 100.0, 'active'),
    (2, 'item_two', 200.0, 'active'),
    (3, 'item_three', 300.0, 'inactive');

-- Update through alias
ALTER TABLE products_alias UPDATE price = price * 1.1 WHERE status = 'active';

-- Delete through alias
ALTER TABLE products_alias DELETE WHERE status = 'inactive';

-- Changes are applied to target table
SELECT * FROM products ORDER BY id;
```

```text
┌─id─┬─name─────┬─price─┬─status─┐
│  1 │ item_one │ 110.0 │ active │
│  2 │ item_two │ 220.0 │ active │
└────┴──────────┴───────┴────────┘
```

<div id="partition-operations">
  ### 파티션 작업
</div>

파티션된 테이블에서는 파티션 작업이 대상 테이블로 전달됩니다:

```sql
CREATE TABLE logs (
    date Date,
    level String,
    message String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE logs_alias ENGINE = Alias('logs');

INSERT INTO logs_alias VALUES 
    ('2024-01-15', 'INFO', 'message1'),
    ('2024-02-15', 'ERROR', 'message2'),
    ('2024-03-15', 'INFO', 'message3');

-- Detach partition through alias
ALTER TABLE logs_alias DETACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 2 (partition 202402 detached)

-- Attach partition back
ALTER TABLE logs_alias ATTACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 3
```

<div id="table-optimization">
  ### 테이블 최적화
</div>

OPTIMIZE 작업은 대상 테이블의 파트를 머지합니다:

```sql
CREATE TABLE events (
    id UInt32,
    data String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE events_alias ENGINE = Alias('events');

-- Multiple inserts create multiple parts
INSERT INTO events_alias VALUES (1, 'data1');
INSERT INTO events_alias VALUES (2, 'data2');
INSERT INTO events_alias VALUES (3, 'data3');

-- Check parts count
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;

-- Optimize through alias
OPTIMIZE TABLE events_alias FINAL;

-- Parts are merged in target table
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;  -- Returns 1
```

<div id="alias-management">
  ### Alias 관리
</div>

Alias은 각각 이름을 변경하거나 삭제할 수 있습니다:

```sql
CREATE TABLE important_data (
    id UInt32,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO important_data VALUES (1, 'critical'), (2, 'important');

CREATE TABLE old_alias ENGINE = Alias('important_data');

-- Rename alias (target table unchanged)
RENAME TABLE old_alias TO new_alias;

-- Create another alias to same table
CREATE TABLE another_alias ENGINE = Alias('important_data');

-- Drop one alias (target table and other aliases unchanged)
DROP TABLE new_alias;

SELECT * FROM another_alias;  -- Still works
SELECT count() FROM important_data;  -- Data intact, returns 2
```