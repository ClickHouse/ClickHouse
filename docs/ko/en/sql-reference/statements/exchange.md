---
description: 'EXCHANGE SQL 문에 대한 문서'
sidebar_label: 'EXCHANGE'
sidebar_position: 49
slug: /sql-reference/statements/exchange
title: 'EXCHANGE SQL 문'
doc_type: '참고'
---

두 테이블 또는 딕셔너리의 이름을 원자적으로 맞바꿉니다.
이 작업은 임시 이름을 사용하는 [`RENAME`](./rename.md) 쿼리로도 수행할 수 있지만, 이 경우 작업은 원자적으로 수행되지 않습니다.

:::note
`EXCHANGE` 쿼리는 [`Atomic`](../../engines/database-engines/atomic.md) 및 [`Shared`](/ko/cloud/reference/shared-catalog#shared-database-engine) 데이터베이스 엔진에서만 지원됩니다.
:::

**구문**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

<div id="exchange-tables">
  ## EXCHANGE TABLES
</div>

두 테이블의 이름을 서로 바꿉니다.

**구문**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

<div id="exchange-multiple-tables">
  ### 여러 테이블 쌍 EXCHANGE
</div>

쉼표로 구분하면 단일 쿼리에서 여러 테이블 쌍을 교환할 수 있습니다.

:::note
여러 테이블 쌍을 교환할 때는 **원자적이 아니라 순차적으로** 교환됩니다. 작업 중 오류가 발생하면 일부 테이블 쌍만 교환되고 나머지는 교환되지 않을 수 있습니다.
:::

**예시**

```sql title="Query"
-- Create tables
CREATE TABLE a (a UInt8) ENGINE=Memory;
CREATE TABLE b (b UInt8) ENGINE=Memory;
CREATE TABLE c (c UInt8) ENGINE=Memory;
CREATE TABLE d (d UInt8) ENGINE=Memory;

-- Exchange two pairs of tables in one query
EXCHANGE TABLES a AND b, c AND d;

SHOW TABLE a;
SHOW TABLE b;
SHOW TABLE c;
SHOW TABLE d;
```

```sql title="Response"
-- Now table 'a' has the structure of 'b', and table 'b' has the structure of 'a'
┌─statement──────────────┐
│ CREATE TABLE default.a↴│
│↳(                     ↴│
│↳    `b` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
┌─statement──────────────┐
│ CREATE TABLE default.b↴│
│↳(                     ↴│
│↳    `a` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘

-- Now table 'c' has the structure of 'd', and table 'd' has the structure of 'c'
┌─statement──────────────┐
│ CREATE TABLE default.c↴│
│↳(                     ↴│
│↳    `d` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
┌─statement──────────────┐
│ CREATE TABLE default.d↴│
│↳(                     ↴│
│↳    `c` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
```

<div id="exchange-dictionaries">
  ## EXCHANGE DICTIONARIES
</div>

두 딕셔너리의 이름을 서로 바꿉니다.

**구문**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**관련 항목**

* [딕셔너리](./create/dictionary/overview.md)