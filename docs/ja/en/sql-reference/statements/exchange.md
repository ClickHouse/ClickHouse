---
description: 'EXCHANGE ステートメントのドキュメント'
sidebar_label: 'EXCHANGE'
sidebar_position: 49
slug: /sql-reference/statements/exchange
title: 'EXCHANGE ステートメント'
doc_type: 'reference'
---

2 つのテーブルまたは Dictionary の名前をアトミックに入れ替えます。
この処理は、一時的な名前を使用した [`RENAME`](./rename.md) クエリでも実行できますが、その場合、この操作はアトミックではありません。

:::note
`EXCHANGE` クエリは、[`Atomic`](../../engines/database-engines/atomic.md) および [`Shared`](/ja/cloud/reference/shared-catalog#shared-database-engine) データベースエンジンでのみサポートされています。
:::

**構文**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

<div id="exchange-tables">
  ## EXCHANGE TABLES
</div>

2つのテーブル名を入れ替えます。

**構文**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

<div id="exchange-multiple-tables">
  ### 複数のテーブルを EXCHANGE する
</div>

複数のテーブルの組を 1 つのクエリで EXCHANGE するには、各組をカンマで区切って指定します。

:::note
複数のテーブルの組を EXCHANGE する場合、EXCHANGE は **アトミックではなく順次** 実行されます。操作中にエラーが発生すると、一部のテーブルの組だけが EXCHANGE され、他の組はされない可能性があります。
:::

**例**

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

2つのDictionaryの名前を入れ替えます。

**構文**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**関連項目**

* [Dictionaries](./create/dictionary/overview.md)