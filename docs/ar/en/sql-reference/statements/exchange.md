---
description: 'توثيق تعليمة EXCHANGE'
sidebar_label: 'EXCHANGE'
sidebar_position: 49
slug: /sql-reference/statements/exchange
title: 'تعليمة EXCHANGE'
doc_type: 'reference'
---

يستبدل اسمي جدولين أو قاموسين بشكل ذري.
ويمكن أيضًا تنفيذ هذه المهمة باستخدام استعلام [`RENAME`](./rename.md) مع اسم مؤقت، لكن العملية في هذه الحالة لا تكون ذرية.

:::note
لا يدعم استعلام `EXCHANGE` إلا محركَي قاعدة البيانات [`Atomic`](../../engines/database-engines/atomic.md) و[`Shared`](/ar/cloud/reference/shared-catalog#shared-database-engine).
:::

**الصيغة**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

<div id="exchange-tables">
  ## EXCHANGE TABLES
</div>

يبدّل اسمي جدولين.

**الصياغة**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

<div id="exchange-multiple-tables">
  ### EXCHANGE لعدة جداول
</div>

يمكنك تبديل عدة أزواج من الجداول في استعلام واحد، وذلك بفصلها بفواصل.

:::note
عند تبديل عدة أزواج من الجداول، تُنفَّذ عمليات التبديل **بالتسلسل، وليس بشكل ذرّي**. وإذا حدث خطأ أثناء العملية، فقد تكون بعض أزواج الجداول قد تبدّلت، بينما لم تتبدّل أزواج أخرى.
:::

**مثال**

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

يبدّل أسماء قاموسين.

**البنية**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**راجع أيضًا**

* [القواميس](./create/dictionary/overview.md)