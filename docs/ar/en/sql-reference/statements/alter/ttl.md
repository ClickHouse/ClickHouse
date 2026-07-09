---
description: 'توثيق التعامل مع table TTL'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: 'التعامل مع table TTL'
doc_type: 'reference'
---

:::note
إذا كنت تبحث عن تفاصيل حول استخدام TTL لإدارة البيانات القديمة، فاطّلع على دليل المستخدم [إدارة البيانات باستخدام TTL](/ar/guides/developer/ttl.md). توضّح المستندات أدناه كيفية تعديل قاعدة TTL حالية أو إزالتها.
:::

<div id="modify-ttl">
  ## تعديل TTL
</div>

يمكنك تعديل [TTL على مستوى الجدول](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) باستخدام طلب بالصيغة التالية:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

<div id="remove-ttl">
  ## إزالة TTL
</div>

يمكن إزالة خاصية TTL من الجدول باستخدام الاستعلام التالي:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**مثال**

لنفترض الجدول التالي مع `TTL` على مستوى الجدول:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

نفّذ `OPTIMIZE` لتنفيذ تنظيف `TTL` قسرًا:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

تم حذف الصف الثاني من الجدول.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

أزِل الآن `TTL` من الجدول باستخدام الاستعلام التالي:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

أعِد إدراج الصف المحذوف، ثم أجبر تنفيذ تنظيف `TTL` مرة أخرى باستخدام `OPTIMIZE`:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

لم يعد `TTL` موجودًا، لذلك لا يُحذف الصف الثاني:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**انظر أيضًا**

* مزيد من المعلومات حول [تعبير TTL](../../../sql-reference/statements/create/table.md#ttl-expression).
* تعديل [العمود باستخدام TTL](/ar/sql-reference/statements/alter/ttl).