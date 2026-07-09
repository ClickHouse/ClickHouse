---
description: 'توثيق UNDROP TABLE'
sidebar_label: 'UNDROP'
slug: /sql-reference/statements/undrop
title: 'UNDROP TABLE'
doc_type: 'مرجع'
---

يلغي عملية إسقاط الجدول.

اعتبارًا من الإصدار 23.3 من ClickHouse، أصبح من الممكن تنفيذ UNDROP لجدول في قاعدة بيانات Atomic
خلال المهلة `database_atomic_delay_before_drop_table_sec` (8 دقائق افتراضيًا) بعد تنفيذ عبارة DROP TABLE. وتُدرَج الجداول التي أُسقِطت في
جدول نظام يسمى `system.dropped_tables`.

إذا كان لديك materialized view بدون بند `TO` مرتبط بالجدول الذي أُسقِط، فسيتعين عليك أيضًا تنفيذ UNDROP للجدول الداخلي لذلك العرض.

:::tip
راجع أيضًا [DROP TABLE](/ar/sql-reference/statements/drop.md)
:::

الصيغة:

```sql
UNDROP TABLE [db.]name [UUID '<uuid>'] [ON CLUSTER cluster]
```

**مثال**

```sql
CREATE TABLE tab
(
    `id` UInt8
)
ENGINE = MergeTree
ORDER BY id;

DROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;
```

```response
Row 1:
──────
index:                 0
database:              default
table:                 tab
uuid:                  aa696a1a-1d70-4e60-a841-4c80827706cc
engine:                MergeTree
metadata_dropped_path: /var/lib/clickhouse/metadata_dropped/default.tab.aa696a1a-1d70-4e60-a841-4c80827706cc.sql
table_dropped_time:    2023-04-05 14:12:12

1 row in set. Elapsed: 0.001 sec. 
```

````sql
UNDROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;

```response
Ok.

0 rows in set. Elapsed: 0.001 sec. 
````

```sql
DESCRIBE TABLE tab
FORMAT Vertical;
```

```response
Row 1:
──────
name:               id
type:               UInt8
default_type:       
default_expression: 
comment:            
codec_expression:   
ttl_expression:     
```