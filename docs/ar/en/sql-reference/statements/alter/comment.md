---
description: 'توثيق لأمر ALTER TABLE ... MODIFY COMMENT الذي يتيح
إضافة تعليقات الجداول أو تعديلها أو إزالتها'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

يضيف تعليقًا للجدول أو يعدّله أو يزيله، سواء أكان قد تم تعيينه سابقًا أم لا. وينعكس تغيير التعليق في كلٍّ من [`system.tables`](../../../operations/system-tables/tables.md)
واستعلام `SHOW CREATE TABLE`.

<div id="syntax">
  ## الصيغة
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## أمثلة
</div>

لإنشاء جدول يتضمّن تعليقًا:

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

لتعديل تعليق الجدول:

```sql title="Query"
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

لعرض التعليق المُعدَّل:

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

لإزالة تعليق الجدول:

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

للتحقق من إزالة التعليق:

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="caveats">
  ## محاذير
</div>

بالنسبة إلى الجداول Replicated، قد يختلف التعليق بين النسخ المتماثلة المختلفة.
ويُطبَّق تعديل التعليق على نسخة متماثلة واحدة فقط.

تتوفّر هذه الميزة بدءًا من الإصدار 23.9، ولا تعمل في إصدارات
ClickHouse الأقدم.

<div id="related-content">
  ## محتوى ذو صلة
</div>

* بند [`COMMENT`](/ar/sql-reference/statements/create/table#comment-clause)
* [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)