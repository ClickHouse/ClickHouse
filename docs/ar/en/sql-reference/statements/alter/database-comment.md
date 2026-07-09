---
description: 'توثيق لعبارات ALTER DATABASE ... MODIFY COMMENT
التي تتيح إضافة تعليق إلى قاعدة البيانات أو تعديله أو إزالته.'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'عبارات ALTER DATABASE ... MODIFY COMMENT'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

يضيف تعليقًا إلى قاعدة البيانات أو يعدّله أو يزيله، بغضّ النظر عمّا إذا كان قد تم تعيينه
سابقًا أم لا. ينعكس تغيير التعليق في كلٍّ من [`system.databases`](/ar/operations/system-tables/databases.md)
واستعلام `SHOW CREATE DATABASE`.

<div id="syntax">
  ## الصيغة
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## أمثلة
</div>

لإنشاء `DATABASE` مع إضافة تعليق:

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

لتعديل التعليق:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

لعرض التعليق المُعدَّل:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE name = 'database_with_comment';
```

```text title="Response"
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

لإزالة تعليق قاعدة البيانات:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

للتحقق من أن التعليق قد أُزيل:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE  name = 'database_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="related-content">
  ## محتوى ذي صلة
</div>

* عبارة [`COMMENT`](/ar/sql-reference/statements/create/table#comment-clause)
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)