---
description: 'توثيق التعامل مع فهارس تخطي البيانات'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: 'التعامل مع فهارس تخطي البيانات'
toc_hidden_folder: true
doc_type: 'reference'
---

العمليات التالية متاحة:

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - يضيف وصف الفهرس إلى البيانات الوصفية للجداول.

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - يزيل وصف الفهرس من البيانات الوصفية للجداول ويحذف ملفات الفهرس من القرص. ويُنفَّذ ذلك على هيئة [mutation](/ar/sql-reference/statements/alter/index.md#mutations).

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - يعيد إنشاء الفهرس الثانوي `name` للـ `partition_name` المحدد. يُنفَّذ على هيئة [mutation](/ar/sql-reference/statements/alter/index.md#mutations). وإذا أُهمِل الجزء `IN PARTITION`، فسيُعاد إنشاء الفهرس لبيانات الجدول بالكامل.

<div id="clear-index">
  ## CLEAR INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - يحذف ملفات الفهرس الثانوي من القرص من دون إزالة تعريفه. ويُنفَّذ ذلك على هيئة [mutation](/ar/sql-reference/statements/alter/index.md#mutations).

تُعَدّ الأوامر `ADD` و`DROP` و`CLEAR` خفيفة، بمعنى أنها لا تغيّر سوى البيانات الوصفية أو تزيل الملفات فقط.
كما أنها مُكرَّرة، إذ تُزامَن البيانات الوصفية للفهارس عبر ClickHouse Keeper أو ZooKeeper.

:::note
لا يُدعَم التعامل مع الفهارس إلا في الجداول التي تستخدم المحرك [`*MergeTree`](/ar/engines/table-engines/mergetree-family/mergetree.md) (بما في ذلك المتغيرات [المُكرَّرة](/ar/engines/table-engines/mergetree-family/replication.md)).
:::