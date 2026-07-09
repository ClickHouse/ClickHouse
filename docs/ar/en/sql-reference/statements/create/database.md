---
description: 'توثيق CREATE DATABASE'
sidebar_label: 'قاعدة البيانات'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'reference'
---

ينشئ قاعدة بيانات جديدة.

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

<div id="clauses">
  ## العبارات
</div>

<div id="if-not-exists">
  ### IF NOT EXISTS
</div>

إذا كانت قاعدة البيانات `db_name` موجودة بالفعل، فلن ينشئ ClickHouse قاعدة بيانات جديدة، و:

* لا يُطلق استثناءً إذا كانت العبارة محددة.
* يُطلق استثناءً إذا لم تكن العبارة محددة.

<div id="on-cluster">
  ### ON CLUSTER
</div>

ينشئ ClickHouse قاعدة البيانات `db_name` على جميع خوادم الكتلة المحددة. راجع مقالة [Distributed DDL](../../../sql-reference/distributed-ddl.md) لمزيد من التفاصيل.

<div id="engine">
  ### المحرّك
</div>

يستخدم ClickHouse افتراضيًا محرك قاعدة البيانات [Atomic](../../../engines/database-engines/atomic.md) الخاص به. وتوجد أيضًا المحركات [MySQL](../../../engines/database-engines/mysql.md) و[PostgresSQL](../../../engines/database-engines/postgresql.md) و[MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) و[Replicated](../../../engines/database-engines/replicated.md) و[SQLite](../../../engines/database-engines/sqlite.md).

<div id="comment">
  ### COMMENT
</div>

يمكنك إضافة تعليق إلى قاعدة البيانات عند إنشائها.

التعليق مدعوم في جميع محركات قواعد البيانات.

**الصيغة**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**مثال**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

<div id="settings">
  ### الإعدادات
</div>

<div id="lazy-load-tables">
  #### lazy_load_tables
</div>

عندما يكون مفعّلًا، لا تُحمَّل الجداول بالكامل أثناء بدء تشغيل قاعدة البيانات. وبدلًا من ذلك، يُنشأ وكيل خفيف لكل جدول، ويُنشأ محرك الجدول الفعلي عند أول وصول إليه. وهذا يقلّل وقت بدء التشغيل واستخدام الذاكرة في قواعد البيانات التي تضم عددًا كبيرًا من الجداول ولا يُستعلَم بنشاط إلا عن مجموعة فرعية منها.

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

ينطبق على محركات قواعد البيانات التي تخزّن البيانات الوصفية للجداول على القرص (مثل `Atomic` و`Ordinary`). تُحمَّل العروض، والعروض المادية، والقواميس، والجداول المستندة إلى دوال الجدول دائمًا بشكل فوري بغض النظر عن هذا الإعداد.

**متى يُستخدم:** يفيد هذا الإعداد في قواعد البيانات التي تحتوي على عدد كبير من الجداول (بالمئات أو الآلاف)، حيث لا يُستعلَم فعليًا إلا عن مجموعة فرعية منها. ويقلّل وقت بدء تشغيل الخادم واستهلاك الذاكرة من خلال تأجيل إنشاء كائنات محرك الجدول، وفحص أجزاء البيانات، وتهيئة الخيوط الخلفية حتى أول وصول.

**التأثير على `system.tables`:**

* قبل الوصول إلى الجدول، يُظهر `system.tables` محركه على أنه `TableProxy`. وبعد أول وصول، يُظهر اسم المحرك الحقيقي (مثل `MergeTree`).
* تُرجع الأعمدة مثل `total_rows` و`total_bytes` القيمة `NULL` للجداول غير المحمّلة لأن وحدة التخزين الفعلية لم تُنشأ بعد.

**التفاعل مع عمليات DDL:**

* تؤدي أوامر `SELECT` و`INSERT` و`ALTER` و`DROP` تلقائيًا إلى تحميل محرك الجدول الحقيقي عند أول استخدام.
* يعمل `RENAME TABLE` من دون التسبب في التحميل.
* بمجرد تحميل جدول، يظل محمّلًا طوال عمر عملية الخادم.

**القيود:**

* قد تعرض أدوات المراقبة التي تعتمد على البيانات الوصفية في `system.tables` (مثل `total_rows` و`engine`) معلومات غير مكتملة للجداول غير المحمّلة.
* يترتب على أول استعلام إلى جدول غير محمّل تكلفة تحميل لمرة واحدة (تحليل عبارة `CREATE TABLE` المخزّنة وتهيئة المحرك).

القيمة الافتراضية: `0` (معطّل).