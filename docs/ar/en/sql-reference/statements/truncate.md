---
description: 'توثيق عبارات TRUNCATE'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'عبارات TRUNCATE'
doc_type: 'مرجع'
---

تُستخدم عبارة `TRUNCATE` في ClickHouse لإزالة جميع البيانات بسرعة من جدول أو قاعدة بيانات مع الحفاظ على بنيتهما.

<div id="truncate-table">
  ## TRUNCATE TABLE
</div>

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<br />

| Parameter            | Description                                                                                                                                  |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `IF EXISTS`          | يمنع ظهور خطأ إذا لم يكن الجدول موجودًا. وإذا لم يُستخدم، فسيُرجع الاستعلام خطأ.                                                             |
| `db.name`            | اسم قاعدة البيانات اختياري.                                                                                                                  |
| `ON CLUSTER cluster` | يشغّل الأمر على مستوى عنقود محدد.                                                                                                            |
| `SYNC`               | يجعل عملية التفريغ متزامنة عبر النسخ المتماثلة عند استخدام الجداول المكررة. وإذا لم يُستخدم، فستحدث عملية التفريغ بشكل غير متزامن افتراضيًا. |

يمكنك استخدام الإعداد [alter&#95;sync](/ar/operations/settings/settings#alter_sync) لضبط انتظار تنفيذ الإجراءات على النسخ المتماثلة.

يمكنك تحديد مدة الانتظار (بالثواني) لكي تنفذ النسخ المتماثلة غير النشطة استعلامات `TRUNCATE` باستخدام الإعداد [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ar/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
إذا كان `alter_sync` مضبوطًا على `2` وكانت بعض النسخ المتماثلة غير نشطة لمدة تتجاوز الوقت المحدد بواسطة الإعداد `replication_wait_for_inactive_replica_timeout`، فسيتم إطلاق الاستثناء `UNFINISHED`.
:::

استعلام `TRUNCATE TABLE` **غير مدعوم** لمحركات الجداول التالية:

* [`View`](../../engines/table-engines/special/view.md)
* [`File`](../../engines/table-engines/special/file.md)
* [`URL`](../../engines/table-engines/special/url.md)
* [`Buffer`](../../engines/table-engines/special/buffer.md)
* [`Null`](../../engines/table-engines/special/null.md)

<div id="truncate-all-tables">
  ## تنفيذ TRUNCATE على جميع الجداول
</div>

```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```

<br />

| المعلمة                                 | الوصف                                            |
| --------------------------------------- | ------------------------------------------------ |
| `ALL`                                   | يزيل البيانات من جميع الجداول في قاعدة البيانات. |
| `IF EXISTS`                             | يمنع حدوث خطأ إذا لم تكن قاعدة البيانات موجودة.  |
| `db`                                    | اسم قاعدة البيانات.                              |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | يُصفّي الجداول وفق النمط.                        |
| `ON CLUSTER cluster`                    | يشغّل الأمر عبر العنقود.                         |

يزيل جميع البيانات من جميع الجداول في قاعدة بيانات.

<div id="truncate-database">
  ## TRUNCATE DATABASE
</div>

```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

<br />

| المعلمة              | الوصف                                             |
| -------------------- | ------------------------------------------------- |
| `IF EXISTS`          | يمنع حدوث خطأ إذا كانت قاعدة البيانات غير موجودة. |
| `db`                 | اسم قاعدة البيانات.                               |
| `ON CLUSTER cluster` | ينفّذ الأمر على عنقود محدد.                     |

يزيل جميع الجداول من قاعدة بيانات مع الإبقاء على قاعدة البيانات نفسها. وعند عدم تضمين العبارة `IF EXISTS`، يعيد الاستعلام خطأ إذا كانت قاعدة البيانات غير موجودة.

:::note
`TRUNCATE DATABASE` غير مدعوم لقواعد البيانات `Replicated`. وبدلاً من ذلك، استخدم `DROP` لقاعدة البيانات ثم `CREATE` لها من جديد.
:::