---
description: 'يتيح الوصول إلى نظام الملفات لسرد الملفات وإرجاع بياناتها الوصفية ومحتوياتها.'
sidebar_label: 'filesystem'
sidebar_position: 62
slug: /sql-reference/table-functions/filesystem
title: 'filesystem'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="filesystem-table-function">
  # دالة الجدول filesystem
</div>

<CloudNotSupportedBadge />

تستعرض دليلًا بشكل متكرر وتُرجع جدولًا يتضمن البيانات الوصفية للملفات (المسارات، والأحجام، والأنواع، والأذونات، وأوقات التعديل)، ومعه اختياريًا محتويات الملفات.

في وضع `clickhouse-server`، يجب أن يكون المسار ضمن دليل [user&#95;files&#95;path](/ar/operations/server-configuration-parameters/settings.md#user_files_path). ويُتبع أي رابط رمزي داخل `user_files_path` يشير إلى مسار خارجه، ولكن لا تُرجع إلا العناصر التي يبدأ مسارها (عبر الرابط الرمزي) بـ `user_files_path`.

في وضع `clickhouse-local`، لا توجد قيود على المسار.

<div id="syntax">
  ## الصيغة
</div>

```sql
filesystem([path])
```

<div id="arguments">
  ## الوسائط
</div>

| المعلمة | الوصف                                                                                                                                                                                                                           |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`  | الدليل المراد سرده. يمكن أن يكون مسارًا مطلقًا (ويجب أن يكون داخل `user_files_path` في وضع الخادم) أو مسارًا نسبيًا بالنسبة إلى `user_files_path`. وإذا كان فارغًا أو غير مُحدَّد، تُستخدَم القيمة `user_files_path` افتراضيًا. |

<div id="returned_columns">
  ## الأعمدة المُعادة
</div>

| العمود              | النوع                      | الوصف                                                                                                                                                                                                                                             |
| ------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`              | `String`                   | الدليل الذي يحتوي على العنصر (ولا يتضمن اسم الملف/الدليل نفسه).                                                                                                                                                                                   |
| `name`              | `String`                   | اسم الملف أو الدليل (آخر مكوّن في المسار).                                                                                                                                                                                                        |
| `file`              | `String` (ALIAS of `name`) | اسم مستعار للعمود `name`.                                                                                                                                                                                                                         |
| `type`              | `Enum8`                    | نوع الملف: `'none'`, `'not_found'`, `'regular'`, `'directory'`, `'symlink'`, `'block'`, `'character'`, `'fifo'`, `'socket'`, `'unknown'`.                                                                                                         |
| `size`              | `Nullable(UInt64)`         | حجم الملف بالبايت (للملفات العادية). تكون القيمة `NULL` للملفات غير العادية (الدلائل، والروابط الرمزية، وما إلى ذلك) وعند حدوث خطأ.                                                                                                               |
| `depth`             | `UInt16`                   | عمق التكرار. تكون القيمة `0` للدليل المُستعلَم عنه نفسه ولعناصره الفرعية المباشرة، وتكون `1` للعناصر الأعمق بمستوى واحد، وهكذا.                                                                                                                   |
| `modification_time` | `Nullable(DateTime64(6))`  | وقت آخر تعديل بدقة الميكروثانية. تكون القيمة `NULL` عند حدوث خطأ.                                                                                                                                                                                 |
| `is_symlink`        | `Bool`                     | ما إذا كان العنصر رابطًا رمزيًا.                                                                                                                                                                                                                  |
| `content`           | `Nullable(String)`         | محتوى الملف (للملفات العادية). تكون القيمة `NULL` للملفات غير العادية (الدلائل، والروابط الرمزية، وما إلى ذلك). تؤدي أخطاء القراءة إلى رفع استثناء. تؤدي قراءة هذا العمود إلى تنفيذ عمليات I/O فعلية على الملف، لذا تجاهله إذا لم تكن بحاجة إليه. |
| `owner_read`        | `Bool`                     | للمالك إذن قراءة.                                                                                                                                                                                                                                 |
| `owner_write`       | `Bool`                     | للمالك إذن كتابة.                                                                                                                                                                                                                                 |
| `owner_exec`        | `Bool`                     | للمالك إذن تنفيذ.                                                                                                                                                                                                                                 |
| `group_read`        | `Bool`                     | للمجموعة إذن قراءة.                                                                                                                                                                                                                               |
| `group_write`       | `Bool`                     | للمجموعة إذن كتابة.                                                                                                                                                                                                                               |
| `group_exec`        | `Bool`                     | للمجموعة إذن تنفيذ.                                                                                                                                                                                                                               |
| `others_read`       | `Bool`                     | للآخرين إذن قراءة.                                                                                                                                                                                                                                |
| `others_write`      | `Bool`                     | للآخرين إذن كتابة.                                                                                                                                                                                                                                |
| `others_exec`       | `Bool`                     | للآخرين إذن تنفيذ.                                                                                                                                                                                                                                |
| `set_gid`           | `Bool`                     | البت Set-GID.                                                                                                                                                                                                                                     |
| `set_uid`           | `Bool`                     | البت Set-UID.                                                                                                                                                                                                                                     |
| `sticky_bit`        | `Bool`                     | البت المثبت.                                                                                                                                                                                                                                      |

لا تُحتسب إلا الأعمدة المستخدمة فعليًا في الاستعلام، لذا فإن اختيار مجموعة فرعية من الأعمدة (وخاصةً عند استبعاد `content`) يكون فعّالًا.

<div id="examples">
  ## أمثلة
</div>

<div id="list-files">
  ### عرض الملفات في user_files
</div>

```sql
SELECT name, type, size, depth
FROM filesystem()
ORDER BY name;
```

<div id="find-large-files">
  ### العثور على الملفات الكبيرة
</div>

```sql
SELECT path, name, size
FROM filesystem()
WHERE type = 'regular' AND size > 1000000
ORDER BY size DESC;
```

<div id="read-contents">
  ### قراءة محتويات الملف
</div>

```sql
SELECT name, content
FROM filesystem('my_directory')
WHERE name LIKE '%.csv';
```

<div id="list-immediate">
  ### إدراج العناصر الفرعية المباشرة فقط
</div>

```sql
SELECT name, type
FROM filesystem('my_directory')
WHERE depth = 0;
```