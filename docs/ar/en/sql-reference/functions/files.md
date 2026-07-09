---
description: 'توثيق الملفات'
sidebar_label: 'الملفات'
slug: /sql-reference/functions/files
title: 'الملفات'
doc_type: 'reference'
---

<div id="file">
  ## file
</div>

يقرأ ملفًا على هيئة سلسلة نصية ويحمّل البيانات إلى العمود المحدد. ولا يُفسَّر محتوى الملف.

انظر أيضًا دالة الجدول [file](../table-functions/file.md).

**البنية**

```sql
file(path[, default])
```

**الوسائط**

* `path` — مسار الملف بالنسبة إلى [user&#95;files&#95;path](../../operations/server-configuration-parameters/settings.md#user_files_path). يدعم أحرف البدل `*`, `**`, `?`, `{abc,def}` و `{N..M}`، حيث يكون `N` و `M` أعدادًا، و `'abc'` و `'def'` سلاسل نصية.
* `default` — القيمة التي تُعاد إذا لم يكن الملف موجودًا أو تعذّر الوصول إليه. أنواع البيانات المدعومة: [String](../data-types/string.md) و [NULL](/ar/operations/settings/formats#input_format_null_as_default).

**مثال**

إدراج البيانات من الملفين a.txt و b.txt في جدول على هيئة سلاسل نصية:

```sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```