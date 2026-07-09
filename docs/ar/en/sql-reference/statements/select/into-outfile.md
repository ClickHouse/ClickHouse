---
description: 'توثيق عبارة INTO OUTFILE'
sidebar_label: 'INTO OUTFILE'
slug: /sql-reference/statements/select/into-outfile
title: 'عبارة INTO OUTFILE'
doc_type: 'مرجع'
---

تعيد عبارة `INTO OUTFILE` توجيه نتيجة استعلام `SELECT` إلى ملف على جانب **العميل**.

الملفات المضغوطة مدعومة. ويُكتشف نوع الضغط من امتداد اسم الملف (ويُستخدم الوضع `'auto'` افتراضيًا). كما يمكن تحديده صراحةً في عبارة `COMPRESSION`. ويمكن تحديد مستوى الضغط لنوع ضغط معيّن في عبارة `LEVEL`.

**الصيغة**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` و`type` هما قيمتان حرفيتان نصيتان. أنواع الضغط المدعومة هي: `'none'` و`'gzip'` و`'deflate'` و`'br'` و`'xz'` و`'zstd'` و`'lz4'` و`'bz2'`.

`level` هو قيمة حرفية رقمية. تُدعَم الأعداد الصحيحة الموجبة ضمن النطاقات التالية: `1-12` للنوع `lz4`، و`1-22` للنوع `zstd`، و`1-9` لأنواع الضغط الأخرى.

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

* هذه الوظيفة متاحة في [عميل سطر الأوامر](../../../interfaces/client.md) و[clickhouse-local](../../../operations/utilities/clickhouse-local.md). لذلك سيفشل أي استعلام يُرسَل عبر [واجهة HTTP](/ar/interfaces/http).
* سيفشل الاستعلام إذا كان هناك ملف موجود بالفعل يحمل اسم الملف نفسه.
* [تنسيق الإخراج](../../../interfaces/formats.md) الافتراضي هو `TabSeparated` (كما في وضع الدُفعات لعميل سطر الأوامر). استخدم عبارة [FORMAT](format.md) لتغييره.
* إذا ورد `AND STDOUT` في الاستعلام، فسيُعرَض أيضًا على standard output الناتج الذي يُكتَب إلى الملف. وإذا استُخدم مع الضغط، فسيُعرَض النص الخام على standard output.
* إذا ورد `APPEND` في الاستعلام، فسيُلحَق الناتج بملف موجود. وإذا استُخدم الضغط، فلا يمكن استخدام `APPEND`.
* عند الكتابة إلى ملف موجود بالفعل، يجب استخدام `APPEND` أو `TRUNCATE`.

**مثال**

نفّذ الاستعلام التالي باستخدام [عميل سطر الأوامر](../../../interfaces/client.md):

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

```text title="Response"
1,"ABC"
```