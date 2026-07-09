---
description: 'يوفر واجهة شبيهة بالجداول للقراءة فقط لجداول Apache Hudi المخزّنة في
  Amazon S3.'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

يوفر واجهة شبيهة بالجداول للقراءة فقط لجداول Apache [Hudi](https://hudi.apache.org/) المخزّنة في Amazon S3.

<div id="syntax">
  ## الصيغة
</div>

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## الوسيطات
</div>

| Argument                                     | Description                                                                                                                                                                                                                                                                                                                                                             |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                                        | عنوان URL للحاوية مع المسار إلى جدول Hudi موجود في S3.                                                                                                                                                                                                                                                                                                                  |
| `aws_access_key_id`, `aws_secret_access_key` | بيانات الاعتماد طويلة الأجل الخاصة بمستخدم حساب [AWS](https://aws.amazon.com/). يمكنك استخدامها للمصادقة على طلباتك. هذه المعلَمات اختيارية. إذا لم يتم تحديد بيانات الاعتماد، فستُستخدم بيانات الاعتماد من إعدادات ClickHouse. لمزيد من المعلومات، راجع [استخدام S3 لتخزين البيانات](/ar/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | [تنسيق](/ar/interfaces/formats) الملف.                                                                                                                                                                                                                                                                                                                                     |
| `structure`                                  | بنية الجدول. التنسيق: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                    |
| `compression`                                | المعلَم اختياري. القيم المدعومة: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. افتراضيًا، سيُكتشف الضغط تلقائيًا استنادًا إلى امتداد الملف.                                                                                                                                                                                                                    |
| `extra_credentials`                          | المعلَم اختياري. يُستخدم لتمرير `role_arn` للوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [تأمين S3](/ar/cloud/data-sources/secure-s3) لمعرفة خطوات الإعداد.                                                                                                                                                                                                         |

<div id="returned_value">
  ## القيمة المُعادة
</div>

جدول بالبنية المحددة لقراءة البيانات من جدول Hudi المحدد في S3.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_etag` — قيمة `etag` الخاصة بالملف. النوع: `LowCardinality(String)`. إذا كانت قيمة `etag` غير معروفة، تكون القيمة `NULL`.

<div id="related">
  ## موضوعات ذات صلة
</div>

* [محرك Hudi](/ar/engines/table-engines/integrations/hudi.md)
* [دالة جدول Hudi للعنقود](/ar/sql-reference/table-functions/hudiCluster.md)