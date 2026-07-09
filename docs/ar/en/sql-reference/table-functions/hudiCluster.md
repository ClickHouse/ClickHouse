---
description: 'امتداد لدالة الجدول hudi. يتيح معالجة الملفات من جداول Apache Hudi في
  Amazon S3 بالتوازي عبر العديد من العُقد ضمن عنقود محدد.'
sidebar_label: 'hudiCluster'
sidebar_position: 86
slug: /sql-reference/table-functions/hudiCluster
title: 'دالة الجدول hudiCluster'
doc_type: 'مرجع'
---

هذا امتداد لدالة الجدول [hudi](/ar/sql-reference/table-functions/hudi.md).

يتيح معالجة الملفات من جداول Apache [Hudi](https://hudi.apache.org/) في Amazon S3 بالتوازي عبر العديد من العُقد ضمن عنقود محدد. على العقدة البادئة، يُنشئ اتصالًا بجميع العُقد في العنقود ويوزّع كل ملف ديناميكيًا. وعلى العقدة العاملة، يطلب من العقدة البادئة المهمة التالية لمعالجتها ثم يعالجها. ويتكرر ذلك حتى تكتمل جميع المهام.

<div id="syntax">
  ## البنية
</div>

```sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## المعاملات
</div>

| المعامل                                      | الوصف                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                               | اسم عنقود يُستخدم لإنشاء مجموعة من العناوين ومعاملات الاتصال بالخوادم البعيدة والمحلية.                                                                                                                                                                                                                                               |
| `url`                                        | عنوان URL للحاوية مع المسار إلى جدول Hudi موجود في S3.                                                                                                                                                                                                                                                                                  |
| `aws_access_key_id`, `aws_secret_access_key` | بيانات اعتماد طويلة الأجل لمستخدم حساب [AWS](https://aws.amazon.com/). يمكنك استخدامها لمصادقة طلباتك. هذه المعاملات اختيارية. إذا لم يتم تحديد بيانات الاعتماد، فستُستخدم من تهيئة ClickHouse. لمزيد من المعلومات، راجع [استخدام S3 لتخزين البيانات](/ar/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | [تنسيق](/ar/interfaces/formats) الملف.                                                                                                                                                                                                                                                                                                     |
| `structure`                                  | بنية الجدول. التنسيق: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                    |
| `compression`                                | هذه المعلمة اختيارية. القيم المدعومة: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. افتراضيًا، سيُكتشف الضغط تلقائيًا من امتداد الملف.                                                                                                                                                                                         |
| `extra_credentials`                          | هذه المعلمة اختيارية. تُستخدم لتمرير `role_arn` للوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [Secure S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات التهيئة.                                                                                                                                                              |

<div id="returned_value">
  ## القيمة المُعادة
</div>

جدول بالبنية المحددة لقراءة البيانات من العنقود من جدول Hudi المحدد في S3.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، فستكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، فستكون القيمة `NULL`.
* `_etag` — etag الخاص بالملف. النوع: `LowCardinality(String)`. إذا كان etag غير معروف، فستكون القيمة `NULL`.

<div id="related">
  ## ذات صلة
</div>

* [محرك Hudi](/ar/engines/table-engines/integrations/hudi.md)
* [دالة الجدول Hudi](/ar/sql-reference/table-functions/hudi.md)