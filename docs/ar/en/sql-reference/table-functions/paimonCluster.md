---
description: 'امتداد لدالة الجدول paimon يتيح معالجة الملفات
  من Apache Paimon بالتوازي عبر عدة عُقد ضمن عنقود محدد.'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimoncluster-table-function">
  # الدالة الجدولية paimonCluster
</div>

<ExperimentalBadge />

هذا امتداد للدالة الجدولية [paimon](/ar/sql-reference/table-functions/paimon.md).

يتيح معالجة الملفات من Apache [Paimon](https://paimon.apache.org/) بالتوازي عبر عدة عُقد في عنقود محدد. على العقدة البادئة، يُنشئ اتصالًا بجميع العُقد في العنقود ويوزّع كل ملف ديناميكيًا. وعلى العقدة العاملة، يطلب من العقدة البادئة المهمة التالية لمعالجتها ثم يعالجها. ويتكرر ذلك حتى تكتمل جميع المهام.

<div id="syntax">
  ## الصيغة
</div>

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## الوسائط
</div>

* `cluster_name` — اسم عنقود يُستخدم لإنشاء مجموعة من العناوين ومعلمات الاتصال بالخوادم البعيدة والمحلية.
* يتطابق وصف جميع الوسائط الأخرى مع وصف الوسائط في دالة الجدول المكافئة [paimon](/ar/sql-reference/table-functions/paimon.md).
* يمكن استخدام المعلَمة الاختيارية `extra_credentials` لتمرير `role_arn` من أجل الوصول المستند إلى الدور في ClickHouse Cloud. راجع [Secure S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات التهيئة.

**القيمة المُعادة**

جدول بالبنية المحددة لقراءة البيانات من العنقود في جدول Paimon المحدد.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_etag` — قيمة `etag` الخاصة بالملف. النوع: `LowCardinality(String)`. إذا كانت قيمة `etag` غير معروفة، تكون القيمة `NULL`.

**انظر أيضًا**

* [دالة الجدول Paimon](/ar/sql-reference/table-functions/paimon.md)