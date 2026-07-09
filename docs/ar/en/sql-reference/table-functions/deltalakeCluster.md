---
description: 'هذا امتداد لدالة الجدول deltaLake.'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
doc_type: 'reference'
---

هذا امتداد لدالة الجدول [deltaLake](/ar/sql-reference/table-functions/deltalake.md).

يتيح معالجة الملفات من جداول [Delta Lake](https://github.com/delta-io/delta) في Amazon S3 بالتوازي عبر عدة عقد ضمن عنقود محدد. على العقدة المُبادِئة، يُنشئ اتصالًا بجميع العقد في العنقود ويوزّع كل ملف ديناميكيًا. وعلى العقدة العاملة، يستعلم من العقدة المُبادِئة عن المهمة التالية لمعالجتها ثم يعالجها. ويتكرر ذلك حتى تكتمل جميع المهام.

<div id="syntax">
  ## الصيغة
</div>

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```

`deltaLakeS3Cluster` هو اسم مستعار لـ `deltaLakeCluster`، وكلاهما خاص بـ S3.

<div id="arguments">
  ## الوسيطات
</div>

* `cluster_name` — اسم عنقود يُستخدم لتكوين مجموعة من العناوين ومعلمات الاتصال بالخوادم المحلية والبعيدة.
* يتطابق وصف جميع الوسيطات الأخرى مع وصف الوسيطات في دالة الجدول المناظرة [deltaLake](/ar/sql-reference/table-functions/deltalake.md).
* يمكن استخدام المعلمة الاختيارية `extra_credentials` لتمرير `role_arn` بغرض الوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [Secure S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات الإعداد.

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات من العنقود في جدول Delta Lake المحدد على S3.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_etag` — قيمة `etag` الخاصة بالملف. النوع: `LowCardinality(String)`. إذا كانت قيمة `etag` غير معروفة، تكون القيمة `NULL`.

<div id="related">
  ## ذات صلة
</div>

* [محرك DeltaLake](/ar/engines/table-engines/integrations/deltalake.md)
* [دالة الجدول DeltaLake](/ar/sql-reference/table-functions/deltalake.md)