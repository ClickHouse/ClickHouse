---
description: 'امتداد لدالة الجدول iceberg يتيح معالجة الملفات من Apache Iceberg
  بالتوازي عبر عدة عُقد ضمن عنقود محدد.'
sidebar_label: 'icebergCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/icebergCluster
title: 'icebergCluster'
doc_type: 'reference'
---

هذا امتداد لدالة الجدول [iceberg](/ar/sql-reference/table-functions/iceberg.md).

يتيح معالجة الملفات من [Apache Iceberg](https://iceberg.apache.org/) بالتوازي عبر عدة عُقد ضمن عنقود محدد. على initiator، يُنشئ connection مع جميع العُقد في العنقود ويوزّع كل ملف ديناميكيًا. وعلى عقدة worker، يطلب من initiator معرفة task التالية المطلوب معالجتها ثم يعالجها. ويتكرر ذلك حتى تكتمل جميع tasks.

<div id="syntax">
  ## البنية
</div>

```sql
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])

icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])

icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## الوسيطات
</div>

* `cluster_name` — اسم عنقود يُستخدم لبناء مجموعة من العناوين ومعلمات الاتصال بالخوادم المحلية والبعيدة.
* يتوافق وصف جميع الوسيطات الأخرى مع وصف الوسيطات في دالة الجدول المكافئة [iceberg](/ar/sql-reference/table-functions/iceberg.md).
* يمكن استخدام المعامل الاختياري `extra_credentials` لتمرير `role_arn` من أجل الوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [Secure S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات الإعداد.

**القيمة المعادة**

جدول بالبنية المحددة لقراءة البيانات من العنقود في جدول Iceberg المحدد.

**أمثلة**

```sql
SELECT * FROM icebergS3Cluster('cluster_simple', 'http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_etag` — قيمة etag الخاصة بالملف. النوع: `LowCardinality(String)`. إذا كانت قيمة etag غير معروفة، تكون القيمة `NULL`.

**انظر أيضًا**

* [محرك Iceberg](/ar/engines/table-engines/integrations/iceberg.md)
* [دالة جدول Iceberg](/ar/sql-reference/table-functions/iceberg.md)