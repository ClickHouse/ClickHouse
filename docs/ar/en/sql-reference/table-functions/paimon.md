---
description: 'يوفّر واجهة شبيهة بالجدول للقراءة فقط لجداول Apache Paimon المخزّنة
  في Amazon S3 أو Azure أو HDFS أو محليًا.'
sidebar_label: 'paimon'
sidebar_position: 90
slug: /sql-reference/table-functions/paimon
title: 'paimon'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimon-table-function">
  # دالة الجدول paimon
</div>

<ExperimentalBadge />

توفّر واجهة شبيهة بالجدول للقراءة فقط للوصول إلى جداول Apache [Paimon](https://paimon.apache.org/) المخزّنة على Amazon S3 أو Azure أو HDFS أو محليًا.

<div id="syntax">
  ## البنية
</div>

```sql
paimon(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonS3(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFS(path_to_table, [,format] [,compression_method])

paimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## الوسائط
</div>

يتوافق وصف الوسائط مع وصفها في دوال الجداول `s3` و`azureBlobStorage` و`HDFS` و`file`.
ويمثل `format` تنسيق ملفات البيانات في جدول Paimon.

بالنسبة إلى `paimonS3`، يمكن استخدام المعلَمة الاختيارية `extra_credentials` لتمرير `role_arn` من أجل الوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [Secure S3](/ar/cloud/data-sources/secure-s3) للاطلاع على خطوات الإعداد.

<div id="returned-value">
  ### القيمة المعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات من جدول Paimon المحدد.

<div id="defining-a-named-collection">
  ## تعريف مجموعة مُسمّاة
</div>

فيما يلي مثال على إعداد مجموعة مُسمّاة لتخزين URL وبيانات الاعتماد:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM paimonS3(paimon_conf, filename = 'test_table')
DESCRIBE paimonS3(paimon_conf, filename = 'test_table')
```

<div id="aliases">
  ## الأسماء المستعارة
</div>

دالة الجدول `paimon` هي الآن اسم مستعار لـ `paimonS3`.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_etag` — قيمة `etag` الخاصة بالملف. النوع: `LowCardinality(String)`. إذا كانت قيمة `etag` غير معروفة، تكون القيمة `NULL`.

<div id="data-types-supported">
  ## أنواع البيانات المدعومة
</div>

| نوع بيانات Paimon                 | نوع بيانات ClickHouse     |
| --------------------------------- | ------------------------- |
| BOOLEAN                           | Int8                      |
| TINYINT                           | Int8                      |
| SMALLINT                          | Int16                     |
| INTEGER                           | Int32                     |
| BIGINT                            | Int64                     |
| FLOAT                             | Float32                   |
| DOUBLE                            | Float64                   |
| STRING,VARCHAR,BYTES,VARBINARY    | String                    |
| DATE                              | Date                      |
| TIME(p),TIME                      | Time(&#39;UTC&#39;)       |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | DateTime64                |
| TIMESTAMP(p)                      | DateTime64(&#39;UTC&#39;) |
| CHAR                              | FixedString(1)            |
| BINARY(n)                         | FixedString(n)            |
| DECIMAL(P,S)                      | Decimal(P,S)              |
| ARRAY                             | Array                     |
| MAP                               | Map                       |

<div id="partition-supported">
  ## التقسيم المدعوم
</div>

أنواع البيانات المدعومة في مفاتيح التقسيم في Paimon:

* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`

<div id="see-also">
  ## انظر أيضًا
</div>

* [دالة الجدول العنقودية لـ Paimon](/ar/sql-reference/table-functions/paimonCluster.md)