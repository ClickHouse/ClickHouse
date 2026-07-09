---
description: 'يتيح لك محرك قاعدة البيانات DataLakeCatalog ربط ClickHouse بكتالوجات البيانات الخارجية والاستعلام عن البيانات المخزنة بتنسيقات الجداول المفتوحة'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

يتيح لك محرك قاعدة البيانات `DataLakeCatalog` ربط ClickHouse بكتالوجات البيانات الخارجية
والاستعلام عن البيانات المخزنة بتنسيقات الجداول المفتوحة دون الحاجة إلى تكرار البيانات.
وهذا يحوّل ClickHouse إلى محرك استعلام قوي يعمل بسلاسة مع
البنية التحتية الحالية لبحيرة البيانات لديك.

<div id="supported-catalogs">
  ## الكتالوجات المدعومة
</div>

يدعم محرك `DataLakeCatalog` كتالوجات البيانات التالية:

* **AWS Glue Catalog** - لجداول Iceberg في بيئات AWS
* **Databricks Unity Catalog** - لجداول Delta Lake وIceberg
* **Hive Metastore** - كتالوج تقليدي ضمن منظومة Hadoop
* **REST Catalogs** - أي كتالوج يدعم مواصفة REST الخاصة بـ Iceberg

<div id="creating-a-database">
  ## إنشاء قاعدة بيانات
</div>

ستحتاج إلى تمكين الإعدادات ذات الصلة التالية لاستخدام محرك `DataLakeCatalog`:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

يمكن إنشاء قواعد بيانات تستخدم المحرك `DataLakeCatalog` باستخدام الصيغة التالية:

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

الإعدادات التالية مدعومة:

| الإعداد                 | الوصف                                                                                                                                                                                                                                                                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `catalog_type`          | نوع الـ كتالوج: `glue`، `unity` (Delta)، `rest` (Iceberg)، `hive`، `onelake` (Iceberg)                                                                                                                                                                                                                                                           |
| `warehouse`             | اسم الـ warehouse/قاعدة البيانات المطلوب استخدامها في الـ كتالوج.                                                                                                                                                                                                                                                                                |
| `catalog_credential`    | بيانات اعتماد المصادقة الخاصة بالـ كتالوج (مثل مفتاح API أو رمز مميز)                                                                                                                                                                                                                                                                            |
| `auth_header`           | ترويسة HTTP مخصّصة للمصادقة مع خدمة الـ كتالوج                                                                                                                                                                                                                                                                                                   |
| `auth_scope`            | نطاق OAuth2 للمصادقة (إذا كنت تستخدم OAuth)                                                                                                                                                                                                                                                                                                       |
| `storage_endpoint`      | عنوان URL لنقطة النهاية الخاصة بالتخزين الأساسي                                                                                                                                                                                                                                                                                                   |
| `oauth_server_uri`      | عنوان URI لخادم تفويض OAuth2 للمصادقة                                                                                                                                                                                                                                                                                                             |
| `vended_credentials`    | قيمة Boolean تشير إلى ما إذا كان يجب استخدام بيانات الاعتماد التي يوفّرها الـ كتالوج (يدعم AWS S3 وAzure ADLS Gen2)                                                                                                                                                                                                                              |
| `aws_access_key_id`     | معرّف مفتاح الوصول في AWS للوصول إلى S3/Glue (إذا لم تكن تستخدم بيانات الاعتماد التي يوفّرها الـ كتالوج)                                                                                                                                                                                                                                         |
| `aws_secret_access_key` | مفتاح الوصول السري في AWS للوصول إلى S3/Glue (إذا لم تكن تستخدم بيانات الاعتماد التي يوفّرها الـ كتالوج)                                                                                                                                                                                                                                         |
| `region`                | منطقة AWS الخاصة بالخدمة (مثل `us-east-1`)                                                                                                                                                                                                                                                                                                        |
| `dlf_access_key_id`     | معرّف مفتاح الوصول للوصول إلى DLF                                                                                                                                                                                                                                                                                                                 |
| `dlf_access_key_secret` | المفتاح السري لمفتاح الوصول للوصول إلى DLF                                                                                                                                                                                                                                                                                                        |
| `force_add_bucket`      | عند إنشاء عناوين URL لتخزين الكائنات من موقع الجدول الذي يوفّره الـ كتالوج و`storage_endpoint`، أضِف اسم الـ bucket/Container في البداية حتى إذا كانت نقطة النهاية تتضمنه بالفعل. القيمة الافتراضية: `false`. اضبطها على `true` مع الـ كتالوجات التي تُرجِع مسارات من دون الـ bucket وتتطلب إضافته عند إنشاء عنوان URL (مسارات على نمط Polaris). |

<div id="examples">
  ## أمثلة
</div>

راجع الأقسام أدناه للاطلاع على أمثلة لاستخدام المحرك `DataLakeCatalog`:

* [Unity Catalog](/ar/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/ar/use-cases/data-lake/glue-catalog)
* كتالوج OneLake
  يمكن استخدامه من خلال تفعيل `allow_experimental_database_iceberg` أو `allow_database_iceberg`.

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
    catalog_type = 'onelake',
    warehouse = warehouse,
    onelake_tenant_id = tenant_id,
    oauth_server_uri = server_uri,
    auth_scope = auth_scope,
    onelake_client_id = client_id,
    onelake_client_secret = client_secret;
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```