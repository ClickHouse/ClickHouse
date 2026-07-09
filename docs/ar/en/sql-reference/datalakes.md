---
description: 'توثيق بحيرات البيانات'
sidebar_label: 'بحيرات البيانات'
sidebar_position: 2
slug: /sql-reference/datalakes
title: 'بحيرات البيانات'
doc_type: 'reference'
---

في هذا القسم، سنستعرض دعم ClickHouse لبحيرات البيانات.
يدعم ClickHouse العديد من تنسيقات الجداول وكتالوجات البيانات الأكثر شيوعًا، بما في ذلك Iceberg وDelta Lake وHudi وAWS Glue وREST Catalog وUnity Catalog وMicrosoft OneLake.

<div id="open-table-formats">
  # صيغ الجداول المفتوحة
</div>

<div id="iceberg">
  ## Iceberg
</div>

راجع [iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg)، التي تدعم القراءة من Amazon S3 والخدمات المتوافقة مع S3 وHDFS وAzure وأنظمة الملفات المحلية. وتُعد [icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster) النسخة الموزعة من الدالة `iceberg`.

<div id="delta-lake">
  ## Delta Lake
</div>

راجع [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake)، الذي يدعم القراءة من Amazon S3 والخدمات المتوافقة مع S3 وAzure وأنظمة الملفات المحلية. وتُعد [deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) النسخة الموزعة من الدالة `deltaLake`.

<div id="hudi">
  ## Hudi
</div>

راجع [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi) الذي يدعم القراءة من Amazon S3 والخدمات المتوافقة مع S3. ويُعد [hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) النسخة الموزعة من الدالة `hudi`.

<div id="data-catalogs">
  # كتالوجات البيانات
</div>

<div id="aws-glue">
  ## AWS Glue
</div>

يمكن استخدام Glue Data Catalog من AWS مع جداول Iceberg. ويمكنك استخدامه مع محرك الجدول `iceberg`، أو مع محرك قواعد البيانات [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="iceberg-rest-catalog">
  ## كتالوج REST لـ Iceberg
</div>

يمكن استخدام كتالوج REST لـ Iceberg مع جداول Iceberg. ويمكنك استخدامه مع محرك الجدول `iceberg`، أو مع محرك قاعدة البيانات [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="unity-catalog">
  ## Unity Catalog
</div>

يمكن استخدام Unity Catalog مع جداول Delta Lake وIceberg على حدّ سواء. ويمكنك استخدامه مع محركَي الجداول `iceberg` و`deltaLake`، أو مع محرك قاعدة البيانات [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="microsoft-onelake">
  ## Microsoft OneLake
</div>

يمكن استخدام Microsoft OneLake مع كلٍّ من جداول Delta Lake وجداول Iceberg. كما يمكنك استخدامه مع محرك قاعدة البيانات [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).