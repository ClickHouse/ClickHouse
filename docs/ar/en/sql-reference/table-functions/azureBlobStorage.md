---
description: 'توفّر واجهة شبيهة بالجدول لتنفيذ select/insert على الملفات في Azure Blob
  Storage. وتشبه دالة s3.'
keywords: ['azure blob storage']
sidebar_label: 'azureBlobStorage'
sidebar_position: 10
slug: /sql-reference/table-functions/azureBlobStorage
title: 'azureBlobStorage'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="azureblobstorage-table-function">
  # دالة الجدول azureBlobStorage
</div>

توفر واجهة شبيهة بالجدول لتنفيذ select/insert على الملفات في [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). دالة الجدول هذه مشابهة لـ [دالة s3](../../sql-reference/table-functions/s3.md).

<div id="syntax">
  ## الصيغة
</div>

<Tabs>
  <TabItem value="connection_string" label="سلسلة الاتصال" default>
    بيانات الاعتماد مضمنة في سلسلة الاتصال، لذلك لا حاجة إلى `account_name`/`account_key` بشكل منفصل:

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="URL حساب التخزين">
    يتطلب تمرير `account_name` و`account_key` كوسيطتين منفصلتين:

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="مجموعة مسماة">
    راجع [المجموعات المسماة](#named-collections) أدناه للاطلاع على القائمة الكاملة بالمفاتيح المدعومة:

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## الوسيطات
</div>

| Argument                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `connection_string`              | سلسلة اتصال تتضمن بيانات اعتماد مضمنة (اسم الحساب + مفتاح الحساب أو SAS token). عند استخدام هذا الشكل، يجب **ألا** تُمرَّر `account_name` و`account_key` بشكل منفصل. راجع [تكوين سلسلة اتصال](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account). |
| `storage_account_url`            | URL نقطة نهاية حساب التخزين، على سبيل المثال `https://myaccount.blob.core.windows.net/`. عند استخدام هذا الشكل، **يجب** أيضًا تمرير `account_name` و`account_key`.                                                                                                                                                                                                                                                                                     |
| `container_name`                 | اسم الحاوية.                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `blobpath`                       | مسار الملف. يدعم أحرف البدل التالية في وضع القراءة فقط: `*`, `**`, `?`, `{abc,def}` و `{N..M}`، حيث إن `N` و`M` — أرقام، و`'abc'` و`'def'` — سلاسل نصية.                                                                                                                                                                                                                                                                                               |
| `account_name`                   | اسم حساب التخزين. **مطلوب** عند استخدام `storage_account_url` من دون SAS؛ ويجب **ألا** يُمرَّر عند استخدام `connection_string`.                                                                                                                                                                                                                                                                                                                        |
| `account_key`                    | مفتاح حساب التخزين. **مطلوب** عند استخدام `storage_account_url` من دون SAS؛ ويجب **ألا** يُمرَّر عند استخدام `connection_string`.                                                                                                                                                                                                                                                                                                                      |
| `format`                         | [تنسيق](/ar/sql-reference/formats) الملف.                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `compression`                    | القيم المدعومة: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. افتراضيًا، سيجري اكتشاف الضغط تلقائيًا استنادًا إلى امتداد الملف (كما لو كان مضبوطًا على `auto`).                                                                                                                                                                                                                                                                               |
| `structure`                      | بنية الجدول. التنسيق هو `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                 |
| `partition_strategy`             | اختياري. القيم المدعومة: `WILDCARD` أو `HIVE`. يتطلب `WILDCARD` وجود `{_partition_id}` في المسار، ويُستبدل هذا بمفتاح التقسيم. لا يسمح `HIVE` باستخدام أحرف البدل، ويفترض أن المسار هو جذر الجدول، ويُنشئ أدلة مقسمة بأسلوب Hive مع Snowflake IDs كأسماء للملفات وتنسيق الملف كامتداد. القيمة الافتراضية هي الإعداد `file_like_engine_default_partition_strategy` (`WILDCARD` ضمن إعدادات `compatibility` الأقدم من `26.6`، و`HIVE` بخلاف ذلك).        |
| `partition_columns_in_data_file` | اختياري. يُستخدم فقط مع استراتيجية التقسيم `HIVE`. يحدد لـ ClickHouse ما إذا كان ينبغي توقّع كتابة أعمدة التقسيم داخل ملف البيانات. القيمة الافتراضية `false`.                                                                                                                                                                                                                                                                                         |
| `extra_credentials`              | استخدم `client_id` و`tenant_id` للمصادقة. إذا تم توفير `extra_credentials`، فستُعطى الأولوية لهما على `account_name` و`account_key`.                                                                                                                                                                                                                                                                                                                   |

<div id="named-collections">
  ## المجموعات المسماة
</div>

يمكن أيضًا تمرير الوسائط باستخدام [المجموعات المسماة](/ar/operations/named-collections). في هذه الحالة، تكون المفاتيح التالية مدعومة:

| Key                   | Required | Description                                                                                                       |
| --------------------- | -------- | ----------------------------------------------------------------------------------------------------------------- |
| `container`           | Yes      | اسم الحاوية. يقابل الوسيطة الموضعية `container_name`.                                                             |
| `blob_path`           | Yes      | مسار الملف (مع أحرف بدل اختيارية). يقابل الوسيطة الموضعية `blobpath`.                                             |
| `connection_string`   | No*      | سلسلة الاتصال مع بيانات الاعتماد المضمّنة. *يجب توفير أحد الخيارين: `connection_string` أو `storage_account_url`. |
| `storage_account_url` | No*      | عنوان URL لنقطة نهاية حساب التخزين. *يجب توفير أحد الخيارين: `connection_string` أو `storage_account_url`.        |
| `account_name`        | No       | مطلوب عند استخدام `storage_account_url`                                                                           |
| `account_key`         | No       | مطلوب عند استخدام `storage_account_url`                                                                           |
| `format`              | No       | تنسيق الملف.                                                                                                      |
| `compression`         | No       | نوع الضغط.                                                                                                        |
| `structure`           | No       | بنية الجدول.                                                                                                      |
| `client_id`           | No       | معرّف العميل للمصادقة.                                                                                            |
| `tenant_id`           | No       | معرّف المستأجر للمصادقة.                                                                                          |

:::note
تختلف أسماء مفاتيح المجموعات المسماة عن أسماء وسائط الدالة الموضعية: `container` (وليس `container_name`) و`blob_path` (وليس `blobpath`).
:::

**مثال:**

```sql
CREATE NAMED COLLECTION azure_my_data AS
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'mycontainer',
    blob_path = 'data/*.parquet',
    account_name = 'myaccount',
    account_key = 'mykey...==',
    format = 'Parquet';

SELECT *
FROM azureBlobStorage(azure_my_data)
LIMIT 5;
```

يمكنك أيضًا تجاوز قيم المجموعة المسماة عند تنفيذ الاستعلام:

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## القيمة المُعادة
</div>

جدول بالبنية المحددة لقراءة البيانات من الملف المحدد أو كتابتها فيه.

<div id="examples">
  ## أمثلة
</div>

<div id="reading-with-storage-account-url">
  ### القراءة باستخدام الصيغة `storage_account_url`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'https://myaccount.blob.core.windows.net/',
    'mycontainer',
    'data/*.parquet',
    'myaccount',
    'mykey...==',
    'Parquet'
)
LIMIT 5;
```

<div id="reading-with-connection-string">
  ### القراءة بصيغة `connection_string`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'data/*.csv',
    'CSVWithNames'
)
LIMIT 5;
```

<div id="writing-with-partitions">
  ### الكتابة باستخدام التقسيمات
</div>

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
) PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

ثم أعد قراءة قسم محدد:

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_1.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
);
```

```response
┌─column1─┬─column2─┬─column3─┐
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.

<div id="partitioned-write">
  ## الكتابة المقسّمة
</div>

<div id="partition-strategy">
  ### استراتيجية التقسيم
</div>

مدعومة فقط مع استعلامات `INSERT`.

`WILDCARD`: يستبدل الرمز البديل `{_partition_id}` في مسار الملف بمفتاح التقسيم الفعلي. ويُحدَّد افتراضيًا فقط ضمن إعدادات `compatibility` الأقدم من `26.6`؛ وإلا تكون القيمة الافتراضية هي `HIVE` (راجع الإعداد `file_like_engine_default_partition_strategy`).

يطبّق `HIVE` التقسيم بنمط Hive لعمليات القراءة والكتابة. ويُنشئ الملفات باستخدام الصيغة التالية: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**مثال على استراتيجية التقسيم `HIVE`**

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root',
    format = 'CSVWithNames',
    compression = 'auto',
    structure = 'year UInt16, country String, id Int32',
    partition_strategy = 'hive'
) PARTITION BY (year, country)
VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
SELECT _path, * FROM azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root/**.csvwithnames'
)

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

<div id="hive-style-partitioning">
  ## الإعداد use_hive_partitioning
</div>

هذا تلميح إلى ClickHouse لتحليل الملفات المُقسَّمة بنمط Hive عند القراءة. ولا يؤثر في الكتابة. ولجعل القراءة والكتابة متناظرتين، استخدم الوسيط `partition_strategy`.

عند تعيين الإعداد `use_hive_partitioning` إلى القيمة 1، سيتعرّف ClickHouse على التقسيم بنمط Hive في المسار (`/name=value/`) وسيتيح استخدام أعمدة التقسيم كأعمدة افتراضية في الاستعلام. وستحمل هذه الأعمدة الافتراضية الأسماء نفسها الموجودة في المسار المُقسَّم.

**مثال**

استخدم عمودًا افتراضيًا أُنشئ باستخدام التقسيم بنمط Hive

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## استخدام توقيعات الوصول المشتركة (SAS)
</div>

توقيع الوصول المشترك (SAS) هو عنوان URI يمنح وصولًا مقيّدًا إلى حاوية أو ملف في Azure Storage. استخدمه لتوفير وصول محدود زمنيًا إلى موارد حساب التخزين من دون مشاركة المفتاح الخاص بحساب التخزين. مزيد من التفاصيل [هنا](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature).

تدعم الدالة `azureBlobStorage` توقيعات الوصول المشتركة (SAS).

يحتوي [Blob SAS token](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) على جميع المعلومات اللازمة لمصادقة الطلب، بما في ذلك blob المستهدف، والأذونات، وفترة الصلاحية. ولإنشاء عنوان URL لـ blob، ألحِق رمز SAS بنقطة نهاية خدمة blob. على سبيل المثال، إذا كانت نقطة النهاية هي `https://clickhousedocstest.blob.core.windows.net/`، يصبح الطلب:

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

بدلًا من ذلك، يمكن للمستخدمين استخدام [عنوان URL لـ Blob SAS الذي تم إنشاؤه](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers):

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## ذات صلة
</div>

* [محرك الجدول AzureBlobStorage](/ar/engines/table-engines/integrations/azureBlobStorage.md)