---
description: 'يوفّر هذا المحرك تكاملًا مع بيئة Azure Blob Storage،
  مما يتيح استيراد البيانات المتدفقة.'
sidebar_label: 'AzureQueue'
sidebar_position: 181
slug: /engines/table-engines/integrations/azure-queue
title: 'محرك الجدول AzureQueue'
doc_type: 'مرجع'
---

يوفّر هذا المحرك تكاملًا مع بيئة [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs)، مما يتيح استيراد البيانات المتدفقة.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE test (name String, value UInt32)
    ENGINE = AzureQueue(...)
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    ...
```

**معلمات المحرك**

معلمات `AzureQueue` هي نفسها المعلمات التي يدعمها محرك الجدول `AzureBlobStorage`. راجع قسم المعلمات [هنا](../../../engines/table-engines/integrations/azureBlobStorage.md).

على غرار محرك الجدول [AzureBlobStorage](/ar/engines/table-engines/integrations/azureBlobStorage)، يمكن للمستخدمين استخدام محاكي Azurite لتطوير Azure Storage محليًا. لمزيد من التفاصيل، راجع [هنا](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage).

**مثال**

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS mode = 'unordered'
```

<div id="settings">
  ## الإعدادات
</div>

مجموعة الإعدادات المدعومة هي في معظمها نفسها الخاصة بمحرك الجدول `S3Queue`، ولكن من دون البادئة `s3queue_`. راجع [القائمة الكاملة للإعدادات](../../../engines/table-engines/integrations/s3queue.md#settings).
للاطّلاع على قائمة الإعدادات المُعدّة للجدول، استخدم الجدول `system.azure_queue_settings`. متاح بدءًا من `24.10`.

فيما يلي الإعدادات المتوافقة فقط مع AzureQueue ولا تنطبق على S3Queue.

<div id="after_processing_move_connection_string">
  ### `after_processing_move_connection_string`
</div>

سلسلة الاتصال الخاصة بـ Azure Blob Storage لنقل الملفات التي تمت معالجتها بنجاح إليها، إذا كانت الوجهة حاوية Azure أخرى.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

<div id="after_processing_move_container">
  ### `after_processing_move_container`
</div>

اسم الحاوية التي تُنقل إليها الملفات التي تمت معالجتها بنجاح، إذا كانت الوجهة حاوية Azure أخرى.

القيم الممكنة:

* String.

القيمة الافتراضية: سلسلة فارغة.

مثال:

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    after_processing_move_container = 'dst-container';
```

<div id="select">
  ## SELECT من محرك الجدول AzureQueue
</div>

تُحظر استعلامات SELECT افتراضيًا على جداول AzureQueue. وهذا يتماشى مع النمط الشائع لقائمة الانتظار، حيث تُقرأ البيانات مرة واحدة ثم تُزال من قائمة الانتظار. ويُحظر SELECT لمنع فقدان البيانات عن طريق الخطأ.
ومع ذلك، قد يكون هذا مفيدًا أحيانًا. وللقيام بذلك، تحتاج إلى ضبط الإعداد `stream_like_engine_allow_direct_select` على `True`.
يحتوي محرك AzureQueue على إعداد خاص لاستعلامات SELECT: `commit_on_select`. اضبطه على `False` للاحتفاظ بالبيانات في قائمة الانتظار بعد قراءتها، أو على `True` لإزالتها.

<div id="description">
  ## الوصف
</div>

لا يُعد `SELECT` مفيدًا كثيرًا في الاستيراد المتدفق (إلا لأغراض تصحيح الأخطاء)، لأن كل ملف لا يمكن استيراده إلا مرة واحدة. ومن العملي أكثر إنشاء سلاسل معالجة آنية باستخدام [العروض المادية](../../../sql-reference/statements/create/view.md). وللقيام بذلك:

1. استخدم المحرّك لإنشاء جدول للاستهلاك من المسار المحدد في Azure Blob Storage واعتبره تدفق بيانات.
2. أنشئ جدولًا بالبنية المطلوبة.
3. أنشئ عرضًا ماديًا يحوّل البيانات من المحرّك ويضعها في جدول تم إنشاؤه مسبقًا.

عند ربط `MATERIALIZED VIEW` بالمحرّك، يبدأ في جمع البيانات في الخلفية.

تكون وسيطات المحرّك بالصيغة `AzureQueue(connection_string, container_name, blobpath, format[, compression])`.

مثال:

```sql
CREATE TABLE azure_queue_engine_table (key UInt64, data String)
  ENGINE=AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
  SETTINGS
      mode = 'unordered';

CREATE TABLE stats (key UInt64, data String)
  ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO stats
  AS SELECT key, data FROM azure_queue_engine_table;

SELECT * FROM stats ORDER BY key;
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف.
* `_file` — اسم الملف.

لمزيد من المعلومات عن الأعمدة الافتراضية، راجع [هذا القسم](../../../engines/table-engines/index.md#table_engines-virtual_columns).

<div id="introspection">
  ## الاستقصاء الداخلي
</div>

فعِّل التسجيل للجدول عبر إعداد الجدول `enable_logging_to_queue_log=1`.

إمكانات الاستقصاء الداخلي مماثلة لتلك الخاصة بـ [محرك الجدول S3Queue](/ar/engines/table-engines/integrations/s3queue#introspection)، مع بعض الاختلافات الواضحة:

1. استخدم `system.azure_queue_metadata_cache` للحالة الموجودة في الذاكرة الخاصة بقائمة الانتظار في إصدارات الخادم &gt;= 25.1. أما في الإصدارات الأقدم، فاستخدم `system.s3queue_metadata_cache` (إذ سيحتوي أيضًا على معلومات عن جداول `azure`).
2. فعِّل `system.azure_queue_log` عبر إعداد ClickHouse الرئيسي، على سبيل المثال.

```xml
  <azure_queue_log>
    <database>system</database>
    <table>azure_queue_log</table>
  </azure_queue_log>
```

يحتوي هذا الجدول الدائم على المعلومات نفسها الموجودة في `system.s3queue_metadata_cache`، لكنه خاص بالملفات المُعالجة وتلك التي تعذّرت معالجتها.

لدى الجدول البنية التالية:

```sql

CREATE TABLE system.azure_queue_log
(
    `hostname` LowCardinality(String) COMMENT 'Hostname',
    `event_date` Date COMMENT 'Event date of writing this log row',
    `event_time` DateTime COMMENT 'Event time of writing this log row',
    `database` String COMMENT 'The name of a database where current S3Queue table lives.',
    `table` String COMMENT 'The name of S3Queue table.',
    `uuid` String COMMENT 'The UUID of S3Queue table',
    `file_name` String COMMENT 'File name of the processing file',
    `rows_processed` UInt64 COMMENT 'Number of processed rows',
    `status` Enum8('Processed' = 0, 'Failed' = 1) COMMENT 'Status of the processing file',
    `processing_start_time` Nullable(DateTime) COMMENT 'Time of the start of processing the file',
    `processing_end_time` Nullable(DateTime) COMMENT 'Time of the end of processing the file',
    `exception` String COMMENT 'Exception message if happened'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
COMMENT 'Contains logging entries with the information files processes by S3Queue engine.'

```

مثال:

```sql
SELECT *
FROM system.azure_queue_log
LIMIT 1
FORMAT Vertical

Row 1:
──────
hostname:              clickhouse
event_date:            2024-12-16
event_time:            2024-12-16 13:42:47
database:              default
table:                 azure_queue_engine_table
uuid:                  1bc52858-00c0-420d-8d03-ac3f189f27c8
file_name:             test_1.csv
rows_processed:        3
status:                Processed
processing_start_time: 2024-12-16 13:42:47
processing_end_time:   2024-12-16 13:42:47
exception:

1 row in set. Elapsed: 0.002 sec.

```