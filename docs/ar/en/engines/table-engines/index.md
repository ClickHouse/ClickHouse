---
description: 'توثيق محركات الجداول'
slug: /engines/table-engines/
toc_folder_title: 'محركات الجداول'
toc_priority: 26
toc_title: 'مقدمة'
title: 'محركات الجداول'
doc_type: 'مرجع'
---

يحدّد محرك الجدول (نوع الجدول) ما يلي:

* كيفية تخزين البيانات وأين تُخزَّن، وأين تُكتَب، ومن أين تُقرَأ.
* الاستعلامات التي يدعمها، وكيفية عملها.
* الوصول المتزامن إلى البيانات.
* استخدام الفهارس، إن وُجدت.
* ما إذا كان تنفيذ الطلبات متعدد الخيوط ممكنًا.
* معلمات النسخ المتماثل للبيانات.

<div id="engine-families">
  ## عائلات المحركات
</div>

<div id="mergetree">
  ### MergeTree
</div>

تُعدّ من أكثر محركات الجداول شمولًا وكفاءةً للمهام ذات الأحمال العالية. والقاسم المشترك بين هذه المحركات هو سرعة إدراج البيانات، تليها معالجتها لاحقًا في الخلفية. تدعم محركات عائلة `MergeTree` النسخ المتماثل للبيانات (من خلال إصدارات المحركات [Replicated*](/ar/engines/table-engines/mergetree-family/replication))، والتقسيم، والفهارس الثانوية لتخطي البيانات، وميزات أخرى غير مدعومة في المحركات الأخرى.

المحركات في هذه العائلة:

| محركات MergeTree                                                                                     |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/ar/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/ar/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/ar/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/ar/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/ar/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/ar/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/ar/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/ar/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

[محركات](../../engines/table-engines/log-family/index.md) خفيفة الوزن بأقل قدر من الوظائف. تكون أكثر فعالية عندما تحتاج إلى كتابة عدد كبير من الجداول الصغيرة بسرعة (حتى نحو مليون صف) ثم قراءتها لاحقًا دفعةً واحدة.

المحركات في هذه العائلة:

| محركات Log                                               |
| -------------------------------------------------------- |
| [TinyLog](/ar/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/ar/engines/table-engines/log-family/stripelog) |
| [Log](/ar/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### محركات التكامل
</div>

محركات للتواصل مع أنظمة أخرى لتخزين البيانات ومعالجتها.

المحركات في هذه العائلة:

| محركات التكامل                                                                  |
| ------------------------------------------------------------------------------- |
| [ODBC](../../engines/table-engines/integrations/odbc.md)                        |
| [JDBC](../../engines/table-engines/integrations/jdbc.md)                        |
| [MySQL](../../engines/table-engines/integrations/mysql.md)                      |
| [MongoDB](../../engines/table-engines/integrations/mongodb.md)                  |
| [Redis](../../engines/table-engines/integrations/redis.md)                      |
| [HDFS](../../engines/table-engines/integrations/hdfs.md)                        |
| [S3](../../engines/table-engines/integrations/s3.md)                            |
| [Kafka](../../engines/table-engines/integrations/kafka.md)                      |
| [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) |
| [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)                |
| [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)            |
| [S3Queue](../../engines/table-engines/integrations/s3queue.md)                  |
| [TimeSeries](../../engines/table-engines/integrations/time-series.md)           |

<div id="special-engines">
  ### المحركات الخاصة
</div>

المحركات في هذه العائلة:

| المحركات الخاصة                                               |
| ------------------------------------------------------------- |
| [Distributed](/ar/engines/table-engines/special/distributed)     |
| [Dictionary](/ar/engines/table-engines/special/dictionary)       |
| [Merge](/ar/engines/table-engines/special/merge)                 |
| [Executable](/ar/engines/table-engines/special/executable)       |
| [File](/ar/engines/table-engines/special/file)                   |
| [Null](/ar/engines/table-engines/special/null)                   |
| [Set](/ar/engines/table-engines/special/set)                     |
| [Join](/ar/engines/table-engines/special/join)                   |
| [URL](/ar/engines/table-engines/special/url)                     |
| [View](/ar/engines/table-engines/special/view)                   |
| [Memory](/ar/engines/table-engines/special/memory)               |
| [Buffer](/ar/engines/table-engines/special/buffer)               |
| [External Data](/ar/engines/table-engines/special/external-data) |
| [GenerateRandom](/ar/engines/table-engines/special/generate)     |
| [KeeperMap](/ar/engines/table-engines/special/keeper-map)        |
| [FileLog](/ar/engines/table-engines/special/filelog)             |

<div id="table_engines-virtual_columns">
  ## الأعمدة الافتراضية
</div>

العمود الافتراضي هو سمة أساسية من سمات محرّك الجدول، ويُعرَّف في شيفرة مصدر المحرّك.

يجب ألا تحدد الأعمدة الافتراضية في استعلام `CREATE TABLE`، ولا يمكنك رؤيتها في نتائج استعلامي `SHOW CREATE TABLE` و`DESCRIBE TABLE`. كما أن الأعمدة الافتراضية للقراءة فقط، لذلك لا يمكنك إدراج البيانات فيها.

لاختيار البيانات من عمود افتراضي، يجب تحديد اسمه في استعلام `SELECT`. لا يعرض `SELECT *` قيماً من الأعمدة الافتراضية.

إذا أنشأت جدولاً بعمود يحمل الاسم نفسه لأحد الأعمدة الافتراضية في الجدول، فسيصبح العمود الافتراضي غير متاح. لا نوصي بذلك. وللمساعدة على تجنب التعارضات، تبدأ أسماء الأعمدة الافتراضية عادةً بشرطة سفلية.

* `_table` — يحتوي على اسم الجدول الذي قُرئت منه البيانات. النوع: [String](../../sql-reference/data-types/string.md).

  بغض النظر عن محرّك الجدول المستخدم، يتضمن كل جدول عموداً افتراضياً عاماً باسم `_table`.

  عند الاستعلام عن جدول يستخدم Merge table engine، يمكنك تعيين شروط ثابتة على `_table` في عبارة `WHERE/PREWHERE` (على سبيل المثال، `WHERE _table='xyz'`). في هذه الحالة، لا تُنفَّذ عملية القراءة إلا على الجداول التي يتحقق فيها الشرط على `_table`، ولذلك يعمل العمود `_table` كفهرس.

  عند استخدام استعلامات بصيغة `SELECT ... FROM (... UNION ALL ...)`، يمكن تحديد الجدول الفعلي الذي تنتمي إليه الصفوف المُعادة من خلال تحديد العمود `_table`.