---
description: 'يوفّر هذا المحرك تكاملًا مع منظومة Apache Hadoop، إذ يتيح إدارة البيانات على HDFS عبر ClickHouse. يشبه هذا المحرك محركَي File وURL، لكنه يوفّر ميزات خاصة بـ Hadoop.'
sidebar_label: 'HDFS'
sidebar_position: 80
slug: /engines/table-engines/integrations/hdfs
title: 'محرك جدول HDFS'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-engine">
  # محرك جدول HDFS
</div>

<CloudNotSupportedBadge />

يوفّر هذا المحرك تكاملًا مع منظومة [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) من خلال إتاحة إدارة البيانات على [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) عبر ClickHouse. يشبه هذا المحرك محركي [File](/ar/engines/table-engines/special/file) و[URL](/ar/engines/table-engines/special/url)، لكنه يوفّر ميزات خاصة بـ Hadoop.

هذه الميزة غير مدعومة من مهندسي ClickHouse، ومن المعروف أن جودتها متواضعة. إذا واجهت أي مشكلات، فأصلحها بنفسك وقدّم طلب سحب.

<div id="usage">
  ## الاستخدام
</div>

```sql
ENGINE = HDFS(URI, format)
```

**معلمات المحرك**

* `URI` - عنوان URI الكامل للملف في HDFS. قد يحتوي جزء المسار من `URI` على أنماط glob. في هذه الحالة، يكون الجدول للقراءة فقط.
* `format` - يحدّد أحد تنسيقات الملفات المتاحة. لتنفيذ استعلامات
  `SELECT`، يجب أن يكون التنسيق مدعومًا للإدخال، ولتنفيذ استعلامات
  `INSERT` – للإخراج. تَرِد التنسيقات المتاحة في قسم
  [التنسيقات](/ar/sql-reference/formats#formats-overview).
* [PARTITION BY expr]

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — اختياري. في معظم الحالات، لن تحتاج إلى مفتاح partition، وإذا احتجت إليه، فعادةً لن تحتاج إلى مفتاح partition أدق من التقسيم حسب الشهر. لا يسرّع التقسيم الاستعلامات (على عكس تعبير ORDER BY). ويجب ألا تستخدم تقسيمًا دقيقًا للغاية مطلقًا. لا تقسّم بياناتك حسب معرّفات العميل أو الأسماء (بل اجعل معرّف العميل أو الاسم هو العمود الأول في تعبير ORDER BY).

للتقسيم حسب الشهر، استخدم التعبير `toYYYYMM(date_column)`، حيث إن `date_column` هو عمود يحتوي على تاريخ من النوع [Date](/ar/sql-reference/data-types/date.md). تكون أسماء الأقسام هنا بالتنسيق `"YYYYMM"`.

**مثال:**

**1.** أعدّ جدول `hdfs_engine_table`:

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** املأ الملف:

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** نفِّذ استعلامًا على البيانات:

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

* يمكن تنفيذ عمليات القراءة والكتابة بالتوازي.
* غير مدعوم:

  * عمليات `ALTER` و`SELECT...SAMPLE`.
  * الفهارس.
  * يمكن استخدام [Zero-copy](../../../operations/storing-data.md#zero-copy) replication، لكن لا يُنصح بذلك.

  :::note لا يزال zero-copy replication غير جاهز للاستخدام في بيئات الإنتاج
  يكون zero-copy replication معطّلًا افتراضيًا في ClickHouse الإصدار 22.8 وما بعده. لا يُنصح باستخدام هذه الميزة في بيئات الإنتاج.
  :::

**أنماط glob في المسار**

يمكن أن يحتوي أكثر من مكوّن في المسار على أنماط glob. ولكي يُعالَج الملف، يجب أن يكون موجودًا وأن يطابق نمط المسار بالكامل. تُحدَّد قائمة الملفات أثناء `SELECT` (وليس عند `CREATE`).

* `*` — يستبدل أي عدد من المحارف باستثناء `/`، بما في ذلك السلسلة الفارغة.
* `?` — يستبدل أي محرف واحد.
* `{some_string,another_string,yet_another_one}` — يستبدل أيًا من السلاسل `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — يستبدل أي رقم ضمن النطاق من N إلى M شاملًا الحدّين.

البُنى التي تحتوي على `{}` مشابهة لدالة الجدول [remote](../../../sql-reference/table-functions/remote.md).

**مثال**

1. لنفترض أن لدينا عدة ملفات بتنسيق TSV مع عناوين URI التالية على HDFS:

   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. توجد عدة طرق لإنشاء جدول يتكوّن من الملفات الستة جميعًا:

{/* */ }

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

طريقة أخرى:

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

يتألف الجدول من جميع الملفات الموجودة في كلا الدليلين (يجب أن تتوافق جميع الملفات مع التنسيق والبنية المحددتين في الاستعلام):

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
إذا كانت قائمة الملفات تحتوي على نطاقات رقمية بأصفار بادئة، فاستخدم الصيغة التي تتضمن أقواسًا لكل رقم على حدة، أو استخدم `?`.
:::

**مثال**

أنشئ جدولًا بملفات تحمل الأسماء `file000` و`file001` و... و`file999`:

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

<div id="configuration">
  ## التكوين
</div>

على غرار GraphiteMergeTree، يدعم محرك HDFS التكوين الموسَّع باستخدام ملف تكوين ClickHouse. هناك مفتاحا تكوين يمكنك استخدامهما: مفتاح عام (`hdfs`) ومفتاح على مستوى المستخدم (`hdfs_*`). يُطبَّق التكوين العام أولًا، ثم يُطبَّق التكوين على مستوى المستخدم (إن وُجد).

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
  <hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
  <hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  <hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
  <hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

<div id="configuration-options">
  ### خيارات التكوين
</div>

<div id="supported-by-libhdfs3">
  #### المدعوم في libhdfs3
</div>

| **المعلمة**                                                             | **القيمة الافتراضية**             |
| ----------------------------------------------------------------------- | --------------------------------- |
| rpc&#95;client&#95;connect&#95;tcpnodelay                               | true                              |
| dfs&#95;client&#95;read&#95;shortcircuit                                | true                              |
| output&#95;replace-datanode-on-failure                                  | true                              |
| input&#95;notretry-another-node                                         | false                             |
| input&#95;localread&#95;mappedfile                                      | true                              |
| dfs&#95;client&#95;use&#95;legacy&#95;blockreader&#95;local             | false                             |
| rpc&#95;client&#95;ping&#95;interval                                    | 10  * 1000                        |
| rpc&#95;client&#95;connect&#95;timeout                                  | 600 * 1000                        |
| rpc&#95;client&#95;read&#95;timeout                                     | 3600 * 1000                       |
| rpc&#95;client&#95;write&#95;timeout                                    | 3600 * 1000                       |
| rpc&#95;client&#95;socket&#95;linger&#95;timeout                        | -1                                |
| rpc&#95;client&#95;connect&#95;retry                                    | 10                                |
| rpc&#95;client&#95;timeout                                              | 3600 * 1000                       |
| dfs&#95;default&#95;replica                                             | 3                                 |
| input&#95;connect&#95;timeout                                           | 600 * 1000                        |
| input&#95;read&#95;timeout                                              | 3600 * 1000                       |
| input&#95;write&#95;timeout                                             | 3600 * 1000                       |
| input&#95;localread&#95;default&#95;buffersize                          | 1 * 1024 * 1024                   |
| dfs&#95;prefetchsize                                                    | 10                                |
| input&#95;read&#95;getblockinfo&#95;retry                               | 3                                 |
| input&#95;localread&#95;blockinfo&#95;cachesize                         | 1000                              |
| input&#95;read&#95;max&#95;retry                                        | 60                                |
| output&#95;default&#95;chunksize                                        | 512                               |
| output&#95;default&#95;packetsize                                       | 64 * 1024                         |
| output&#95;default&#95;write&#95;retry                                  | 10                                |
| output&#95;connect&#95;timeout                                          | 600 * 1000                        |
| output&#95;read&#95;timeout                                             | 3600 * 1000                       |
| output&#95;write&#95;timeout                                            | 3600 * 1000                       |
| output&#95;close&#95;timeout                                            | 3600 * 1000                       |
| output&#95;packetpool&#95;size                                          | 1024                              |
| output&#95;heartbeat&#95;interval                                       | 10 * 1000                         |
| dfs&#95;client&#95;failover&#95;max&#95;attempts                        | 15                                |
| dfs&#95;client&#95;read&#95;shortcircuit&#95;streams&#95;cache&#95;size | 256                               |
| dfs&#95;client&#95;socketcache&#95;expiryMsec                           | 3000                              |
| dfs&#95;client&#95;socketcache&#95;capacity                             | 16                                |
| dfs&#95;default&#95;blocksize                                           | 64 * 1024 * 1024                  |
| dfs&#95;default&#95;uri                                                 | &quot;hdfs://localhost:9000&quot; |
| hadoop&#95;security&#95;authentication                                  | &quot;simple&quot;                |
| hadoop&#95;security&#95;kerberos&#95;ticket&#95;cache&#95;path          | &quot;&quot;                      |
| dfs&#95;client&#95;log&#95;severity                                     | &quot;INFO&quot;                  |
| dfs&#95;domain&#95;socket&#95;path                                      | &quot;&quot;                      |

قد يوضح [مرجع إعدادات HDFS](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) بعض المعلمات.

<div id="clickhouse-extras">
  #### إعدادات ClickHouse الإضافية
</div>

| **المعلمة**                       | **القيمة الافتراضية** |
| --------------------------------- | --------------------- |
| hadoop&#95;kerberos&#95;keytab    | &quot;&quot;          |
| hadoop&#95;kerberos&#95;principal | &quot;&quot;          |
| libhdfs3&#95;conf                 | &quot;&quot;          |

<div id="limitations">
  ### القيود
</div>

* يمكن أن يكون `hadoop_security_kerberos_ticket_cache_path` و`libhdfs3_conf` عامَّين فقط، وليس لكل مستخدم على حدة

<div id="kerberos-support">
  ## دعم Kerberos
</div>

إذا كانت قيمة المعلَمة `hadoop_security_authentication` هي `kerberos`، فسيستخدم ClickHouse المصادقة عبر Kerberos.
يمكن العثور على المعلمات [هنا](#clickhouse-extras)، وقد يكون `hadoop_security_kerberos_ticket_cache_path` مفيدًا.
لاحظ أنه بسبب قيود libhdfs3، لا يُدعَم إلا الأسلوب القديم،
ولا تكون اتصالات datanode مؤمّنة بواسطة SASL (ويُعد `HADOOP_SECURE_DN_USER` مؤشرًا موثوقًا على هذا
النهج الأمني). استخدم `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` كمرجع.

إذا جرى تحديد `hadoop_kerberos_keytab` أو `hadoop_kerberos_principal` أو `hadoop_security_kerberos_ticket_cache_path`، فستُستخدم مصادقة Kerberos. وفي هذه الحالة، يكون كلٌّ من `hadoop_kerberos_keytab` و`hadoop_kerberos_principal` مطلوبًا.

<div id="namenode-ha">
  ## دعم التوفّر العالي لـ Namenode في HDFS
</div>

يدعم `libhdfs3` التوفّر العالي لـ Namenode في HDFS.

* انسخ `hdfs-site.xml` من إحدى عقد HDFS إلى `/etc/clickhouse-server/`.
* أضف المقتطف التالي إلى ملف إعدادات ClickHouse:

```xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

* ثم استخدم قيمة الوسم `dfs.nameservices` في ملف `hdfs-site.xml` باعتبارها عنوان `namenode` في URI الخاص بـ HDFS. على سبيل المثال، استبدل `hdfs://appadmin@192.168.101.11:8020/abc/` بـ `hdfs://appadmin@my_nameservice/abc/`.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان الحجم غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.

<div id="storage-settings">
  ## إعدادات التخزين
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/ar/operations/settings/settings.md#hdfs_truncate_on_insert) - يسمح بتفريغ الملف قبل إدراج البيانات فيه. يكون معطّلًا افتراضيًا.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/ar/operations/settings/settings.md#hdfs_create_new_file_on_insert) - يسمح بإنشاء ملف جديد مع كل عملية إدراج إذا كان التنسيق يحتوي على لاحقة. يكون معطّلًا افتراضيًا.
* [hdfs&#95;skip&#95;empty&#95;files](/ar/operations/settings/settings.md#hdfs_skip_empty_files) - يسمح بتخطي الملفات الفارغة أثناء القراءة. يكون معطّلًا افتراضيًا.

**انظر أيضًا**

* [الأعمدة الافتراضية](../../../engines/table-engines/index.md#table_engines-virtual_columns)