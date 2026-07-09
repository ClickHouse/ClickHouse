---
description: 'صفحة تشرح دعم المعاملات (ACID) في ClickHouse'
slug: /guides/developer/transactional
title: 'دعم المعاملات (ACID)'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="transactional-acid-support">
  # دعم المعاملات (ACID)
</div>

<div id="case-1-insert-into-one-partition-of-one-table-of-the-mergetree-family">
  ## الحالة 1: INSERT إلى partition واحدة في table واحدة من عائلة MergeTree*
</div>

يكون هذا معامليًا (ACID) إذا كانت الصفوف المُدرجة مُجمّعة ومُدرجة على هيئة block واحد (انظر الملاحظات):

* Atomic: تنجح عملية INSERT بالكامل أو تُرفض بالكامل: إذا أُرسل تأكيد إلى العميل، فهذا يعني أن جميع الصفوف قد أُدرجت؛ وإذا أُرسل خطأ إلى العميل، فهذا يعني أنه لم يُدرج أي صف.
* Consistent: إذا لم تُنتهك أي قيود على الجدول، فستُدرج جميع الصفوف في عملية INSERT وتنجح العملية؛ وإذا انتُهكت القيود، فلن يُدرج أي صف.
* Isolated: يرى العملاء المتزامنون snapshot متسقة للجدول — أي حالة الجدول كما كانت قبل محاولة INSERT أو بعد نجاحها؛ ولا تظهر أي حالة جزئية. أما العملاء داخل معاملة أخرى فلديهم [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation)، بينما العملاء خارج معاملة فلديهم مستوى العزل [read uncommitted](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Read_uncommitted).
* Durable: تُكتب عملية INSERT الناجحة إلى نظام الملفات قبل الرد على العميل، سواء على replica واحدة أو عدة replicas (يتحكم في ذلك الإعداد `insert_quorum`)، ويمكن لـ ClickHouse أن يطلب من نظام التشغيل مزامنة بيانات نظام الملفات مع وسيط التخزين (يتحكم في ذلك الإعداد `fsync_after_insert`).
* يمكن تنفيذ INSERT إلى عدة جداول بعبارة statement واحدة إذا كانت materialized views متضمنة (أي عندما تكون عملية INSERT من العميل إلى جدول له materialized views مرتبطة به).

<div id="case-2-insert-into-multiple-partitions-of-one-table-of-the-mergetree-family">
  ## الحالة 2: INSERT في عدة أقسام ضمن جدول واحد من عائلة MergeTree*
</div>

مثل الحالة 1 أعلاه، مع هذا التفصيل:

* إذا كان الجدول يحتوي على العديد من الأقسام وكان INSERT يشمل العديد من الأقسام، فإن عملية الإدراج في كل قسم تُعدّ معاملةً مستقلة بحد ذاتها

<div id="case-3-insert-into-one-distributed-table-of-the-mergetree-family">
  ## الحالة 3: INSERT في جدول موزّع واحد من عائلة MergeTree*
</div>

مثل الحالة 1 أعلاه، مع هذا التفصيل:

* إن عملية INSERT في جدول Distributed ليست معاملاتية ككل، بينما تكون عملية الإدراج في كل shard معاملاتية

<div id="case-4-using-a-buffer-table">
  ## الحالة 4: استخدام جدول Buffer
</div>

* إن الإدراج في جداول Buffer ليس ذرّيًا ولا معزولًا ولا متسقًا ولا يضمن الديمومة

<div id="case-5-using-async_insert">
  ## الحالة 5: استخدام async_insert
</div>

مثل الحالة 1 أعلاه، مع هذا التفصيل:

* تكون خاصية الذرية مضمونة حتى إذا كان `async_insert` ممكّنًا وكانت قيمة `wait_for_async_insert` مضبوطة على 1 (القيمة الافتراضية)، ولكن إذا كانت قيمة `wait_for_async_insert` مضبوطة على 0، فلن تكون خاصية الذرية مضمونة.

<div id="notes">
  ## ملاحظات
</div>

* تُجمَّع الصفوف التي يُدرجها العميل بتنسيق بيانات معيّن في block واحد عندما:
  * يكون تنسيق insert قائمًا على الصفوف (مثل CSV وTSV وValues وJSONEachRow وغيرها)، وتحتوي البيانات على أقل من `max_insert_block_size` صف (~ 1 000 000 افتراضيًا)، أو أقل من `min_chunk_bytes_for_parallel_parsing` بايت (10 MB افتراضيًا) في حال استخدام parsing المتوازي (وهو مُمكَّن افتراضيًا)
  * يكون تنسيق insert قائمًا على الأعمدة (مثل Native وParquet وORC وغيرها)، وتحتوي البيانات على block بيانات واحد فقط
* قد يعتمد حجم block المُدرج عمومًا على العديد من Settings (على سبيل المثال: `max_block_size` و`max_insert_block_size` و`min_insert_block_size_rows` و`min_insert_block_size_bytes` و`preferred_block_size_bytes` وغيرها)
* إذا لم يتلقَّ العميل ردًا من الخادم، فلن يعرف ما إذا كانت المعاملة قد نجحت، ويمكنه تكرار المعاملة باستخدام خصائص الإدراج exactly-once
* يستخدم ClickHouse داخليًا [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) مع [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) للـ المعاملات المتزامنة
* تظل جميع خصائص ACID سارية حتى في حالة إيقاف الخادم أو تعطّله
* يجب تمكين insert&#95;quorum عبر مناطق AZ مختلفة أو تمكين fsync لضمان inserts دائمة في الإعداد المعتاد
* لا يشمل معنى &quot;consistency&quot; ضمن مصطلحات ACID دلالات الأنظمة الموزعة؛ راجع https://jepsen.io/consistency، إذ تتحكم بها إعدادات مختلفة (select&#95;sequential&#95;consistency)
* لا يتناول هذا الشرح ميزة المعاملات الجديدة التي تتيح معاملات كاملة الميزات عبر جداول متعددة وmaterialized views ولعدة عمليات SELECT وغير ذلك. (راجع القسم التالي حول المعاملات وCommit وRollback)

<div id="transactions-commit-and-rollback">
  ## المعاملات وCommit والتراجع
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

بالإضافة إلى الوظائف الموضّحة في بداية هذا المستند، يوفّر ClickHouse دعماً تجريبياً للمعاملات وعمليات Commit ووظيفة التراجع.

<div id="requirements">
  ### المتطلبات
</div>

* انشر ClickHouse Keeper أو ZooKeeper لتتبّع المعاملات
* قواعد بيانات Atomic فقط (الافتراضي)
* محرك الجداول Non-Replicated MergeTree فقط
* فعّل دعم المعاملات التجريبي بإضافة هذا الإعداد في `config.d/transactions.xml`:
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

<div id="notes-1">
  ### ملاحظات
</div>

* هذه ميزة تجريبية، ومن المتوقع أن تطرأ عليها تغييرات.
* إذا حدث استثناء أثناء المعاملة، فلا يمكنك إجراء commit للمعاملة.  ويشمل ذلك جميع الاستثناءات، بما فيها استثناءات `UNKNOWN_FUNCTION` الناتجة عن الأخطاء الإملائية.
* المعاملات المتداخلة غير مدعومة؛ أنهِ المعاملة الحالية وابدأ معاملة جديدة بدلًا من ذلك

<div id="configuration">
  ### التهيئة
</div>

تعتمد هذه الأمثلة على خادم ClickHouse أحادي العقدة مع تمكين ClickHouse Keeper.

<div id="enable-experimental-transaction-support">
  #### تمكين دعم المعاملات التجريبي
</div>

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

<div id="basic-configuration-for-a-single-clickhouse-server-node-with-clickhouse-keeper-enabled">
  #### الإعداد الأساسي لعقدة واحدة من خادم ClickHouse مع تفعيل ClickHouse Keeper
</div>

:::note
راجع وثائق [النشر](/ar/deployment-guides/terminology.md) للاطلاع على تفاصيل نشر خادم ClickHouse وتوفير نصاب مناسب من عُقد ClickHouse Keeper. الإعداد المعروض هنا مخصص لأغراض تجريبية.
:::

```xml title=/etc/clickhouse-server/config.d/config.xml
<clickhouse replace="true">
    <logger>
        <level>debug</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
        <size>1000M</size>
        <count>3</count>
    </logger>
    <display_name>node 1</display_name>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <zookeeper>
        <node>
            <host>clickhouse-01</host>
            <port>9181</port>
        </node>
    </zookeeper>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>information</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>clickhouse-keeper-01</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

<div id="example">
  ### مثال
</div>

<div id="verify-that-experimental-transactions-are-enabled">
  #### تحقّق من تفعيل المعاملات التجريبية
</div>

نفّذ `BEGIN TRANSACTION` أو `START TRANSACTION` ثم `ROLLBACK` للتأكّد من أن المعاملات التجريبية مفعّلة، وأن ClickHouse Keeper مفعّل لأنه يُستخدم لتتبّع المعاملات.

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

:::tip
إذا ظهر لك الخطأ التالي، فتحقق من ملف الإعدادات للتأكد من أن `allow_experimental_transactions` مضبوط على `1` (أو أي قيمة أخرى غير `0` أو `false`).

```response
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

يمكنك أيضًا التحقق من ClickHouse Keeper من خلال تنفيذ

```bash
echo ruok | nc localhost 9181
```

من المفترض أن يستجيب ClickHouse Keeper بـ `imok`.
:::

```sql
ROLLBACK
```

```response
Ok.
```

<div id="create-a-table-for-testing">
  #### أنشئ جدولًا لأغراض الاختبار
</div>

:::tip
إنشاء الجداول لا يدعم المعاملات. نفِّذ استعلام DDL هذا خارج أي معاملة.
:::

```sql
CREATE TABLE mergetree_table
(
    `n` Int64
)
ENGINE = MergeTree
ORDER BY n
```

```response
Ok.
```

<div id="begin-a-transaction-and-insert-a-row">
  #### ابدأ معاملة ثم أدرِج صفًا
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (10)
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 10 │
└────┘
```

:::note
يمكنك إجراء استعلام على الجدول من داخل معاملة، وسترى أن الصف قد أُدرج رغم أنه لم يُنفَّذ له commit بعد.
:::

<div id="rollback-the-transaction-and-query-the-table-again">
  #### نفّذ التراجع عن المعاملة، ثم استعلم من الجدول مرة أخرى
</div>

تحقق من أنه تم التراجع عن المعاملة:

```sql
ROLLBACK
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
Ok.

0 rows in set. Elapsed: 0.002 sec.
```

<div id="complete-a-transaction-and-query-the-table-again">
  #### أكمِل المعاملة ثم نفِّذ استعلامًا على الجدول مرة أخرى
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (42)
```

```response
Ok.
```

```sql
COMMIT
```

```response
Ok. Elapsed: 0.002 sec.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 42 │
└────┘
```

<div id="transactions-introspection">
  ### فحص المعاملات
</div>

يمكنك فحص المعاملات عبر الاستعلام عن جدول `system.transactions`، ولكن لاحظ أنه لا يمكنك الاستعلام عن هذا
الجدول من جلسة `clickhouse client` تكون داخل معاملة. افتح جلسة ثانية من `clickhouse client` للاستعلام عن هذا الجدول.

```sql
SELECT *
FROM system.transactions
FORMAT Vertical
```

```response
Row 1:
──────
tid:         (33,61,'51e60bce-6b82-4732-9e1d-b40705ae9ab8')
tid_hash:    11240433987908122467
elapsed:     210.017820947
is_readonly: 1
state:       RUNNING
```

<div id="more-details">
  ## مزيد من التفاصيل
</div>

راجع [التذكرة المرجعية](https://github.com/ClickHouse/ClickHouse/issues/48794) للاطلاع على اختبارات أكثر شمولاً بكثير ومتابعة آخر المستجدات.