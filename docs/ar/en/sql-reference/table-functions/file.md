---
description: 'محرك جدول يوفّر واجهة شبيهة بالجدول لإجراء SELECT من
  الملفات وINSERT إليها، على غرار دالة الجدول s3. استخدم `file` عند العمل
  مع الملفات المحلية، و`s3` عند العمل مع الحاويات في تخزين الكائنات مثل
  S3 أو GCS أو MinIO.'
sidebar_label: 'file'
sidebar_position: 60
slug: /sql-reference/table-functions/file
title: 'file'
doc_type: 'مرجع'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="file-table-function">
  # دالة الجدول file
</div>

محرك جدول يوفّر واجهة شبيهة بالجدول لتنفيذ `SELECT` من الملفات و`INSERT` إليها، على نحوٍ مشابه لدالة الجدول [s3](/ar/sql-reference/table-functions/s3.md). استخدم `file` عند العمل مع الملفات المحلية، واستخدم `s3` عند العمل مع الحاويات في تخزين الكائنات مثل S3 أو GCS أو MinIO.

يمكن استخدام الدالة `file` في استعلامات `SELECT` و`INSERT` لقراءة الملفات أو الكتابة إليها.

<div id="syntax">
  ## الصيغة
</div>

```sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

في استعلامات `SELECT`، يمكن أيضًا أن يكون `path` تعبيرًا يُرجع `Array(String)`:

```sql
file(['file1.csv', 'file2.csv'], 'CSV', 'column1 UInt32, column2 UInt32')
```

<div id="arguments">
  ## المعاملات
</div>

| المعلمة           | الوصف                                                                                                                                                                                                                                                                                                                                                               |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`            | المسار النسبي إلى الملف انطلاقًا من [user&#95;files&#95;path](/ar/operations/server-configuration-parameters/settings.md#user_files_path)، أو `Array(String)` من المسارات في استعلامات `SELECT`. ويدعم في وضع القراءة فقط [أنماط glob](#globs-in-path) التالية: `*` و `?` و `{abc,def}` (حيث تكون `'abc'` و `'def'` سلاسل نصية) و `{N..M}` (حيث يكون `N` و `M` عددين). |
| `path_to_archive` | المسار النسبي إلى أرشيف zip/tar/7z. ويدعم أنماط glob نفسها التي يدعمها `path`.                                                                                                                                                                                                                                                                                      |
| `format`          | [تنسيق](/ar/interfaces/formats) الملف.                                                                                                                                                                                                                                                                                                                                 |
| `structure`       | بنية الجدول. التنسيق: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                |
| `compression`     | نوع الضغط الموجود عند استخدامه في استعلام `SELECT`، أو نوع الضغط المطلوب عند استخدامه في استعلام `INSERT`. وأنواع الضغط المدعومة هي `gz` و `br` و `xz` و `zst` و `lz4` و `bz2`.                                                                                                                                                                                     |

:::tip
عند عدم تحديد المعامل `structure`، يستدل ClickHouse على المخطط من التنسيق نفسه.
وتنتج التنسيقات المختلفة أسماء أعمدة وأنواعًا افتراضية مختلفة.
ولعرض المخطط لتنسيق معيّن، استخدم [`DESC`](/ar/sql-reference/statements/describe-table) مع دالة الجدول [`format`](/ar/sql-reference/table-functions/format).

على سبيل المثال:

```sql
DESC format(LineAsString, 'Hello\nWorld')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

:::

<div id="returned_value">
  ## القيمة المُعادة
</div>

جدول لقراءة البيانات من ملف أو لكتابتها فيه.

<div id="examples-for-writing-to-a-file">
  ## أمثلة على الكتابة إلى ملف
</div>

<div id="write-to-a-tsv-file">
  ### الكتابة إلى ملف TSV
</div>

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

ونتيجة لذلك، تُكتب البيانات في الملف `test.tsv`:

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1    2    3
3    2    1
1    3    2
```

<div id="partitioned-write-to-multiple-tsv-files">
  ### كتابة مقسّمة بحسب الأقسام إلى عدة ملفات TSV
</div>

إذا حدّدت تعبير `PARTITION BY` عند إدراج البيانات في دالة الجدول من النوع `file`، فسيُنشأ ملف منفصل لكل قسم. ويساعد تقسيم البيانات إلى ملفات منفصلة على تحسين أداء عمليات القراءة.

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

ونتيجةً لذلك، تُكتب البيانات في ثلاثة ملفات: `test_1.tsv` و`test_2.tsv` و`test_3.tsv`.

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3    2    1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1    3    2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1    2    3
```

<div id="examples-for-reading-from-a-file">
  ## أمثلة لقراءة البيانات من ملف
</div>

<div id="select-from-a-csv-file">
  ### SELECT من ملف CSV
</div>

أولًا، اضبط `user_files_path` في إعدادات الخادم ثم جهّز ملفًا باسم `test.csv`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

ثم اقرأ البيانات من `test.csv` إلى جدول، ثم اعرض أول صفين منه:

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="inserting-data-from-a-file-into-a-table">
  ### إدراج البيانات من ملف في جدول
</div>

```sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

قراءة البيانات من `table.csv`، الموجود في `archive1.zip` أو `archive2.zip` أو كليهما:

```sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

<div id="globs-in-path">
  ## أنماط glob في المسار
</div>

يمكن أن تستخدم المسارات أنماط glob. يجب أن تطابق الملفات نمط المسار بالكامل، لا اللاحقة أو البادئة فقط. وهناك استثناء واحد: إذا كان المسار يشير إلى
دليل موجود ولا يستخدم أنماط glob، فستُضاف `*` ضمنيًا إلى المسار بحيث
تُحدَّد جميع الملفات الموجودة في الدليل.

* `*` — تمثل أي عدد من المحارف باستثناء `/`، بما في ذلك السلسلة الفارغة.
* `?` — تمثل محرفًا واحدًا عشوائيًا.
* `{some_string,another_string,yet_another_one}` — تُستبدل بأي من السلاسل `'some_string', 'another_string', 'yet_another_one'`. ويمكن أن تحتوي السلاسل على الرمز `/`.
* `{N..M}` — يمثل أي عدد `>= N` و `<= M`.
* `**` - تمثل جميع الملفات داخل مجلد، بشكل递اعي.

تُعد التركيبات التي تحتوي على `{}` مشابهة لدوال الجدول [remote](remote.md) و [hdfs](hdfs.md).

<div id="examples">
  ## أمثلة
</div>

**مثال**

لنفترض وجود هذه الملفات بالمسارات النسبية التالية:

* `some_dir/some_file_1`
* `some_dir/some_file_2`
* `some_dir/some_file_3`
* `another_dir/some_file_1`
* `another_dir/some_file_2`
* `another_dir/some_file_3`

نفّذ استعلامًا للحصول على العدد الإجمالي للصفوف في جميع الملفات:

```sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

تعبير مسار بديل يحقق النتيجة نفسها:

```sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

استعلم عن إجمالي عدد الصفوف في `some_dir` باستخدام `*` الضمني:

```sql
SELECT count(*) FROM file('some_dir', 'TSV', 'name String, value UInt32');
```

:::note
إذا كانت قائمة الملفات لديك تتضمن نطاقات رقمية بأصفار بادئة، فاستخدم الصيغة التي تحتوي على أقواس لكل رقم على حدة، أو استخدم `?`.
:::

**مثال**

استعلم عن العدد الإجمالي للصفوف في الملفات المسماة `file000` و`file001` و... و`file999`:

```sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**مثال**

استعلم عن إجمالي عدد الصفوف في جميع الملفات داخل الدليل `big_dir/` بشكلٍ تكراري:

```sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**مثال**

نفّذ استعلامًا للحصول على العدد الإجمالي للصفوف من جميع الملفات `file002` داخل أي مجلد ضمن الدليل `big_dir/` بشكلٍ تكراري:

```sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان حجم الملف غير معروف، فستكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، فستكون القيمة `NULL`.

<div id="hive-style-partitioning">
  ## إعداد use_hive_partitioning
</div>

عند ضبط الإعداد `use_hive_partitioning` على القيمة 1، سيكتشف ClickHouse التقسيم بأسلوب Hive في المسار (`/name=value/`)، وسيتيح استخدام أعمدة التقسيم كأعمدة افتراضية في الاستعلام. وستحمل هذه الأعمدة الافتراضية الأسماء نفسها الموجودة في مسار التقسيم.

**مثال**

استخدام عمود افتراضي تم إنشاؤه باستخدام التقسيم بأسلوب Hive

```sql
SELECT * FROM file('data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="settings">
  ## الإعدادات
</div>

| الإعداد                                                                                                                                 | الوصف                                                                                                                                                                                       |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/ar/operations/settings/settings#engine_file_empty_if_not_exists)                    | يتيح إرجاع بيانات فارغة من ملف غير موجود. يكون معطّلًا افتراضيًا.                                                                                                                           |
| [engine&#95;file&#95;truncate&#95;on&#95;insert](/ar/operations/settings/settings#engine_file_truncate_on_insert)                          | يتيح اقتطاع الملف قبل الإدراج فيه. يكون معطّلًا افتراضيًا.                                                                                                                                  |
| [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/ar/operations/settings/settings.md#engine_file_allow_create_multiple_files) | يتيح إنشاء ملف جديد عند كل عملية insert إذا كان للتنسيق لاحقة. يكون معطّلًا افتراضيًا.                                                                                                      |
| [engine&#95;file&#95;skip&#95;empty&#95;files](/ar/operations/settings/settings.md#engine_file_skip_empty_files)                           | يتيح تخطي الملفات الفارغة أثناء القراءة. يكون معطّلًا افتراضيًا.                                                                                                                            |
| [storage&#95;file&#95;read&#95;method](/ar/operations/settings/settings#engine_file_empty_if_not_exists)                                   | طريقة قراءة البيانات من ملف التخزين، وهي إحدى القيم التالية: read أو pread أو mmap (فقط لـ clickhouse-local). القيمة الافتراضية: `pread` لـ clickhouse-server، و`mmap` لـ clickhouse-local. |

<div id="related">
  ## ذات صلة
</div>

* [الأعمدة الافتراضية](/ar/engines/table-engines/index.md#table_engines-virtual_columns)
* [إعادة تسمية الملفات بعد اكتمال المعالجة](/ar/operations/settings/settings.md#rename_files_after_processing)