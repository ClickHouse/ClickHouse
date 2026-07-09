---
description: 'تنشئ جدولًا من ملفات في HDFS. تشبه دالة الجدول هذه دالتي الجدول
  url و file.'
sidebar_label: 'hdfs'
sidebar_position: 80
slug: /sql-reference/table-functions/hdfs
title: 'hdfs'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-function">
  # دالة الجدول hdfs
</div>

تنشئ جدولًا من ملفات في HDFS. تشبه دالة الجدول هذه دالتي الجدول [url](../../sql-reference/table-functions/url.md) و[file](../../sql-reference/table-functions/file.md).

<div id="syntax">
  ## الصياغة
</div>

```sql
hdfs(URI, format, structure)
```

<div id="arguments">
  ## الوسائط
</div>

| الوسيط      | الوصف                                                                                                                                                                                 |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `URI`       | عنوان URI النسبي للملف في HDFS. يدعم مسار الملف أنماط glob التالية في وضع القراءة فقط: `*` و `?` و `{abc,def}` و `{N..M}`، حيث إن `N` و `M` — أرقام، و`'abc'` و `'def'` — سلاسل نصية. |
| `format`    | [تنسيق](/ar/sql-reference/formats) الملف.                                                                                                                                                |
| `structure` | بنية الجدول. التنسيق: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                  |

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات من الملف المحدد أو كتابتها إليه.

**مثال**

جدول من `hdfs://hdfs1:9000/test` واختيار أول صفّين منه:

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="globs_in_path">
  ## أنماط glob في المسار
</div>

قد تستخدم المسارات أنماط glob. يجب أن تطابق الملفات نمط المسار بالكامل، وليس اللاحقة أو البادئة فقط.

* `*` — يمثّل عددًا غير محدد من المحارف باستثناء `/`، بما في ذلك السلسلة الفارغة.
* `**` — يمثّل جميع الملفات داخل مجلد على نحوٍ تكراري.
* `?` — يمثّل محرفًا واحدًا عشوائيًا.
* `{some_string,another_string,yet_another_one}` — يستبدل بأيٍّ من السلاسل `'some_string', 'another_string', 'yet_another_one'`. ويمكن أن تحتوي السلاسل على الرمز `/`.
* `{N..M}` — يمثّل أي رقم `>= N` و `<= M`.

البُنى التي تستخدم `{}` مشابهة لدوال الجدول [remote](remote.md) و [file](file.md).

**مثال**

1. لنفترض أن لدينا عدة ملفات بعناوين URI التالية على HDFS:

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. استعلم عن عدد الصفوف في هذه الملفات:

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. استعلم عن عدد الصفوف في جميع ملفات الدليلين التاليين:

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
إذا كانت قائمة الملفات لديك تحتوي على نطاقات رقمية بأصفار بادئة، فاستخدم الصيغة ذات الأقواس المعقوفة لكل رقم على حدة أو استخدم `?`.
:::

**مثال**

استعلم عن البيانات من الملفات التي تحمل الأسماء `file000` و`file001` و... و`file999`:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان الحجم غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.

<div id="hive-style-partitioning">
  ## إعداد use_hive_partitioning
</div>

عند تعيين `use_hive_partitioning` إلى 1، سيكتشف ClickHouse التقسيم بنمط Hive في المسار (`/name=value/`) وسيتيح استخدام أعمدة التقسيم كأعمدة افتراضية في الاستعلام. وستحمل هذه الأعمدة الافتراضية الأسماء نفسها الموجودة في مسار التقسيم.

**مثال**

استخدم عمودًا افتراضيًا أُنشئ باستخدام التقسيم بنمط Hive

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="storage-settings">
  ## إعدادات التخزين
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/ar/operations/settings/settings.md#hdfs_truncate_on_insert) - يتيح اقتطاع الملف قبل الإدراج فيه. يكون معطّلًا افتراضيًا.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/ar/operations/settings/settings.md#hdfs_create_new_file_on_insert) - يتيح إنشاء ملف جديد عند كل عملية إدراج إذا كان التنسيق يتضمن لاحقة. يكون معطّلًا افتراضيًا.
* [hdfs&#95;skip&#95;empty&#95;files](/ar/operations/settings/settings.md#hdfs_skip_empty_files) - يتيح تخطي الملفات الفارغة أثناء القراءة. يكون معطّلًا افتراضيًا.

<div id="related">
  ## انظر أيضًا
</div>

* [الأعمدة الافتراضية](../../engines/table-engines/index.md#table_engines-virtual_columns)