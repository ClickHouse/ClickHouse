---
description: 'يحتفظ محرك الجدول File بالبيانات في ملف بأحد تنسيقات الملفات
  المدعومة (`TabSeparated` و`Native` وما إلى ذلك).'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'محرك الجدول File'
doc_type: 'reference'
---

يحتفظ محرك الجدول File بالبيانات في ملف بأحد [تنسيقات الملفات](/ar/interfaces/formats#formats-overview) المدعومة (`TabSeparated` و`Native` وما إلى ذلك).

سيناريوهات الاستخدام:

* تصدير البيانات من ClickHouse إلى ملف.
* تحويل البيانات من تنسيق إلى آخر.
* تحديث البيانات في ClickHouse عبر تحرير ملف على القرص.

:::note
هذا المحرك غير متاح حاليًا في ClickHouse Cloud، لذا [استخدم الدالة الجدولية S3 بدلًا منه](/ar/sql-reference/table-functions/s3.md).
:::

<div id="usage-in-clickhouse-server">
  ## الاستخدام في خادم ClickHouse
</div>

```sql
File(Format)
```

تحدِّد المعلمة `Format` أحد تنسيقات الملفات المتاحة. لتنفيذ استعلامات
`SELECT`، يجب أن يكون التنسيق مدعومًا للإدخال، ولتنفيذ استعلامات
`INSERT` يجب أن يكون مدعومًا للإخراج. التنسيقات المتاحة مُدرجة في قسم
[التنسيقات](/ar/interfaces/formats#formats-overview).

لا يسمح ClickHouse بتحديد مسار نظام الملفات لـ `File`. وبدلًا من ذلك، يستخدم المجلد المحدد بواسطة الإعداد [path](../../../operations/server-configuration-parameters/settings.md) في تكوين الخادم.

عند إنشاء جدول باستخدام `File(Format)`، يُنشئ دليلًا فرعيًا فارغًا داخل ذلك المجلد. وعند كتابة البيانات إلى هذا الجدول، تُحفَظ في الملف `data.Format` داخل ذلك الدليل الفرعي.

يمكنك إنشاء هذا الدليل الفرعي والملف يدويًا في نظام ملفات الخادم، ثم [ATTACH](../../../sql-reference/statements/attach.md) ببيانات الجدول المطابقة للاسم، وبذلك يمكنك الاستعلام عن البيانات من ذلك الملف.

:::note
توخَّ الحذر عند استخدام هذه الوظيفة، لأن ClickHouse لا يتتبع التغييرات الخارجية التي تطرأ على هذه الملفات. ونتيجة عمليات الكتابة المتزامنة عبر ClickHouse ومن خارجه غير معرّفة.
:::

<div id="example">
  ## مثال
</div>

**1.** قم بإعداد جدول `file_engine_table`:

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

بشكل افتراضي، ينشئ ClickHouse المجلد `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** أنشئ يدويًا الملف `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` بحيث يتضمن:

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** نفِّذ استعلامًا على البيانات:

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## الاستخدام في ClickHouse-local
</div>

في [clickhouse-local](../../../operations/utilities/clickhouse-local.md)، يقبل محرك File مسار الملف بالإضافة إلى `Format`. يمكن تحديد تدفقات الإدخال/الإخراج الافتراضية باستخدام أسماء رقمية أو أسماء مقروءة بشريًا مثل `0` أو `stdin`، و`1` أو `stdout`. كما يمكن قراءة الملفات المضغوطة وكتابتها استنادًا إلى مَعلمة إضافية للمحرك أو امتداد الملف (`gz` أو `br` أو `xz`).

**مثال:**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## تفاصيل التنفيذ
</div>

* يمكن تنفيذ عدة استعلامات `SELECT` بالتزامن، لكن استعلامات `INSERT` ستنتظر بعضها البعض.
* يمكن إنشاء ملف جديد باستخدام استعلام `INSERT`.
* إذا كان الملف موجودًا، فسيُضيف `INSERT` إليه قيمًا جديدة.
* غير مدعوم:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * الفهارس
  * النسخ المتماثل

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — اختياري. يمكن إنشاء ملفات منفصلة بتقسيم البيانات وفقًا لمفتاح التقسيم. في معظم الحالات، لن تحتاج إلى مفتاح تقسيم، وحتى عند الحاجة إليه، فلن تحتاج غالبًا إلى مفتاح تقسيم أكثر تفصيلًا من التقسيم الشهري. لا يسرّع التقسيم الاستعلامات (على عكس تعبير ORDER BY). لا تستخدم أبدًا تقسيمًا شديد التفصيل. ولا تقسّم بياناتك حسب معرّفات العملاء أو أسمائهم (واجعل بدلًا من ذلك معرّف العميل أو اسمه هو العمود الأول في تعبير ORDER BY).

للتقسيم حسب الشهر، استخدم التعبير `toYYYYMM(date_column)`، حيث إن `date_column` هو عمود يحتوي على تاريخ من النوع [Date](/ar/sql-reference/data-types/date.md). وتكون أسماء الأقسام هنا بالتنسيق `"YYYYMM"`.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار الملف. النوع: `LowCardinality(String)`.
* `_file` — اسم الملف. النوع: `LowCardinality(String)`.
* `_size` — حجم الملف بالبايت. النوع: `Nullable(UInt64)`. إذا كان الحجم غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.

<div id="settings">
  ## الإعدادات
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/ar/operations/settings/settings#engine_file_empty_if_not_exists) - يتيح قراءة بيانات فارغة من ملف غير موجود. يكون معطّلًا افتراضيًا.
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/ar/operations/settings/settings#engine_file_truncate_on_insert) - يتيح تفريغ الملف قبل الإدراج فيه. يكون معطّلًا افتراضيًا.
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/ar/operations/settings/settings.md#engine_file_allow_create_multiple_files) - يتيح إنشاء ملف جديد عند كل عملية إدراج إذا كان format يحتوي على لاحقة. يكون معطّلًا افتراضيًا.
* [engine&#95;file&#95;skip&#95;empty&#95;files](/ar/operations/settings/settings.md#engine_file_skip_empty_files) - يتيح تخطي الملفات الفارغة أثناء القراءة. يكون معطّلًا افتراضيًا.
* [storage&#95;file&#95;read&#95;method](/ar/operations/settings/settings#engine_file_empty_if_not_exists) - طريقة قراءة البيانات من ملف التخزين، وهي إحدى القيم التالية: `read`، `pread`، `mmap`. لا تنطبق طريقة `mmap` على clickhouse-server (إذ إنها مخصّصة لـ clickhouse-local). القيمة الافتراضية: `pread` لـ clickhouse-server، و`mmap` لـ clickhouse-local.