---
description: 'يتيح هذا المحرك استيراد البيانات إلى SQLite وتصديرها منه، كما يدعم الاستعلام
  عن جداول SQLite مباشرةً من ClickHouse.'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'محرك جدول SQLite'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # محرك جدول SQLite
</div>

<CloudNotSupportedBadge />

يتيح هذا المحرك استيراد البيانات إلى SQLite وتصديرها منه، كما يدعم تنفيذ الاستعلام عن جداول SQLite مباشرةً من ClickHouse.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**معلمات المحرك**

* `db_path` — مسار ملف SQLite الذي يحتوي على قاعدة بيانات.
* `table` — اسم جدول في قاعدة بيانات SQLite، أو استعلام يُمرَّر إلى SQLite كما هو (راجع [تمرير استعلام بدلًا من اسم جدول](#passing-a-query)).

<div id="passing-a-query">
  ## تمرير استعلام بدلًا من اسم جدول
</div>

بدلًا من اسم الجدول، يمكن أن تكون الوسيطة `table` استعلام `SELECT` يُمرَّر إلى SQLite كما هو. ويُستنتج تركيب الجدول من نتيجة الاستعلام. ويمكن كتابة الاستعلام إما على هيئة استعلام فرعي، أو بتغليفه داخل الدالة `query`:

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

هذا الجدول للقراءة فقط: لا يُسمح بتنفيذ `INSERT` عليه. كما تدعم الدالة الجدولية [`sqlite`](/ar/sql-reference/table-functions/sqlite) الصياغة نفسها.

:::note
يحلّل ClickHouse صيغة الاستعلام الفرعي `(SELECT ...)` ثم يعيد تسلسلها قبل إرسالها إلى SQLite. لذلك يجب أن تكون بصيغة ClickHouse SQL صالحة. ولتمرير صياغة خاصة بـ SQLite لا يستطيع ClickHouse تحليلها، استخدم صيغة `query('...')`، إذ يُرسَل نصها إلى SQLite حرفيًا كما هو.

أي `WHERE` أو `LIMIT` خارجي، أو aggregation، وما إلى ذلك، في استعلام ClickHouse المحيط **لا** يُدفَع إلى الاستعلام المُمرَّر — بل يُطبَّق داخل ClickHouse بعد جلب نتيجة الاستعلام كاملة. ولتقييد البيانات المقروءة من SQLite، ضع عامل التصفية داخل الاستعلام المُمرَّر. عند استخدام [`external_table_strict_query = 1`](/ar/operations/settings/settings#external_table_strict_query)، يُرفَض أي عامل تصفية خارجي لا يمكن دفعه، ويُثار استثناء بدلًا من تطبيقه محليًا.
:::

<div id="data-types-support">
  ## دعم أنواع البيانات
</div>

عند تحديد أنواع أعمدة ClickHouse صراحةً في تعريف الجدول، يمكن تفسير أنواع ClickHouse التالية من أعمدة TEXT في SQLite:

* [Date](../../../sql-reference/data-types/date.md)، [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md)، [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* جميع أنواع الأعداد الصحيحة ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

راجع [محرك قاعدة بيانات SQLite](../../../engines/database-engines/sqlite.md#data_types-support) للاطلاع على تعيين الأنواع الافتراضي.

<div id="usage-example">
  ## مثال على الاستخدام
</div>

يوضح استعلامًا لإنشاء جدول SQLite:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

يُرجِع البيانات من الجدول:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**انظر أيضًا**

* محرك [SQLite](../../../engines/database-engines/sqlite.md)
* الدالة الجدولية [sqlite](../../../sql-reference/table-functions/sqlite.md)