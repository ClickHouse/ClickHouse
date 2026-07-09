---
alias: []
description: 'توثيق تنسيق Npy'
input_format: true
keywords: ['Npy']
output_format: true
slug: /interfaces/formats/Npy
title: 'Npy'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✔       | ✔       |              |

<div id="description">
  ## الوصف
</div>

صُمم التنسيق `Npy` لتحميل مصفوفة NumPy من ملف `.npy` إلى ClickHouse.
تنسيق ملفات NumPy هو تنسيق ثنائي يُستخدم لتخزين مصفوفات البيانات الرقمية بكفاءة.
أثناء الاستيراد، يتعامل ClickHouse مع البعد الأعلى على أنه مصفوفة من الصفوف ذات عمود واحد.

يوضح الجدول أدناه أنواع بيانات Npy المدعومة والنوع المقابل لكلٍ منها في ClickHouse:

<div id="data_types-matching">
  ## مطابقة أنواع البيانات
</div>

| نوع بيانات Npy (`INSERT`) | نوع بيانات ClickHouse                                   | نوع بيانات Npy (`SELECT`) |
| ------------------------- | ------------------------------------------------------- | ------------------------- |
| `i1`                      | [Int8](/ar/sql-reference/data-types/int-uint.md)           | `i1`                      |
| `i2`                      | [Int16](/ar/sql-reference/data-types/int-uint.md)          | `i2`                      |
| `i4`                      | [Int32](/ar/sql-reference/data-types/int-uint.md)          | `i4`                      |
| `i8`                      | [Int64](/ar/sql-reference/data-types/int-uint.md)          | `i8`                      |
| `u1`, `b1`                | [UInt8](/ar/sql-reference/data-types/int-uint.md)          | `u1`                      |
| `u2`                      | [UInt16](/ar/sql-reference/data-types/int-uint.md)         | `u2`                      |
| `u4`                      | [UInt32](/ar/sql-reference/data-types/int-uint.md)         | `u4`                      |
| `u8`                      | [UInt64](/ar/sql-reference/data-types/int-uint.md)         | `u8`                      |
| `f2`, `f4`                | [Float32](/ar/sql-reference/data-types/float.md)           | `f4`                      |
| `f8`                      | [Float64](/ar/sql-reference/data-types/float.md)           | `f8`                      |
| `S`, `U`                  | [String](/ar/sql-reference/data-types/string.md)           | `S`                       |
|                           | [FixedString](/ar/sql-reference/data-types/fixedstring.md) | `S`                       |

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="saving-an-array-in-npy-format-using-python">
  ### حفظ مصفوفة بتنسيق ‎.npy‎ باستخدام بايثون
</div>

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

<div id="reading-a-numpy-file-in-clickhouse">
  ### قراءة ملف NumPy في ClickHouse
</div>

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

<div id="selecting-data">
  ### تحديد البيانات
</div>

يمكنك تحديد بيانات من جدول في ClickHouse وحفظها في ملف بتنسيق Npy باستخدام الأمر التالي عبر clickhouse-client:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
