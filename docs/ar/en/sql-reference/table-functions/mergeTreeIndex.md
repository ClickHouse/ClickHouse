---
description: 'يمثّل محتويات ملفات الفهرس وملفات العلامات في جداول MergeTree.
  ويمكن استخدامه لفحص البنية الداخلية.'
sidebar_label: 'mergeTreeIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeIndex
title: 'mergeTreeIndex'
doc_type: 'reference'
---

يمثّل محتويات ملفات الفهرس وملفات العلامات في جداول MergeTree. ويمكن استخدامه لفحص البنية الداخلية.

<div id="syntax">
  ## الصيغة
</div>

```sql
mergeTreeIndex(database, table [, with_marks = true] [, with_minmax = true])
```

<div id="arguments">
  ## الوسائط
</div>

| الوسيط        | الوصف                                                      |
| ------------- | ---------------------------------------------------------- |
| `database`    | اسم قاعدة البيانات التي سيُقرأ منها الفهرس والعلامات.      |
| `table`       | اسم الجدول الذي سيُقرأ منه الفهرس والعلامات.               |
| `with_marks`  | ما إذا كان سيتم تضمين الأعمدة المصحوبة بعلامات في النتيجة. |
| `with_minmax` | ما إذا كان سيتم تضمين فهرس min-max في النتيجة.             |

<div id="returned_value">
  ## القيمة المعادة
</div>

كائن جدول يحتوي على أعمدة تتضمن قيم الفهرس الأساسي وفهرس min-max (إذا كان مفعّلًا) للجدول المصدر، وأعمدة تتضمن قيم العلامات (إذا كانت مفعّلة) لجميع الملفات الممكنة في أجزاء البيانات الخاصة بالجدول المصدر، بالإضافة إلى الأعمدة الافتراضية:

* `part_name` - اسم جزء البيانات.
* `mark_number` - رقم العلامة الحالية في جزء البيانات.
* `rows_in_granule` - عدد الصفوف في الحبيبة الحالية.

قد يحتوي عمود العلامات على القيمة `(NULL, NULL)` إذا كان العمود غير موجود في جزء البيانات أو لم تُكتب العلامات الخاصة بأحد التدفقات الفرعية التابعة له (على سبيل المثال، في الأجزاء المدمجة).

<div id="usage-example">
  ## مثال على الاستخدام
</div>

```sql
CREATE TABLE test_table
(
    `id` UInt64,
    `n` UInt64,
    `arr` Array(UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 8;

INSERT INTO test_table SELECT number, number, range(number % 5) FROM numbers(5);

INSERT INTO test_table SELECT number, number, range(number % 5) FROM numbers(10, 10);
```

```sql
SELECT * FROM mergeTreeIndex(currentDatabase(), test_table, with_marks = true);
```

```text
┌─part_name─┬─mark_number─┬─rows_in_granule─┬─id─┬─id.mark─┬─n.mark──┬─arr.size0.mark─┬─arr.mark─┐
│ all_1_1_0 │           0 │               3 │  0 │ (0,0)   │ (42,0)  │ (NULL,NULL)    │ (84,0)   │
│ all_1_1_0 │           1 │               2 │  3 │ (133,0) │ (172,0) │ (NULL,NULL)    │ (211,0)  │
│ all_1_1_0 │           2 │               0 │  4 │ (271,0) │ (271,0) │ (NULL,NULL)    │ (271,0)  │
└───────────┴─────────────┴─────────────────┴────┴─────────┴─────────┴────────────────┴──────────┘
┌─part_name─┬─mark_number─┬─rows_in_granule─┬─id─┬─id.mark─┬─n.mark─┬─arr.size0.mark─┬─arr.mark─┐
│ all_2_2_0 │           0 │               3 │ 10 │ (0,0)   │ (0,0)  │ (0,0)          │ (0,0)    │
│ all_2_2_0 │           1 │               3 │ 13 │ (0,24)  │ (0,24) │ (0,24)         │ (0,24)   │
│ all_2_2_0 │           2 │               3 │ 16 │ (0,48)  │ (0,48) │ (0,48)         │ (0,80)   │
│ all_2_2_0 │           3 │               1 │ 19 │ (0,72)  │ (0,72) │ (0,72)         │ (0,128)  │
│ all_2_2_0 │           4 │               0 │ 19 │ (0,80)  │ (0,80) │ (0,80)         │ (0,160)  │
└───────────┴─────────────┴─────────────────┴────┴─────────┴────────┴────────────────┴──────────┘
```

```sql
DESCRIBE mergeTreeIndex(currentDatabase(), test_table, with_marks = true) SETTINGS describe_compact_output = 1;
```

```text
┌─name────────────┬─type─────────────────────────────────────────────────────────────────────────────────────────────┐
│ part_name       │ String                                                                                           │
│ mark_number     │ UInt64                                                                                           │
│ rows_in_granule │ UInt64                                                                                           │
│ id              │ UInt64                                                                                           │
│ id.mark         │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ n.mark          │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ arr.size0.mark  │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ arr.mark        │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
└─────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────┘
```