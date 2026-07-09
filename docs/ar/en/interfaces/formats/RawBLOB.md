---
description: 'توثيق تنسيق RawBLOB'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
doc_type: 'reference'
---

<div id="description">
  ## الوصف
</div>

تقرأ تنسيقات `RawBLOB` جميع بيانات الإدخال كقيمة واحدة. لا يمكن تحليل سوى جدول يحتوي على حقل واحد من النوع [`String`](/ar/sql-reference/data-types/string.md) أو ما شابهه.
ويُخرَج الناتج بتنسيق ثنائي من دون محددات أو إفلات. وإذا أُخرِجت أكثر من قيمة واحدة، يصبح التنسيق ملتبسًا، ويستحيل قراءة البيانات مجددًا.

<div id="raw-formats-comparison">
  ### مقارنة التنسيقات الخام
</div>

فيما يلي مقارنة بين التنسيقين `RawBLOB` و[`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB`:

* تُخرَج البيانات بتنسيق ثنائي، من دون إفلات؛
* لا توجد فواصل بين القيم؛
* لا يوجد محرف سطر جديد في نهاية كل قيمة.

`TabSeparatedRaw`:

* تُخرَج البيانات من دون إفلات؛
* تحتوي الصفوف على قيم مفصولة بعلامات تبويب؛
* يوجد محرف line feed بعد آخر قيمة في كل صف.

فيما يلي مقارنة بين التنسيقين `RawBLOB` و[RowBinary](./RowBinary/RowBinary.md).

`RawBLOB`:

* تُخرَج حقول String من دون أن يسبقها الطول.

`RowBinary`:

* تُمثَّل حقول String على هيئة طول بتنسيق varint (‏unsigned [LEB128] (https://en.wikipedia.org/wiki/LEB128))، متبوعًا ببايتات السلسلة.

عند تمرير بيانات فارغة إلى مُدخل `RawBLOB`، يطرح ClickHouse استثناء:

```text
Code: 108. DB::Exception: No data to insert
```

<div id="example-usage">
  ## مثال للاستخدام
</div>

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
