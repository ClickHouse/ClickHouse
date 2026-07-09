---
description: 'دالة جدول تتيح تحويل البيانات المُرسلة إلى الخادم ببنية معيّنة
  وإدراجها بكفاءة في جدول ذي بنية مختلفة.'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

`input(structure)` - دالة جدول تتيح تحويل البيانات المُرسلة إلى
الخادم ببنية معيّنة وإدراجها بكفاءة في جدول ذي بنية مختلفة.

`structure` - بنية البيانات المُرسلة إلى الخادم بالتنسيق التالي `'column1_name column1_type, column2_name column2_type, ...'`.
على سبيل المثال: `'id UInt32, name String'`.

لا يمكن استخدام هذه الدالة إلا في استعلام `INSERT SELECT` ولمرة واحدة فقط، لكنها بخلاف ذلك تتصرف مثل دالة جدول عادية
(على سبيل المثال، يمكن استخدامها في استعلام فرعي، وما إلى ذلك).

يمكن إرسال البيانات بأي طريقة كما في استعلام `INSERT` العادي، وتمريرها بأي [تنسيق](/ar/sql-reference/formats)
متاح، على أن يُحدَّد في نهاية الاستعلام (على خلاف `INSERT SELECT` العادي).

الميزة الرئيسية لهذه الدالة هي أنه عندما يتلقى الخادم البيانات من العميل، فإنه يحولها بالتزامن
وفقًا لقائمة التعبيرات في عبارة `SELECT` ثم يدرجها في الجدول الهدف. ولا يتم إنشاء جدول مؤقت
يحتوي على جميع البيانات المنقولة.

<div id="examples">
  ## أمثلة
</div>

* افترض أن جدول `test` له البنية التالية `(a String, b String)`
  وأن البيانات في `data.csv` لها بنية مختلفة `(col1 String, col2 Date, col3 Int32)`. يكون استعلام إدراج
  البيانات من `data.csv` في جدول `test` مع التحويل المتزامن كما يلي:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

* إذا كان `data.csv` يحتوي على بيانات لها البنية نفسها `test_structure` مثل الجدول `test`، فإن هذين الاستعلامين متكافئان:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```