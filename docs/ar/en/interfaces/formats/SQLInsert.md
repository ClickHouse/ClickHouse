---
alias: []
description: 'توثيق تنسيق SQLInsert'
input_format: false
keywords: ['SQLInsert']
output_format: true
slug: /interfaces/formats/SQLInsert
title: 'SQLInsert'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✗       | ✔       |                |

<div id="description">
  ## الوصف
</div>

يُخرج البيانات على شكل سلسلة من عبارات `INSERT INTO table (columns...) VALUES (...), (...) ...;`.

<div id="example-usage">
  ## مثال على الاستخدام
</div>

مثال:

```sql
SELECT number AS x, number + 1 AS y, 'Hello' AS z FROM numbers(10) FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size = 2
```

```sql
INSERT INTO table (x, y, z) VALUES (0, 1, 'Hello'), (1, 2, 'Hello');
INSERT INTO table (x, y, z) VALUES (2, 3, 'Hello'), (3, 4, 'Hello');
INSERT INTO table (x, y, z) VALUES (4, 5, 'Hello'), (5, 6, 'Hello');
INSERT INTO table (x, y, z) VALUES (6, 7, 'Hello'), (7, 8, 'Hello');
INSERT INTO table (x, y, z) VALUES (8, 9, 'Hello'), (9, 10, 'Hello');
```

لقراءة البيانات التي يُنتجها هذا التنسيق، يمكنك استخدام تنسيق الإدخال [MySQLDump](../formats/MySQLDump.md).

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| Setting                                                                                                                                         | Description                                     | Default   |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- | --------- |
| [`output_format_sql_insert_max_batch_size`](../../operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size)             | الحد الأقصى لعدد الصفوف في عبارة INSERT واحدة. | `65505`   |
| [`output_format_sql_insert_table_name`](../../operations/settings/settings-formats.md/#output_format_sql_insert_table_name)                     | اسم الجدول في استعلام INSERT الناتج.            | `'table'` |
| [`output_format_sql_insert_include_column_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names) | تضمين أسماء الأعمدة في استعلام INSERT.          | `true`    |
| [`output_format_sql_insert_use_replace`](../../operations/settings/settings-formats.md/#output_format_sql_insert_use_replace)                   | استخدام عبارة REPLACE بدلًا من INSERT.         | `false`   |
| [`output_format_sql_insert_quote_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_quote_names)                   | وضع أسماء الأعمدة بين علامتي &quot;&#96;&quot;. | `true`    |