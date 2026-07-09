---
alias: []
description: 'وثائق تنسيق CustomSeparated'
input_format: true
keywords: ['CustomSeparated']
output_format: true
slug: /interfaces/formats/CustomSeparated
title: 'CustomSeparated'
doc_type: 'مرجع'
---

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✔       | ✔       |              |

<div id="description">
  ## الوصف
</div>

على غرار [Template](../Template/Template.md)، لكنه يطبع أو يقرأ جميع أسماء الأعمدة وأنواعها، ويستخدم قاعدة الإفلات من الإعداد [format&#95;custom&#95;escaping&#95;rule](../../../operations/settings/settings-formats.md/#format_custom_escaping_rule) والفواصل من الإعدادات التالية:

* [format&#95;custom&#95;field&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_custom_field_delimiter)
* [format&#95;custom&#95;row&#95;before&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
* [format&#95;custom&#95;row&#95;after&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
* [format&#95;custom&#95;row&#95;between&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
* [format&#95;custom&#95;result&#95;before&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
* [format&#95;custom&#95;result&#95;after&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_custom_result_after_delimiter)

:::note
لا يستخدم إعدادات قواعد الإفلات ولا الفواصل المأخوذة من سلاسل التنسيق.
:::

يوجد أيضًا التنسيق [`CustomSeparatedIgnoreSpaces`](../CustomSeparated/CustomSeparatedIgnoreSpaces.md)، وهو مشابه لـ [TemplateIgnoreSpaces](../Template//TemplateIgnoreSpaces.md).

<div id="example-usage">
  ## مثال على الاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف النص التالي المسمّى `football.txt`:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

اضبط إعدادات الفاصل المخصص:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

أدرج البيانات:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparated;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اضبط إعدادات الفاصل المخصّص:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

اقرأ البيانات باستخدام التنسيق `CustomSeparated`:

```sql
SELECT *
FROM football
FORMAT CustomSeparated
```

سيكون الإخراج بالتنسيق المخصص المُعَدّ:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

إعدادات إضافية:

| الإعداد                                                                                                                                                                                    | الوصف                                                                                                                          | الافتراضي |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ | --------- |
| [input&#95;format&#95;custom&#95;detect&#95;header](../../../operations/settings/settings-formats.md/#input_format_custom_detect_header)                                                   | يفعّل الاكتشاف التلقائي لصفّ رأس يحتوي على الأسماء والأنواع، إن وُجد.                                                          | `true`    |
| [input&#95;format&#95;custom&#95;skip&#95;trailing&#95;empty&#95;lines](../../../operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines)                   | يتجاوز الأسطر الفارغة اللاحقة في نهاية الملف.                                                                                  | `false`   |
| [input&#95;format&#95;custom&#95;allow&#95;variable&#95;number&#95;of&#95;columns](../../../operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) | يسمح بوجود عدد متغيّر من الأعمدة في تنسيق CustomSeparated، ويتجاهل الأعمدة الإضافية ويستخدم القيم الافتراضية للأعمدة المفقودة. | `false`   |