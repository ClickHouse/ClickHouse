---
alias: []
description: 'وثائق تنسيق CustomSeparatedWithNamesAndTypes'
input_format: true
keywords: ['CustomSeparatedWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/CustomSeparatedWithNamesAndTypes
title: 'CustomSeparatedWithNamesAndTypes'
doc_type: 'reference'
---

| إدخال | إخراج | الاسم البديل |
| ----- | ----- | ------------ |
| ✔     | ✔     |              |

<div id="description">
  ## الوصف
</div>

يطبع أيضًا صفَّي ترويسة يتضمنان أسماء الأعمدة وأنواع البيانات، على غرار [TabSeparatedWithNamesAndTypes](../TabSeparated/TabSeparatedWithNamesAndTypes.md).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف txt التالي الذي يحمل الاسم `football.txt`:

```text
row('date';'season';'home_team';'away_team';'home_team_goals';'away_team_goals'),row('Date';'Int16';'LowCardinality(String)';'LowCardinality(String)';'Int8';'Int8'),row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

اضبط إعدادات الفواصل المخصّصة:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

أدخِل البيانات:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparatedWithNamesAndTypes;
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

اقرأ البيانات بتنسيق `CustomSeparatedWithNamesAndTypes`:

```sql
SELECT *
FROM football
FORMAT CustomSeparatedWithNamesAndTypes
```

سيكون الإخراج بالتنسيق المخصص الذي تم تكوينه:

```text
row('date';'season';'home_team';'away_team';'home_team_goals';'away_team_goals'),row('Date';'Int16';'LowCardinality(String)';'LowCardinality(String)';'Int8';'Int8'),row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

:::note
إذا كان الإعداد [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) مضبوطًا على `1`،
فستُطابَق أعمدة بيانات الإدخال مع أعمدة الجدول بحسب أسمائها، وسيُتخطى أي عمود يحمل اسمًا غير معروف إذا كان الإعداد [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطًا على `1`.
وإلا فسيُتخطى الصف الأول.
:::

:::note
إذا كان الإعداد [`input_format_with_types_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) مضبوطًا على `1`،
فستُقارَن الأنواع الواردة في بيانات الإدخال بأنواع الأعمدة المقابلة في الجدول. وإلا فسيُتخطى الصف الثاني.
:::