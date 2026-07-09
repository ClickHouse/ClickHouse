---
description: 'توثيق لتنسيق JSONCompactStringsEachRowWithNamesAndTypes'
keywords: ['JSONCompactStringsEachRowWithNamesAndTypes']
slug: /interfaces/formats/JSONCompactStringsEachRowWithNamesAndTypes
title: 'JSONCompactStringsEachRowWithNamesAndTypes'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✔       |                |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق `JSONCompactEachRow` في أنه يطبع أيضًا صفَّي ترويسة يحتويان على أسماء الأعمدة وأنواعها، على غرار [TabSeparatedWithNamesAndTypes](/ar/interfaces/formats/TabSeparatedRawWithNamesAndTypes).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف JSON باسم `football.json` يحتوي على البيانات التالية:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

أدخِل البيانات:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactStringsEachRowWithNamesAndTypes;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات باستخدام التنسيق `JSONCompactStringsEachRowWithNamesAndTypes`:

```sql
SELECT *
FROM football
FORMAT JSONCompactStringsEachRowWithNamesAndTypes
```

سيكون الإخراج بتنسيق JSON:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

:::note
إذا كان الإعداد [input&#95;format&#95;with&#95;names&#95;use&#95;header](/ar/operations/settings/settings-formats.md/#input_format_with_names_use_header) مضبوطًا على 1،
فستُطابَق الأعمدة في بيانات الإدخال مع أعمدة الجدول بحسب أسمائها، وسيتم تخطي الأعمدة ذات الأسماء غير المعروفة إذا كان الإعداد [input&#95;format&#95;skip&#95;unknown&#95;fields](/ar/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطًا على 1.
أما خلاف ذلك، فسيتم تخطي الصف الأول.
:::

:::note
إذا كان الإعداد [input&#95;format&#95;with&#95;types&#95;use&#95;header](/ar/operations/settings/settings-formats.md/#input_format_with_types_use_header) مضبوطًا على 1،
فستُقارَن الأنواع في بيانات الإدخال بأنواع الأعمدة المقابلة في الجدول. أما خلاف ذلك، فسيتم تخطي الصف الثاني.
:::