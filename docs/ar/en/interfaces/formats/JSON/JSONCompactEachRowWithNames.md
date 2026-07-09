---
alias: []
description: 'توثيق لصيغة JSONCompactEachRowWithNames'
input_format: true
keywords: ['JSONCompactEachRowWithNames']
output_format: true
slug: /interfaces/formats/JSONCompactEachRowWithNames
title: 'JSONCompactEachRowWithNames'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✔       | ✔       |              |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`JSONCompactEachRow`](./JSONCompactEachRow.md) في أنه يطبع أيضًا صف العناوين الذي يحتوي على أسماء الأعمدة، على غرار تنسيق [`TabSeparatedWithNames`](../TabSeparated/TabSeparatedWithNames.md).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف JSON يتضمّن البيانات التالية، باسم `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

أدرِج البيانات:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactEachRowWithNames;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات باستخدام صيغة `JSONCompactEachRowWithNames`:

```sql
SELECT *
FROM football
FORMAT JSONCompactEachRowWithNames
```

سيكون الناتج بتنسيق JSON:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

:::note
إذا كان الإعداد [`input_format_with_names_use_header`](/ar/operations/settings/settings-formats.md/#input_format_with_names_use_header) مضبوطًا على 1،
فستُطابَق أعمدة بيانات الإدخال مع أعمدة الجدول بحسب أسمائها، وسيتم تخطي الأعمدة ذات الأسماء غير المعروفة إذا كان الإعداد [`input_format_skip_unknown_fields`](/ar/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطًا على 1.
وإلا فسيتم تخطي الصف الأول.
:::