---
alias: []
description: 'وثائق لتنسيق JSONColumns'
input_format: true
keywords: ['JSONColumns']
output_format: true
slug: /interfaces/formats/JSONColumns
title: 'JSONColumns'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✔       |                |

<div id="description">
  ## الوصف
</div>

:::tip
يوفّر خرج تنسيقات JSONColumns* اسم الحقل في ClickHouse ثم محتوى كل صف في الجدول لهذا الحقل؛
وبصريًا، تُدوَّر البيانات 90 درجة إلى اليسار.
:::

في هذا التنسيق، تُمثَّل جميع البيانات على شكل كائن JSON واحد.

:::note
يخزّن تنسيق `JSONColumns` جميع البيانات مؤقتًا في الذاكرة ثم يُخرجها ككتلة واحدة، لذا قد يؤدي ذلك إلى ارتفاع استهلاك الذاكرة.
:::

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف JSON يتضمن البيانات التالية، باسم `football.json`:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

أدخل البيانات:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONColumns;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات باستخدام تنسيق `JSONColumns`:

```sql
SELECT *
FROM football
FORMAT JSONColumns
```

سيكون الإخراج بتنسيق JSON:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

أثناء الاستيراد، سيتم تخطي الأعمدة ذات الأسماء غير المعروفة إذا كان الإعداد [`input_format_skip_unknown_fields`](/ar/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطًا على `1`.
وسيتم ملء الأعمدة غير الموجودة في الكتلة بالقيم الافتراضية (يمكنك هنا استخدام الإعداد [`input_format_defaults_for_omitted_fields`](/ar/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields))