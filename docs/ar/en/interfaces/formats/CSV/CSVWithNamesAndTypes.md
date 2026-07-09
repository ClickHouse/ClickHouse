---
alias: []
description: 'توثيق حول تنسيق CSVWithNamesAndTypes'
input_format: true
keywords: ['CSVWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/CSVWithNamesAndTypes
title: 'CSVWithNamesAndTypes'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم البديل |
| ------- | ------- | ------------ |
| ✔       | ✔       |              |

<div id="description">
  ## الوصف
</div>

يطبع أيضًا صفَّي ترويسة يتضمنان أسماء الأعمدة وأنواعها، على غرار [TabSeparatedWithNamesAndTypes](../formats/TabSeparatedWithNamesAndTypes).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

:::tip
اعتبارًا من [الإصدار](https://github.com/ClickHouse/ClickHouse/releases) 23.1، سيكتشف ClickHouse تلقائيًا صفوف الرؤوس في ملفات CSV عند استخدام تنسيق `CSV`، لذا لا حاجة إلى استخدام `CSVWithNames` أو `CSVWithNamesAndTypes`.
:::

باستخدام ملف CSV التالي المسمى `football_types.csv`:

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
Date,Int16,LowCardinality(String),LowCardinality(String),Int8,Int8
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

أنشئ جدولًا:

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

أدرِج البيانات باستخدام تنسيق `CSVWithNamesAndTypes`:

```sql
INSERT INTO football FROM INFILE 'football_types.csv' FORMAT CSVWithNamesAndTypes;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات باستخدام تنسيق `CSVWithNamesAndTypes`:

```sql
SELECT *
FROM football
FORMAT CSVWithNamesAndTypes
```

سيكون الناتج ملف CSV يتضمن صفَّي ترويسة لأسماء الأعمدة وأنواعها:

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"Date","Int16","LowCardinality(String)","LowCardinality(String)","Int8","Int8"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

:::note
إذا كان الإعداد [input&#95;format&#95;with&#95;names&#95;use&#95;header](/ar/operations/settings/settings-formats.md/#input_format_with_names_use_header) مضبوطًا على `1`،
فستُطابَق أعمدة بيانات الإدخال مع أعمدة الجدول حسب أسمائها، وسيتم تخطي الأعمدة ذات الأسماء غير المعروفة إذا كان الإعداد [input&#95;format&#95;skip&#95;unknown&#95;fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطًا على `1`.
وإلا فسيتم تخطي الصف الأول.
:::

:::note
إذا كان الإعداد [input&#95;format&#95;with&#95;types&#95;use&#95;header](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) مضبوطًا على `1`،
فستُقارَن أنواع بيانات الإدخال بأنواع الأعمدة المقابلة في الجدول. وإلا فسيتم تخطي الصف الثاني.
:::