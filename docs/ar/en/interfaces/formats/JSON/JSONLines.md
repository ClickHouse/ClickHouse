---
alias: ['JSONEachRow', 'JSONLines', 'NDJSON', 'JSONL']
description: 'وثائق تنسيق JSONLines'
keywords: ['JSONLines']
slug: /interfaces/formats/JSONLines
title: 'JSONLines'
doc_type: 'مرجع'
---

| الإدخال | الإخراج | الاسم المستعار                                |
| ------- | ------- | --------------------------------------------- |
| ✔       | ✔       | `JSONEachRow`, `JSONLines`, `NDJSON`, `JSONL` |

<div id="description">
  ## الوصف
</div>

في هذه الصيغة، يُخرج ClickHouse كل صف على شكل كائن JSON مستقل، بحيث يكون كل كائن في سطر جديد.

تُعرف هذه الصيغة أيضًا باسم `JSONEachRow` و`JSONLines` و`NDJSON` (JSON مفصول بأسطر جديدة) أو `JSONL`. وهذه الأسماء مرادفات للصيغة نفسها، ويمكن استخدامها بالتبادل لكلٍّ من الإدخال والإخراج.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف JSON باسم `football.json` ويحتوي على البيانات التالية:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

أدرج البيانات:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONLines;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات بصيغة `JSONLines`:

```sql
SELECT *
FROM football
FORMAT JSONLines
```

سيكون الإخراج بتنسيق JSON:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

سيُتخطّى استيراد أعمدة البيانات ذات الأسماء غير المعروفة إذا ضُبط الإعداد [input&#95;format&#95;skip&#95;unknown&#95;fields](/ar/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) على 1.

<div id="format-settings">
  ## إعدادات الصيغة
</div>
