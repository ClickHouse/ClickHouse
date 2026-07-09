---
alias: ['TSVRaw', 'Raw']
description: 'توثيق لتنسيق TabSeparatedRaw'
input_format: true
keywords: ['TabSeparatedRaw']
output_format: true
slug: /interfaces/formats/TabSeparatedRaw
title: 'TabSeparatedRaw'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم البديل    |
| ------- | ------- | --------------- |
| ✔       | ✔       | `TSVRaw`, `Raw` |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`TabSeparated`](/ar/interfaces/formats/TabSeparated) في أن الصفوف تُكتب بدون إفلات.

:::note
عند التحليل باستخدام هذا التنسيق، لا يُسمح باستخدام محارف الجدولة أو فواصل الأسطر داخل أي حقل.
:::

للمقارنة بين تنسيق `TabSeparatedRaw` وتنسيق `RawBlob`، انظر: [مقارنة التنسيقات الخام](../RawBLOB.md/#raw-formats-comparison)

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف TSV التالي، المسمى `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

أدرِج البيانات:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRaw;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات باستخدام التنسيق `TabSeparatedRaw`:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRaw
```

سيكون الناتج بتنسيق مفصول بعلامات الجدولة:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
