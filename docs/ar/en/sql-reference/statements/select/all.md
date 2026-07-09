---
description: 'توثيق بند ALL'
sidebar_label: 'ALL'
slug: /sql-reference/statements/select/all
title: 'بند ALL'
doc_type: 'reference'
---

إذا كانت هناك عدة صفوف متطابقة في جدول، فإن `ALL` يعيدها جميعًا. ويُعد `SELECT ALL` مطابقًا لـ `SELECT` من دون `DISTINCT`. وإذا تم تحديد كلٍّ من `ALL` و`DISTINCT`، فسيتم إطلاق استثناء.

يمكن تحديد `ALL` داخل الدوال التجميعية، رغم أنه لا يترك أي تأثير عملي على نتيجة الاستعلام.

على سبيل المثال:

```sql
SELECT sum(ALL number) FROM numbers(10);
```

وهو مكافئ لـ:

```sql
SELECT sum(number) FROM numbers(10);
```