---
description: 'توثيق لعبارة EXISTS'
sidebar_label: 'EXISTS'
sidebar_position: 45
slug: /sql-reference/statements/exists
title: 'عبارة EXISTS'
doc_type: 'مرجع'
---

```sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

يُرجع عمودًا واحدًا من النوع `UInt8`، يحتوي على القيمة الوحيدة `0` إذا كان الجدول أو قاعدة البيانات غير موجودين، أو `1` إذا كان الجدول موجودًا في قاعدة البيانات المحددة.