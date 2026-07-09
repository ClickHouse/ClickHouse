---
description: 'توثيق عبارة USE'
sidebar_label: 'USE'
sidebar_position: 53
slug: /sql-reference/statements/use
title: 'عبارة USE'
doc_type: 'reference'
---

```sql
USE [DATABASE] db
```

يتيح لك هذا تعيين قاعدة البيانات الحالية للجلسة.

تُستخدم قاعدة البيانات الحالية للبحث عن الجداول إذا لم تُحدَّد قاعدة البيانات صراحةً في الاستعلام بذكرها قبل اسم الجدول مفصولةً بنقطة.

لا يمكن تنفيذ هذا الاستعلام عند استخدام بروتوكول HTTP، إذ لا يوجد مفهوم للجلسة.