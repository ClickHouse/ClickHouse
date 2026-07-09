---
description: 'تُستخدم لأغراض الاختبار باعتبارها أسرع طريقة لإنشاء عدد كبير من الصفوف.
  وهي مشابهة لجدولي النظام `system.zeros` و`system.zeros_mt`.'
sidebar_label: 'zeros'
sidebar_position: 145
slug: /sql-reference/table-functions/zeros
title: 'zeros'
doc_type: 'reference'
---

* `zeros(N)` – تُرجع جدولًا يحتوي على عمود واحد باسم &#39;zero&#39; من النوع (UInt8)، ويتضمن القيمة الصحيحة 0 مكررة `N` مرة
* `zeros_mt(N)` – مماثلة لـ `zeros`، لكنها تستخدم خيوط تنفيذ متعددة.

تُستخدم هذه الدالة لأغراض الاختبار باعتبارها أسرع طريقة لإنشاء عدد كبير من الصفوف. وهي مشابهة لجدولي النظام `system.zeros` و`system.zeros_mt`.

الاستعلامات التالية متكافئة:

```sql
SELECT * FROM zeros(10);
SELECT * FROM system.zeros LIMIT 10;
SELECT * FROM zeros_mt(10);
SELECT * FROM system.zeros_mt LIMIT 10;
```

```response
┌─zero─┐
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
└──────┘
```