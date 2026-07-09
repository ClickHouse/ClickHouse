---
description: 'توثيق أنواع البيانات في ClickHouse'
sidebar_label: 'قائمة أنواع البيانات'
sidebar_position: 1
slug: /sql-reference/data-types/
title: 'أنواع البيانات في ClickHouse'
doc_type: 'reference'
---

يشرح هذا القسم أنواع البيانات التي يدعمها ClickHouse، مثل [الأعداد الصحيحة](int-uint.md) و[الأعداد ذات الفاصلة العائمة](float.md) و[السلاسل النصية](string.md).

يوفّر جدول النظام [system.data&#95;type&#95;families](/ar/operations/system-tables/data_type_families)
لمحة عامة عن جميع أنواع البيانات المتاحة.
كما يوضّح ما إذا كان نوع البيانات اسمًا مستعارًا لنوع بيانات آخر، وما إذا كان اسمه يراعي حالة الأحرف (على سبيل المثال `bool` مقابل `BOOL`).