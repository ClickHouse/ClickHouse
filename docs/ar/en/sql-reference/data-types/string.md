---
description: 'توثيق نوع البيانات String في ClickHouse'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

سلاسل نصية بطول اعتباطي. الطول غير محدود. يمكن أن تحتوي القيمة على مجموعة اعتباطية من البايتات، بما في ذلك بايتات null.
يستبدل النوع String الأنواع VARCHAR وBLOB وCLOB وغيرها من الأنواع في أنظمة إدارة قواعد البيانات الأخرى.

عند إنشاء الجداول، يمكن تعيين معلمات رقمية للحقول النصية (على سبيل المثال `VARCHAR(255)`)، لكن ClickHouse يتجاهلها.

الأسماء المستعارة:

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## الترميزات
</div>

لا يعتمد ClickHouse مفهوم الترميزات. يمكن أن تحتوي السلاسل النصية على أي مجموعة من البايتات، وتُخزَّن وتُعرَض كما هي.
إذا كنت بحاجة إلى تخزين نصوص، فنوصي باستخدام ترميز UTF-8. وعلى الأقل، إذا كانت الطرفية لديك تستخدم UTF-8 (كما هو موصى به)، فسيكون بإمكانك قراءة قيمك وكتابتها من دون إجراء أي تحويلات.
وبالمثل، تتوفر لبعض الدوال الخاصة بالعمل مع السلاسل النصية إصدارات منفصلة تعمل على افتراض أن السلسلة النصية تحتوي على بايتات تمثل نصًا مُرمَّزًا بـ UTF-8.
على سبيل المثال، تحسب الدالة [length](/ar/sql-reference/functions/array-functions#length) طول السلسلة النصية بالبايتات، بينما تحسب الدالة [lengthUTF8](../functions/string-functions.md#lengthUTF8) طول السلسلة النصية بنقاط Unicode البرمجية، على افتراض أن القيمة مُرمَّزة بـ UTF-8.